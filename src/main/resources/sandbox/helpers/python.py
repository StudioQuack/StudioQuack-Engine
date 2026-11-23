# StudioQuack in-sandbox Python helper. Prepended to the user's node script by
# GenerateScriptService; the "__SQ_CONTEXT__" token below is replaced at build time with the
# Base64-encoded execution context (so context reads never need the network). Keep this template
# free of printf-style %s placeholders.
#
# Typed BPMN errors / boundary events (raise a typed BPMN error; do not return {"error":...}):
#   raise StudioQuackError("Error_BadInput", "a and b must be numbers")
#   studioquack.escalate("SLA_BREACH")
#   if studioquack.isCancelled():
#       ...
import base64 as _sq_b64
import json as _sq_json
import os as _sq_os
import sys as _sq_sys
import urllib.error as _sq_urlerr
import urllib.request as _sq_url
import urllib.parse as _sq_urlparse
import socket as _sq_socket
import http.client as _sq_http_client
import threading as _sq_threading
import signal as _sq_signal
import time as _sq_time

class _SqUnixConnection(_sq_http_client.HTTPConnection):
    def connect(self):
        self.sock = _sq_socket.socket(_sq_socket.AF_UNIX, _sq_socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(_sq_os.environ.get("SQ_HELPER_SOCKET", ""))

class _SqUnixHandler(_sq_url.HTTPHandler):
    def http_open(self, req):
        return self.do_open(_SqUnixConnection, req)

_sq_opener = _sq_url.build_opener(_SqUnixHandler)

_SQ_CTX_ENCODED = "__SQ_CONTEXT__"
try:
    _sq_ctx = _sq_json.loads(_sq_b64.b64decode(_SQ_CTX_ENCODED).decode("utf-8")) if _SQ_CTX_ENCODED else {}
except Exception:
    _sq_ctx = {}
if not isinstance(_sq_ctx, dict):
    _sq_ctx = {}
# __SQ_EDGE_TRANSFORM_HOOK__

# Stateful runner: keep the long-lived app up across requests (returnResult must NOT exit).
_SQ_STATEFUL_RUNNER = (_sq_os.environ.get("SQ_STATEFUL_RUNNER", "").strip().lower()
                       in {"1", "true", "yes", "on"})

# Cancel / typed-fail bookkeeping (process + per-request). Never log tokens or cancel reasons.
_sq_cancel_lock = _sq_threading.Lock()
_sq_cancel_callbacks = []
_sq_cancel_fired = False
_sq_process_cancelled = False
_sq_cancelled_requests = set()
_sq_cancel_poll_at = 0.0
_sq_sigterm_installed = False
_sq_current_request_id = None
_sq_active_cancelled = False
_sq_current_failure = None
_sq_in_fail = False


def _sq_require_error_code(errorCode):
    code = "" if errorCode is None else str(errorCode).strip()
    if not code:
        raise ValueError("errorCode is required")
    if code.upper() == "SQ_SKIP":
        raise ValueError("errorCode 'SQ_SKIP' is reserved")
    return code


class StudioQuackError(Exception):
    """Typed business error. Raising it ends the step as POST /v1/helper/fail → BPMN error boundary."""

    def __init__(self, errorCode, message=None, data=None):
        self.errorCode = _sq_require_error_code(errorCode)
        self.message = "" if message is None else str(message)
        self.data = data
        Exception.__init__(self, self.message or self.errorCode)


def _sq_current_is_cancelled():
    if _sq_process_cancelled or _sq_active_cancelled:
        return True
    rid = _sq_current_request_id
    return rid is not None and str(rid) in _sq_cancelled_requests


def _sq_run_cancel_callbacks():
    global _sq_cancel_fired
    with _sq_cancel_lock:
        if _sq_cancel_fired:
            cbs = []
        else:
            _sq_cancel_fired = True
            cbs = list(_sq_cancel_callbacks)
    for cb in cbs:
        try:
            cb()
        except Exception:
            pass


def _sq_mark_process_cancelled():
    global _sq_process_cancelled
    with _sq_cancel_lock:
        _sq_process_cancelled = True
    _sq_run_cancel_callbacks()


def _sq_mark_request_cancelled(request_id):
    global _sq_active_cancelled
    rid = str(request_id)
    with _sq_cancel_lock:
        _sq_cancelled_requests.add(rid)
        if _sq_current_request_id is not None and rid == str(_sq_current_request_id):
            _sq_active_cancelled = True
    if (not _SQ_STATEFUL_RUNNER) or rid == str(_sq_current_request_id or ""):
        _sq_run_cancel_callbacks()


def _sq_on_sigterm(_signum, _frame):
    _sq_mark_process_cancelled()


def _sq_ensure_sigterm():
    global _sq_sigterm_installed
    if _sq_sigterm_installed:
        return
    try:
        _sq_signal.signal(_sq_signal.SIGTERM, _sq_on_sigterm)
    except Exception:
        pass
    _sq_sigterm_installed = True


def _sq_poll_cancelled():
    """GET /v1/helper/cancelled at most once per second. A positive answer is cached forever."""
    global _sq_cancel_poll_at
    if _sq_current_is_cancelled():
        return True
    now = _sq_time.monotonic()
    if (now - _sq_cancel_poll_at) < 1.0:
        return False
    _sq_cancel_poll_at = now
    resp = _sq_get_json("/v1/helper/cancelled", timeout=5.0)
    if isinstance(resp, dict) and (resp.get("cancelled") or resp.get("canceled")):
        rid = _sq_current_request_id
        if rid:
            _sq_mark_request_cancelled(rid)
        elif _SQ_STATEFUL_RUNNER:
            _sq_mark_current_cancelled()
        else:
            _sq_mark_process_cancelled()
        return True
    return False


def _sq_mark_current_cancelled():
    """Cancel the in-flight request only (stateful); does not set the SIGTERM process flag."""
    global _sq_active_cancelled
    with _sq_cancel_lock:
        _sq_active_cancelled = True
        if _sq_current_request_id:
            _sq_cancelled_requests.add(str(_sq_current_request_id))
    _sq_run_cancel_callbacks()


# Preload: import operator-flagged libraries (SQ_PRELOAD_MODULES, injected by the daemon from the
# dependency manifest's preload=true entries) up-front so EVERY node script reuses them and they
# are page-cache warm on the shared /opt/sq/deps mount. Best-effort — an unavailable/mis-named
# module is skipped so a stray preload entry never breaks a run.
for _sq_pre in [m.strip() for m in (_sq_os.environ.get("SQ_PRELOAD_MODULES", "") or "").split(",") if m.strip()]:
    try:
        __import__(_sq_pre)
    except Exception:
        pass


def _sq_helper_base():
    """Loopback helper-channel base URL. Now a dummy HTTP host since transport is AF_UNIX."""
    if not _sq_os.environ.get("SQ_HELPER_SOCKET"):
        return ""
    return "http://sq-helper"


def _sq_helper_bearer():
    """Per-job bearer minted by the daemon (revoked when the job finishes)."""
    return (_sq_os.environ.get("SQ_HELPER_BEARER")
            or _sq_os.environ.get("SQVM_HELPER_TOKEN") or "")


def _sq_post_json(path, body, timeout=15.0):
    """POST JSON to the daemon helper channel; returns parsed JSON or None. Never raises."""
    base = _sq_helper_base()
    if not base:
        return None
    try:
        req = _sq_url.Request(
            base + path,
            data=_sq_json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json",
                     "Authorization": "Bearer " + _sq_helper_bearer()},
            method="POST",
        )
        with _sq_opener.open(req, timeout=timeout) as r:
            raw = r.read()
            if not raw:
                return {}
            try:
                return _sq_json.loads(raw.decode("utf-8"))
            except Exception:
                return {}
    except Exception:
        return None


def _sq_get_json(path, timeout=15.0):
    """GET JSON from the daemon helper channel; returns parsed JSON or None. Never raises."""
    base = _sq_helper_base()
    if not base:
        return None
    try:
        req = _sq_url.Request(
            base + path,
            headers={"Authorization": "Bearer " + _sq_helper_bearer()},
            method="GET",
        )
        with _sq_opener.open(req, timeout=timeout) as r:
            raw = r.read()
            if not raw:
                return {}
            try:
                return _sq_json.loads(raw.decode("utf-8"))
            except Exception:
                return {}
    except Exception:
        return None


def _sq_request_or_raise(method, path, body, timeout, label):
    """
    Fail-loud request to the daemon helper channel. Used by the calls whose RESULT the
    workflow depends on (the LLM proxy and the FUNCTION-port ``execute``).
    """
    base = _sq_helper_base()
    if not base:
        raise RuntimeError(
            label + ": SQ_HELPER_SOCKET is not set — this container has no helper broker "
            "(the image must ship python3 so the daemon can plant its in-container broker); "
            "studioquack.* callbacks are unavailable")
    data = None
    headers = {"Authorization": "Bearer " + _sq_helper_bearer()}
    if body is not None:
        data = _sq_json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = _sq_url.Request(base + path, data=data, headers=headers, method=method)
    try:
        with _sq_opener.open(req, timeout=timeout) as r:
            raw = r.read()
    except _sq_urlerr.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", errors="replace")[:512]
        except Exception:
            pass
        raise RuntimeError(
            label + ": " + method + " " + path
            + " status=" + str(e.code) + " body=" + repr(detail)) from e
    except Exception as e:
        # DNS/connect/timeout: the sandbox never reached the daemon. Naming the target is the whole
        # diagnosis — the host alias is the part that breaks per-platform (Podman for Windows routes it to
        # the WSL VM, and Windows Firewall drops the high port), and everything daemon-side still looks
        # healthy when it.
        raise RuntimeError(
            label + ": " + method + " " + path
            + " could not reach the daemon helper channel at " + _sq_os.environ.get("SQ_HELPER_SOCKET", "")
            + " — " + type(e).__name__ + ": " + str(e)) from e
    if not raw:
        return {}
    try:
        return _sq_json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise RuntimeError(
            label + ": " + method + " " + path
            + " returned a non-JSON body: " + type(e).__name__ + ": " + str(e)) from e


def _sq_iter_sse(line_iter):
    """
    Yields each ``partial`` payload, then the ``complete`` payload (and returns). ``error``
    raises ``RuntimeError`` using ``data.message`` when the payload is a dict.
    """
    pending_event = None
    pending_data_lines = []

    def _flush():
        nonlocal pending_event, pending_data_lines
        if pending_event is None and not pending_data_lines:
            return None
        ev_name = pending_event or "message"
        raw = "\n".join(pending_data_lines)
        pending_event = None
        pending_data_lines = []
        try:
            data = _sq_json.loads(raw) if raw else None
        except Exception:
            data = raw
        return (ev_name, data)

    for raw_line in line_iter:
        if raw_line is None:
            continue
        if isinstance(raw_line, (bytes, bytearray)):
            line = raw_line.decode("utf-8", errors="replace")
        else:
            line = str(raw_line)
        line = line.rstrip("\r\n")
        if line == "":
            flushed = _flush()
            if flushed is None:
                continue
            name, data = flushed
            if name == "partial":
                yield data
            elif name == "complete":
                yield data
                return
            elif name == "error":
                msg = "stream error"
                if isinstance(data, dict) and "message" in data:
                    msg = data.get("message") or msg
                raise RuntimeError(msg)
            elif name == "done":
                return
            continue
        if line.startswith(":"):
            continue
        if line.startswith("event:"):
            pending_event = line[len("event:"):].strip()
            continue
        if line.startswith("data:"):
            payload = line[len("data:"):]
            if payload.startswith(" "):
                payload = payload[1:]
            pending_data_lines.append(payload)
            continue


def _sq_stream_execute(port_value, body):
    """
    POST /v1/helper/execute/stream over AF_UNIX and return an SSE generator. Constructs
    ``_SqUnixConnection`` the same way ``_SqUnixHandler.http_open`` does (dummy host ``sq-
    helper``, timeout as a keyword so it is not taken as a TCP port) so the body is read
    line-by-line via http.client.
    """
    label = "studioquack.execute('" + str(port_value) + "')"
    if not _sq_os.environ.get("SQ_HELPER_SOCKET"):
        raise RuntimeError(
            label + ": SQ_HELPER_SOCKET is not set — this container has no helper broker "
            "(the image must ship python3 so the daemon can plant its in-container broker); "
            "studioquack.* callbacks are unavailable")
    payload = _sq_json.dumps(body).encode("utf-8")
    headers = {
        "Authorization": "Bearer " + _sq_helper_bearer(),
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
    }
    # Same construction as urllib's HTTPHandler.do_open(_SqUnixConnection, req).
    conn = _SqUnixConnection("sq-helper", timeout=None)
    try:
        conn.request("POST", "/v1/helper/execute/stream", body=payload, headers=headers)
        resp = conn.getresponse()
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        raise RuntimeError(
            label + ": POST /v1/helper/execute/stream"
            + " could not reach the daemon helper channel at "
            + _sq_os.environ.get("SQ_HELPER_SOCKET", "")
            + " — " + type(e).__name__ + ": " + str(e)) from e
    if resp.status < 200 or resp.status >= 300:
        detail = ""
        try:
            detail = resp.read().decode("utf-8", errors="replace")[:512]
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
        raise RuntimeError(
            label + ": POST /v1/helper/execute/stream"
            + " status=" + str(resp.status) + " body=" + repr(detail))

    def _generate():
        try:
            def _lines():
                fp = resp.fp
                while True:
                    line = fp.readline() if fp is not None else b""
                    if not line:
                        break
                    yield line
            yield from _sq_iter_sse(_lines())
        finally:
            try:
                conn.close()
            except Exception:
                pass

    return _generate()


def _sq_llm_request(method, path, body, timeout=None):
    """
    Fail-loud POST/GET to the daemon LLM helper channel. Buffered JSON only — never set
    ``Accept: text/event-stream``.
    """
    return _sq_request_or_raise(method, path, body, timeout, "studioquack.llm")


class _LlmHelper:
    """`studioquack.llm` namespace. chat()/local()/completion() all route through the
    daemon's loopback LLM proxy (/v1/helper/llm/...), which injects the environment's
    configured OpenAI-compatible provider credentials — the sandbox never sees the key."""

    def __init__(self):
        self._caller_env_id = _sq_os.environ.get("SQ_HELPER_CALLER_ENV_ID", "")

    def completion(self, messages=None, model=None, prompt=None, timeout_s=None, env=None, **params):
        """
        The seeded Local LLM node and ``local-llm.py`` call ``studioquack.llm.completion(...)``
        and expect the full chat-completions envelope (``choices[0].message``). The daemon maps
        ``POST /v1/helper/llm/completion`` onto the configured provider chat URL.
        """
        body = {}
        if messages is not None:
            body["messages"] = [dict(m) if isinstance(m, dict) else m for m in messages]
        elif prompt is not None:
            # Provider servers that only honour ``messages`` still get a usable turn.
            body["messages"] = [{"role": "user", "content": prompt}]
        if prompt is not None:
            body["prompt"] = prompt
        if model:
            body["model"] = model
        for key, value in params.items():
            if value is not None:
                body[key] = value
        if env is not None:
            # Routing metadata for the daemon (stripped before the payload reaches any
            # provider) — set AFTER **params so the explicit parameter always wins.
            body["envId"] = env
        return _sq_llm_request("POST", "/v1/helper/llm/completion", body, timeout=timeout_s)

    def chat(self, env_id=None, *, model=None, messages=None, prompt=None,
             temperature=None, max_tokens=None, return_envelope=False, timeout_s=None, env=None, **extra):
        """``env`` names the target environment (id or name); omitting it uses this environment."""
        body = {}
        if model:
            body["model"] = model
        if messages is not None:
            body["messages"] = [dict(m) for m in messages]
        elif prompt is not None:
            body["messages"] = [{"role": "user", "content": prompt}]
        if temperature is not None:
            body["temperature"] = temperature
        if max_tokens is not None:
            body["max_tokens"] = max_tokens
        for k, v in extra.items():
            body.setdefault(k, v)
        # Env_id and env (explicit keyword) are the same targeting argument; the explicit keyword wins
        # when both are passed.
        effective_env = env if env is not None else env_id
        if effective_env is not None:
            body["envId"] = effective_env

        envelope = _sq_llm_request("POST", "/v1/helper/llm/chat/completions", body, timeout=timeout_s)
        if return_envelope:
            return envelope
        return _sq_extract_content(envelope)

    # local() is kept as an alias of chat() — on the new daemon both go through the same
    # loopback provider proxy (there is no separate BE hop to distinguish).
    def local(self, **kwargs):
        return self.chat(**kwargs)

    # ---- local model management (same operations in the JS/TS and Java helpers) ----
    def models(self):
        """Models the engine's provider reports (GET /v1/helper/llm/models)."""
        return _sq_llm_request("GET", "/v1/helper/llm/models", None, timeout=30.0)

    def load(self, model=None, **params):
        """Load a model on the engine's provider. Extra params are forwarded verbatim."""
        return _sq_llm_request("POST", "/v1/helper/llm/load", _sq_model_body(model, params))

    def unload(self, model=None, **params):
        return _sq_llm_request("POST", "/v1/helper/llm/unload", _sq_model_body(model, params))

    def download(self, model=None, **params):
        """Start a model download; poll download_status() for progress."""
        body = _sq_model_body(model, params)
        if model is not None:
            body.setdefault("repo", model)
        return _sq_llm_request("POST", "/v1/helper/llm/download", body)

    def download_status(self, job_id=None):
        path = "/v1/helper/llm/download/status"
        if job_id:
            path += "?job_id=" + _sq_urlparse.quote(str(job_id), safe="")
        return _sq_llm_request("GET", path, None, timeout=30.0)

    Load = load
    Unload = unload
    Download = download
    DownloadStatus = download_status
    downloadStatus = download_status


def _sq_model_body(model, params):
    body = {k: v for k, v in (params or {}).items() if v is not None}
    if model is not None:
        body.setdefault("model", model)
        body.setdefault("model_id", model)
    return body


_sq_tunnels, _sq_tunnel_lock = {}, _sq_threading.Lock()


class _SqTunnel:
    """
    A local address that forwards to a NAMED stateful instance through the daemon. The
    sandbox has no route to a sibling container, so nothing here talks to the instance
    directly.
    """

    # Long enough that an idle connection is cheap, short enough that a closed tunnel is noticed.
    RECV_TIMEOUT_MS = 2000
    CHUNK = 65536

    def __init__(self, container_name, port):
        self.container_name = str(container_name)
        self.target_port = int(port)
        self._closed = False
        self._listener = _sq_socket.socket(_sq_socket.AF_INET, _sq_socket.SOCK_STREAM)
        self._listener.setsockopt(_sq_socket.SOL_SOCKET, _sq_socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(8)
        self.host, self.port = self._listener.getsockname()[0], self._listener.getsockname()[1]
        # Verify the target up-front so a wrong name/port fails at connect() — where the script can
        # see it — instead of surfacing later as an unexplained connection reset in a DB driver.
        probe = self._open_stream()
        self._close_stream(probe)
        _sq_threading.Thread(target=self._accept_loop, daemon=True).start()

    # ---- addressing conveniences -------------------------------------------------
    @property
    def address(self):
        return (self.host, self.port)

    def url(self, scheme="http", path=""):
        return scheme + "://" + self.host + ":" + str(self.port) + path

    def __iter__(self):
        return iter(self.address)

    def __getitem__(self, index):
        return self.address[index]

    def __repr__(self):
        return ("<studioquack tunnel " + self.host + ":" + str(self.port) + " -> "
                + self.container_name + ":" + str(self.target_port) + ">")

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()
        return False

    def close(self):
        self._closed = True
        try:
            self._listener.close()
        except Exception:
            pass

    # ---- daemon stream ops -------------------------------------------------------
    def _open_stream(self):
        resp = _sq_request_or_raise(
            "POST", "/v1/helper/connect/open",
            {"containerName": self.container_name, "port": self.target_port}, 30.0,
            "studioquack.connect('" + self.container_name + "', " + str(self.target_port) + ")")
        stream_id = (resp or {}).get("streamId")
        if not stream_id:
            raise RuntimeError("studioquack.connect: the daemon returned no streamId for "
                               + self.container_name + ":" + str(self.target_port))
        return stream_id

    def _close_stream(self, stream_id):
        try:
            _sq_post_json("/v1/helper/connect/close", {"streamId": stream_id})
        except Exception:
            pass

    def _accept_loop(self):
        while not self._closed:
            try:
                conn, _peer = self._listener.accept()
            except Exception:
                return
            _sq_threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn):
        """One local connection ↔ one daemon-held socket, pumped in both directions."""
        stream_id = None
        try:
            stream_id = self._open_stream()
            down = _sq_threading.Thread(target=self._downlink, args=(conn, stream_id), daemon=True)
            down.start()
            self._uplink(conn, stream_id)
            down.join(timeout=5.0)
        except Exception:
            pass
        finally:
            if stream_id:
                self._close_stream(stream_id)
            try:
                conn.close()
            except Exception:
                pass

    def _uplink(self, conn, stream_id):
        """Local client → daemon → instance."""
        while not self._closed:
            try:
                data = conn.recv(self.CHUNK)
            except Exception:
                return
            if not data:
                return
            _sq_request_or_raise(
                "POST", "/v1/helper/connect/send",
                {"streamId": stream_id, "dataB64": _sq_b64.b64encode(data).decode("ascii")},
                60.0, "studioquack.connect send")

    def _downlink(self, conn, stream_id):
        """Instance → daemon → local client."""
        # The daemon parks its read for RECV_TIMEOUT_MS, so allow generous slack on the HTTP wait;
        # a poll that timed out client-side would abandon bytes the daemon had already dequeued.
        timeout_s = (self.RECV_TIMEOUT_MS / 1000.0) + 30.0
        while not self._closed:
            try:
                resp = _sq_request_or_raise(
                    "POST", "/v1/helper/connect/recv",
                    {"streamId": stream_id, "timeoutMs": self.RECV_TIMEOUT_MS},
                    timeout_s, "studioquack.connect recv")
            except Exception:
                return
            chunk = (resp or {}).get("dataB64") or ""
            if chunk:
                try:
                    conn.sendall(_sq_b64.b64decode(chunk))
                except Exception:
                    return
            if (resp or {}).get("eof"):
                try:
                    conn.shutdown(_sq_socket.SHUT_WR)
                except Exception:
                    pass
                return


def _sq_extract_content(envelope):
    if not isinstance(envelope, dict):
        return ""
    if "content" in envelope:
        return str(envelope.get("content") or "")
    choices = envelope.get("choices")
    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        msg = choices[0].get("message")
        if isinstance(msg, dict):
            return str(msg.get("content") or "")
        if choices[0].get("text") is not None:
            return str(choices[0].get("text"))
    return ""


class StudioQuackHelper:
    """
    Accessors read the baked execution context (no network); side effects post to the daemon
    helper channel.
    """

    class ResultItem:
        def __init__(self, port_value, result):
            self.portValue = str(port_value)
            self.result = result

        @property
        def port(self):
            return self.portValue

        @property
        def value(self):
            return self.result

        def to_wire(self):
            v = self.result if isinstance(self.result, str) else _sq_json.dumps(self.result, ensure_ascii=False, default=str)
            return {"port": self.portValue, "value": v}

    def __init__(self, api_url=None, token=None, encoded_execution_context=None, *, execution_context=None):

        if execution_context is not None and isinstance(execution_context, dict):
            self.execution_context = dict(execution_context)
        elif encoded_execution_context:
            self.execution_context = _decode_b64_wrapper(encoded_execution_context)
        else:
            self.execution_context = dict(_sq_ctx)
        self.api_url = api_url or ""
        self.token = token or _sq_helper_bearer()
        self.llm = _LlmHelper()

    # ---- node-to-node execute → daemon-mediated /v1/helper/execute ----
    def execute(self, port_value, node_request, node_id=None, env=None, stream=False):
        """
        Invoke a connected node through its FUNCTION port (e.g. an agent calling its `model`
        provider). The two conditions are NOT interchangeable: `[]` from a successful call means
        the daemon matched no connection on this port (a real wiring gap), whereas an
        unreachable helper channel means the call never happened at all.
        """
        body = {"portValue": port_value, "payload": node_request}
        if node_id is not None:
            body["nodeId"] = node_id
        if env is not None:
            body["envId"] = env
        if stream:
            return _sq_stream_execute(port_value, body)
        # No time limit: this wait blocks until the sub-workflow returns, however long it runs. The
        # daemon ends it when the run is cancelled, and the node's own wall-clock limit (if set) still
        # applies to the container.
        resp = _sq_request_or_raise(
            "POST", "/v1/helper/execute", body, None,
            "studioquack.execute('" + str(port_value) + "')")
        if not isinstance(resp, dict):
            return []
        return resp.get("result", [])

    # ---- stateful-instance access → daemon-mediated tunnel ----
    def connect(self, container_name, port):
        """
        Open a secure line to a NAMED stateful container and return a local address for it. Use
        the instance name, never an address:: host, port = studioquack.connect("my-postgres",
        5432) conn = psycopg2.connect(host=host, port=port, user=..., dbname=...) api =
        studioquack.connect("my-service", 8080).
        """
        key = str(container_name) + ":" + str(int(port))
        with _sq_tunnel_lock:
            existing = _sq_tunnels.get(key)
            if existing is not None and not existing._closed:
                return existing
            tunnel = _SqTunnel(container_name, port)
            _sq_tunnels[key] = tunnel
            return tunnel

    Connect = connect

    def llm_local(self, messages, model=None, tools=None, **params):
        """Chat completion on this engine's provider; returns the full envelope. Fail-loud:
        raises on an empty ``messages`` list, a transport fault or a refused call."""
        if not isinstance(messages, list) or not messages:
            raise ValueError("llm_local requires a non-empty `messages` list")
        return self.llm.completion(messages=messages, model=model, tools=tools, **params)

    llmLocal = llm_local

    # ---- engine addressing → daemon-mediated /v1/helper/engine-ip ----
    def engineIP(self, engine_id):
        """
        Resolve an engine id to its CURRENT overlay address (/). A Network Access Control rule
        is keyed by engine id, never by address, because an engine's overlay address changes
        when it reconnects.
        """
        resp = _sq_request_or_raise(
            "POST", "/v1/helper/engine-ip", {"engineId": str(engine_id)}, 15.0,
            "studioquack.engineIP('" + str(engine_id) + "')")
        ip = (resp or {}).get("ip")
        if not ip:
            raise RuntimeError("studioquack.engineIP('" + str(engine_id)
                               + "'): the daemon returned no address")
        return ip

    # ---- execution context accessors + mutators ----
    def getExecutionContext(self):
        return dict(self.execution_context) if isinstance(self.execution_context, dict) else {}

    def setExecutionContext(self, ctx):
        self.execution_context = dict(ctx) if isinstance(ctx, dict) else {}
        return self.execution_context

    def updateExecutionContext(self, patch):
        if not isinstance(self.execution_context, dict):
            self.execution_context = {}
        if isinstance(patch, dict):
            for k, v in patch.items():
                self.execution_context[str(k)] = v
        return self.execution_context

    def setExecutionContextValue(self, key, value):
        if not key or not isinstance(key, str):
            return self.execution_context if isinstance(self.execution_context, dict) else {}
        if not isinstance(self.execution_context, dict):
            self.execution_context = {}
        self.execution_context[key] = value
        return self.execution_context

    def updateInput(self, port_value, value):
        node = self._current_node_mutable()
        if node is None:
            return self.execution_context
        input_list = node.get("inputList")
        if not isinstance(input_list, list):
            input_list = []
            node["inputList"] = input_list
        for entry in input_list:
            if isinstance(entry, dict) and entry.get("targetPortValue") == port_value:
                entry["value"] = value
                return self.execution_context
        input_list.append({"targetPortValue": port_value, "value": value})
        return self.execution_context

    def setOutput(self, port_value, value):
        node = self._current_node_mutable()
        if node is None:
            return self.execution_context
        output_list = node.get("outputList")
        if not isinstance(output_list, list):
            output_list = []
            node["outputList"] = output_list
        for entry in output_list:
            if isinstance(entry, dict) and entry.get("portValue") == port_value:
                entry["value"] = value
                return self.execution_context
        output_list.append({"portValue": port_value, "value": value})
        return self.execution_context

    def returnExecutionContext(self):
        snapshot = self.getExecutionContext()
        _sq_post_json("/v1/helper/transform-context", {"executionContext": snapshot})
        return snapshot

    def _current_node_mutable(self):
        if not isinstance(self.execution_context, dict):
            return None
        node = self.execution_context.get("currentNode")
        return node if isinstance(node, dict) else None

    def getWorkflowExecutionId(self):
        v = self.getExecutionContext().get("workflowExecutionId", "")
        return str(v) if v is not None else ""

    def getCurrentNode(self):
        node = self.getExecutionContext().get("currentNode")
        return dict(node) if isinstance(node, dict) else {}

    def getEdgeTransformListForCurrentNode(self):
        v = self.getCurrentNode().get("edgeTransformListForCurrentNode")
        return list(v) if isinstance(v, list) else []

    def getCurrentNodeId(self):
        v = self.getCurrentNode().get("id")
        if v is None:
            v = self.getExecutionContext().get("currentNodeId", "")
        return str(v) if v is not None else ""

    # ---- inputs / outputs ----
    def getInput(self):
        v = self.getCurrentNode().get("inputList", [])
        return list(v) if isinstance(v, list) else []

    def getInputValuesByPortValue(self, port_value, node_id=None):
        result = []
        for item in self.getInput():
            if not isinstance(item, dict):
                continue
            if item.get("targetPortValue") != port_value:
                continue
            if node_id is not None and item.get("nodeId") != node_id:
                continue
            result.append(item.get("value"))
        if not result:
            result = self.setDefaultInputValuesByPortValue(port_value)
        return result

    def setDefaultInputValuesByPortValue(self, port_value):
        schema = self.getPortSchemaByName(port_value)
        default = {}
        children = schema.get("children") if isinstance(schema, dict) else None
        if isinstance(children, list):
            for child in children:
                if isinstance(child, dict) and child.get("shortCode"):
                    default[child.get("shortCode")] = None
        return [default] if default else []

    def getOutput(self):
        v = self.getCurrentNode().get("outputList", [])
        return list(v) if isinstance(v, list) else []

    def getSpecListForNodeOutput(self):
        v = self.getCurrentNode().get("specListForNodeOutput", [])
        return list(v) if isinstance(v, list) else []


    @staticmethod
    def returnResult(items):
        if not isinstance(items, list):
            raise TypeError("returnResult expects a list of ResultItem objects.")
        for item in items:
            if not isinstance(item, StudioQuackHelper.ResultItem):
                raise TypeError("Each element in the list must be a ResultItem object.")
            if item.portValue is not None:
                print("portValue:" + str(item.portValue) + ":::result:" + str(item.result))
            else:
                print("requires targetNodeId and portValue for each ResultItem")
        _post_helper_channel_result(items)
        if _SQ_STATEFUL_RUNNER:
            return
        _sq_sys.exit(0)

    def set_request_context(self, execution_context=None, *, helper_url=None, helper_token=None,
                            request_id=None):
        global _sq_current_request_id, _sq_current_failure, _sq_active_cancelled, _sq_cancel_fired
        if isinstance(execution_context, dict):
            self.execution_context = dict(execution_context)
        if helper_token:
            self.token = helper_token
        rid = request_id
        if rid is None and isinstance(execution_context, dict):
            rid = execution_context.get("requestId")
        rid = str(rid).strip() if rid not in (None, "") else None
        with _sq_cancel_lock:
            _sq_current_request_id = rid
            _sq_current_failure = None
            _sq_active_cancelled = bool(rid is not None and rid in _sq_cancelled_requests)
            # New request may observe cancel again; do not clear SIGTERM or re-run hooks.
            if not _sq_process_cancelled and not _sq_active_cancelled:
                _sq_cancel_fired = False
        return self

    # ---- template / ports ----
    def getTemplate(self):
        v = self.getCurrentNode().get("data", {})
        return dict(v) if isinstance(v, dict) else {}

    def getPortByValue(self, port_value):
        for port in self.getTemplate().get("ports", []) or []:
            if isinstance(port, dict) and port.get("value") == port_value:
                return port
        return None

    def getPortValueByName(self, port_value):
        port = self.getPortByValue(port_value)
        return port.get("value", {}) if port else {}

    def getPortSchemaByName(self, port_value):
        port = self.getPortByValue(port_value)
        if not port:
            return {}
        schema = port.get("schema")
        if isinstance(schema, str):
            try:
                return _sq_json.loads(schema)
            except Exception:
                return {}
        return schema if isinstance(schema, dict) else {}

    # ---- parameters ----
    def getParameters(self):
        v = self.getTemplate().get("configurationParameters", [])
        return list(v) if isinstance(v, list) else []

    def getParameterByShortCode(self, short_code):
        for param in self.getParameters():
            if isinstance(param, dict) and param.get("shortCode") == short_code:
                return param
        return None

    def getParameterValueByShortCode(self, short_code, return_value=None):
        param = self.getParameterByShortCode(short_code)
        if param:
            return param.get("value", {})
        return return_value

    # ---- secrets (resolved to a per-job tmpfs .env by the daemon) ----
    def getSecret(self, key):
        if not key or not isinstance(key, str):
            return None
        env_key = "SQVM_SECRET__" + key.upper()
        if env_key in _sq_os.environ:
            return _sq_os.environ[env_key]
        path = _sq_os.environ.get("SQVM_SECRETS_FILE", "")
        if not path or not _sq_os.path.isfile(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.rstrip("\n")
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    if k == env_key:
                        return _unquote_dotenv(v)
        except OSError:
            return None
        return None

    # ---- connections ----
    def getConnections(self):
        v = self.getCurrentNode().get("connectionList", [])
        return list(v) if isinstance(v, list) else []

    def findConnectionByPortValue(self, port_value):
        for connection in self.getConnections():
            if isinstance(connection, dict) and connection.get("portValue") == port_value:
                return connection
        return None

    def getConnectionDetailListByPortValue(self, port_value):
        conn = self.findConnectionByPortValue(port_value)
        if conn:
            v = conn.get("connectionDetailList", [])
            return list(v) if isinstance(v, list) else []
        return []

    def findFirstConnectionNodeIdByPortValue(self, port_value):
        conn = self.findConnectionByPortValue(port_value)
        details = conn.get("connectionDetailList") if conn else None
        if isinstance(details, list) and details and isinstance(details[0], dict):
            return details[0].get("nodeId")
        return None

    def getDucktapeConfigurations(self, node_id, source_port_name, target_port_name):
        out = []
        current_id = self.getCurrentNodeId()
        for cfg in self.getEdgeTransformListForCurrentNode():
            if not isinstance(cfg, dict):
                continue
            src_n, src_p = cfg.get("sourceNodeId"), cfg.get("sourcePortShortCode")
            tgt_n, tgt_p = cfg.get("targetNodeId"), cfg.get("targetPortShortCode")
            if (src_n == current_id and src_p == source_port_name
                    and tgt_n == node_id and tgt_p == target_port_name):
                out.append(cfg)
            elif (src_n == node_id and src_p == target_port_name
                  and tgt_n == current_id and tgt_p == source_port_name):
                out.append(cfg)
        return out

    def getConnectionDetailParameters(self, connection_detail):
        v = connection_detail.get("configurationParameters", []) if isinstance(connection_detail, dict) else []
        return list(v) if isinstance(v, list) else []

    def getConnectionDetailParameterByShortCode(self, configuration_parameters, short_code):
        for cp in configuration_parameters or []:
            if isinstance(cp, dict) and cp.get("shortCode") == short_code:
                return cp.get("value")
        return None

    # ---- side effects ----
    def metric(self, name, value):
        _sq_post_json("/v1/helper/metric", {"name": name, "value": value})

    def log(self, message, level="info"):
        _sq_post_json("/v1/helper/log", {"message": str(message), "level": level})

    def emit_partial(self, chunk, channel="answer", port=None):
        """
        Returns True only when the daemon JSON says delivered=true. Missing socket, transport
        faults, and a miss (no listener) all return False — never raise, and never log the chunk
        (it may carry customer content).
        """
        if not _sq_os.environ.get("SQ_HELPER_SOCKET"):
            return False
        body = {"channel": channel or "answer", "delta": chunk}
        if port:
            body["port"] = port
        resp = _sq_post_json("/v1/helper/emit-partial", body)
        if not isinstance(resp, dict):
            return False
        return bool(resp.get("delivered", False))

    # ---- typed BPMN error / boundary events ----
    def fail(self, errorCode=None, message=None, data=None, *, error_code=None):
        """POST /v1/helper/fail, write SQ_FAIL to stderr; exit 1 unless SQ_STATEFUL_RUNNER."""
        global _sq_current_failure, _sq_in_fail
        code = _sq_require_error_code(errorCode if errorCode is not None else error_code)
        msg = "" if message is None else str(message)
        payload = {"errorCode": code}
        if message is not None:
            payload["message"] = msg
        if data is not None:
            payload["data"] = data
        _sq_in_fail = True
        try:
            _sq_sys.stderr.write("SQ_FAIL " + code + ": " + msg + "\n")
            _sq_sys.stderr.flush()
            _sq_post_json("/v1/helper/fail", payload)
            _sq_current_failure = {"errorCode": code, "message": msg, "data": data}
            if _SQ_STATEFUL_RUNNER:
                return
            _sq_sys.exit(1)
        finally:
            if _SQ_STATEFUL_RUNNER:
                _sq_in_fail = False

    Fail = fail

    def escalate(self, escalationCode, message=None, data=None):
        """POST /v1/helper/escalate; returns so the step can keep running (non-interrupting)."""
        code = "" if escalationCode is None else str(escalationCode).strip()
        if not code:
            raise ValueError("escalationCode is required")
        body = {"escalationCode": code}
        if message is not None:
            body["message"] = str(message)
        if data is not None:
            body["data"] = data
        _sq_post_json("/v1/helper/escalate", body)

    def sendSignal(self, signalName, scope="global"):
        """POST /v1/helper/signal {signalName, scope}."""
        name = "" if signalName is None else str(signalName).strip()
        if not name:
            raise ValueError("signalName is required")
        if name.startswith("__sq_"):
            raise ValueError("signalName must not start with '__sq_'")
        _sq_post_json("/v1/helper/signal", {"signalName": name, "scope": scope or "global"})

    def sendMessage(self, messageName, correlationKey=None):
        """POST /v1/helper/message {messageName, correlationKey?}. None key = current run."""
        name = "" if messageName is None else str(messageName).strip()
        if not name:
            raise ValueError("messageName is required")
        body = {"messageName": name}
        if correlationKey is not None:
            body["correlationKey"] = correlationKey
        _sq_post_json("/v1/helper/message", body)

    def isCancelled(self):
        """True after SIGTERM, a cancel for the current request, or GET /v1/helper/cancelled."""
        _sq_ensure_sigterm()
        if _sq_current_is_cancelled():
            return True
        return _sq_poll_cancelled()

    def onCancel(self, callback):
        """Register a callback run once when cancel is observed. Callbacks never raise."""
        _sq_ensure_sigterm()
        if callback is None:
            return
        if _sq_current_is_cancelled():
            try:
                callback()
            except Exception:
                pass
            return
        with _sq_cancel_lock:
            _sq_cancel_callbacks.append(callback)
        _sq_poll_cancelled()

    def handleCancel(self, requestId, reason=None):
        """Mark requestId cancelled. Blank id, or no current request, cancels this step."""
        rid = None if requestId is None else str(requestId).strip()
        if not rid:
            _sq_mark_current_cancelled()
            return
        _sq_mark_request_cancelled(rid)
        if _sq_current_request_id is None:
            _sq_mark_current_cancelled()

    def isCancelPath(self, url_path):
        """True when url_path (query ignored) is {SQ_APP_PATH or /invoke}/cancel, or /cancel."""
        path = (url_path or "").split("?", 1)[0]
        app = (_sq_os.environ.get("SQ_APP_PATH") or "/invoke").rstrip("/")
        return path in (app + "/cancel", "/cancel")

    def handleCancelHttp(self, handler):
        """If this is POST {SQ_APP_PATH}/cancel, handle it and answer 200 {ok:true}. Returns True if consumed."""
        method = (getattr(handler, "command", "") or "").upper()
        if method != "POST":
            return False
        if not self.isCancelPath(getattr(handler, "path", "") or ""):
            return False
        raw = b"{}"
        try:
            length = int(handler.headers.get("Content-Length") or "0")
            if length > 0:
                raw = handler.rfile.read(length)
        except Exception:
            raw = b"{}"
        try:
            body = _sq_json.loads(raw.decode("utf-8") or "{}")
            if not isinstance(body, dict):
                body = {}
        except Exception:
            body = {}
        self.handleCancel(body.get("requestId"), body.get("reason"))
        payload = b'{"ok":true}'
        handler.send_response(200)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)
        return True

    # ---- snake_case convenience aliases (new-style scripts) ----
    def full_context(self):
        return self.getExecutionContext()

    def get_execution_context(self):
        return self.getExecutionContext()

    def current_node(self):
        return self.getCurrentNode()

    def current_node_id(self):
        return self.getCurrentNodeId()

    def workflow_execution_id(self):
        return self.getWorkflowExecutionId()

    def parameters(self):
        return self.getParameters()

    def connections(self):
        return self.getConnections()

    def inputs(self):
        out = {}
        for item in self.getInput():
            if isinstance(item, dict):
                key = item.get("targetPortValue") or item.get("portValue")
                if key:
                    out[key] = item.get("value")
        return out

    def input(self, key, default=None):
        return self.inputs().get(key, default)

    def return_result(self, items):
        normalised = []
        if isinstance(items, dict):
            for port, value in items.items():
                normalised.append(StudioQuackHelper.ResultItem(port, value))
        else:
            for item in (items or []):
                if isinstance(item, StudioQuackHelper.ResultItem):
                    normalised.append(item)
                elif isinstance(item, dict):
                    normalised.append(StudioQuackHelper.ResultItem(item.get("port"), item.get("value")))
        StudioQuackHelper.returnResult(normalised)

    def is_cancelled(self):
        return self.isCancelled()

    def on_cancel(self, callback):
        return self.onCancel(callback)

    def send_signal(self, signalName, scope="global"):
        return self.sendSignal(signalName, scope=scope)

    def send_message(self, messageName, correlationKey=None):
        return self.sendMessage(messageName, correlationKey=correlationKey)

    # ---- camelCase twins of the short accessors (same names as the JS/TS and Java helpers) ----
    def fullContext(self):
        return self.getExecutionContext()

    def currentNode(self):
        return self.getCurrentNode()

    def currentNodeId(self):
        return self.getCurrentNodeId()

    def workflowExecutionId(self):
        return self.getWorkflowExecutionId()

    def setRequestContext(self, execution_context=None, helper_token=None, request_id=None):
        return self.set_request_context(execution_context, helper_token=helper_token, request_id=request_id)

    def handle_cancel(self, requestId, reason=None):
        return self.handleCancel(requestId, reason)


def _unquote_dotenv(raw):
    if len(raw) >= 2 and raw[0] == '"' and raw[-1] == '"':
        body = raw[1:-1]
    else:
        return raw
    out = []
    i = 0
    mapping = {"n": "\n", "r": "\r", "t": "\t", '"': '"', "\\": "\\"}
    while i < len(body):
        ch = body[i]
        if ch == "\\" and i + 1 < len(body):
            out.append(mapping.get(body[i + 1], body[i + 1]))
            i += 2
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _decode_b64_wrapper(encoded):
    try:
        parsed = _sq_json.loads(_sq_b64.b64decode(encoded).decode("utf-8"))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _post_helper_channel_result(items):
    wire = []
    for it in items:
        if isinstance(it, StudioQuackHelper.ResultItem):
            wire.append(it.to_wire())
        elif isinstance(it, dict):
            wire.append({"port": str(it.get("port") or it.get("portValue") or ""),
                         "value": str(it.get("value") or it.get("result") or "")})
    _sq_post_json("/v1/helper/result", {"ok": True, "items": wire})


studioquack = StudioQuackHelper(execution_context=_sq_ctx)
StudioQuack = studioquack
_StudioQuack = StudioQuackHelper   # class alias for isinstance / re-instantiation


def _sq_excepthook(typ, exc, tb):
    """Convert uncaught user-script exceptions into typed /v1/helper/fail. Never swallow into output."""
    if typ is SystemExit or typ is KeyboardInterrupt or not isinstance(exc, Exception):
        return _sq_sys.__excepthook__(typ, exc, tb)
    if _sq_in_fail:
        return _sq_sys.__excepthook__(typ, exc, tb)
    try:
        if isinstance(exc, StudioQuackError):
            studioquack.fail(exc.errorCode, exc.message, exc.data)
        else:
            # Keep the stack in the container log (stderr) before folding to a typed failure:
            # the boundary handler gets the code, the operator still gets the traceback.
            _sq_sys.__excepthook__(typ, exc, tb)
            studioquack.fail(
                "SANDBOX_FAILED",
                type(exc).__name__ + ": " + str(exc),
                {"exceptionType": type(exc).__name__},
            )
    except SystemExit:
        raise
    except Exception:
        _sq_sys.__excepthook__(typ, exc, tb)


_sq_sys.excepthook = _sq_excepthook
