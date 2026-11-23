# === StudioQuack Edge Transform — CANONICAL EXAMPLES (do not fork; S5 copies this) === Copy a
# top-level attribute: target.fullName = source.userName Nested read → nested write. Typed author
# failures: raise StudioQuackError("CODE", "msg") or studioquack.fail("CODE", "msg").
import base64 as _sq_b64
import json as _sq_json
import os as _sq_os
import sys as _sq_sys
import socket as _sq_socket
import http.client as _sq_http_client
import urllib.request as _sq_url


class TransformMissingPathError(Exception):
    """Raised when source.<path> is absent; message includes the dotted path."""


class TransformDeniedError(Exception):
    """Raised for denied node-SDK / secret / connect / file surfaces on transforms."""


def _sq_require_error_code(errorCode):
    code = "" if errorCode is None else str(errorCode).strip()
    if not code:
        raise ValueError("errorCode is required")
    if code.upper() == "SQ_SKIP":
        raise ValueError("errorCode 'SQ_SKIP' is reserved")
    return code


class StudioQuackError(Exception):
    """Typed transform error. Raising it ends the hop as POST /v1/helper/fail."""

    def __init__(self, errorCode, message=None, data=None):
        self.errorCode = _sq_require_error_code(errorCode)
        self.message = "" if message is None else str(message)
        if data is not None and not isinstance(data, dict):
            raise ValueError("data must be a dict or None")
        self.data = data
        Exception.__init__(self, self.message or self.errorCode)


def getSecret(*_a, **_k):
    raise TransformDeniedError("denied: getSecret")


class _DeniedLlm(object):
    def __getattr__(self, _name):
        raise TransformDeniedError("denied: studioquack.llm")

    def __call__(self, *_a, **_k):
        raise TransformDeniedError("denied: studioquack.llm")


class _DeniedStudioQuack(object):
    def execute(self, *_a, **_k):
        raise TransformDeniedError("denied: studioquack.execute")

    @property
    def llm(self):
        return _DeniedLlm()

    def connect(self, *_a, **_k):
        raise TransformDeniedError("denied: studioquack.connect")

    def getSecret(self, *_a, **_k):
        raise TransformDeniedError("denied: getSecret")


class _TransformHelper(_DeniedStudioQuack):
    """Restricted helper: denials stay; fail is the only extra surface."""

    def fail(self, errorCode, message=None, data=None):
        code = _sq_require_error_code(errorCode)
        msg = "" if message is None else str(message)
        payload = {"errorCode": code}
        if message is not None:
            payload["message"] = msg
        if data is not None:
            payload["data"] = data
        _sq_post_json("/v1/helper/fail", payload)
        _sq_sys.stderr.write("SQ_FAIL " + code + ": " + msg + "\n")
        _sq_sys.stderr.flush()
        _sq_sys.exit(1)


studioquack = _TransformHelper()


def _sq_split_path(path):
    if path is None:
        return []
    if isinstance(path, (list, tuple)):
        return [str(p) for p in path]
    s = str(path).strip()
    if not s:
        return []
    return [p for p in s.split(".") if p]


def _sq_resolve(root, parts, soft, default=None):
    cur = root
    walked = []
    for part in parts:
        walked.append(part)
        dotted = ".".join(walked)
        if isinstance(cur, dict):
            if part not in cur:
                if soft:
                    return default
                raise TransformMissingPathError("missing path '%s'" % dotted)
            cur = cur[part]
        else:
            if soft:
                return default
            raise TransformMissingPathError("missing path '%s'" % dotted)
    return cur


class _SqPathWriter(object):
    """Proxy that supports target.a.b = v nested assignment with auto-create."""

    def __init__(self, owner, parts):
        self._owner = owner
        self._parts = list(parts)

    def __getattr__(self, name):
        if name.startswith("_"):
            return object.__getattribute__(self, name)
        return _SqPathWriter(self._owner, self._parts + [name])

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        self._owner._set(self._parts + [name], value)


class _TransformSource(object):
    def __init__(self, value):
        self._root = value

    def __getattr__(self, name):
        if name.startswith("_"):
            return object.__getattribute__(self, name)
        return _TransformSourceView(self._root, [name])

    def __getitem__(self, key):
        return _TransformSourceView(self._root, [str(key)])

    def get(self, path, default=None):
        return _sq_resolve(self._root, _sq_split_path(path), True, default)


class _TransformSourceView(object):
    def __init__(self, root, parts):
        self._root = root
        self._parts = list(parts)
        # Eager hard-read so missing intermediate paths fail at attribute access time.
        self._value = _sq_resolve(root, self._parts, False)

    def __getattr__(self, name):
        if name.startswith("_"):
            return object.__getattribute__(self, name)
        return _TransformSourceView(self._root, self._parts + [name])

    def __getitem__(self, key):
        return _TransformSourceView(self._root, self._parts + [str(key)])

    def get(self, path, default=None):
        return _sq_resolve(self._value, _sq_split_path(path), True, default)

    def __bool__(self):
        return bool(self._value)

    def __eq__(self, other):
        return self._value == other

    def __ne__(self, other):
        return self._value != other

    def __repr__(self):
        return repr(self._value)

    def __str__(self):
        return str(self._value)

    def __int__(self):
        return int(self._value)

    def __float__(self):
        return float(self._value)

    def __iter__(self):
        return iter(self._value)

    def __len__(self):
        return len(self._value)

    @property
    def value(self):
        return self._value


class _TransformTarget(object):
    def __init__(self):
        self._value = None
        self._written = False

    def __getattr__(self, name):
        if name.startswith("_"):
            return object.__getattribute__(self, name)
        return _SqPathWriter(self, [name])

    def __setattr__(self, name, value):
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        self._set([name], value)

    def set(self, path, value):
        self._set(_sq_split_path(path), value)

    def replace(self, obj):
        self._value = obj
        self._written = True

    def _set(self, parts, value):
        if isinstance(value, _TransformSourceView):
            value = value._value
        if not parts:
            self.replace(value)
            return
        if not self._written or self._value is None or not isinstance(self._value, dict):
            self._value = {}
            self._written = True
        cur = self._value
        for part in parts[:-1]:
            nxt = cur.get(part)
            if not isinstance(nxt, dict):
                nxt = {}
                cur[part] = nxt
            cur = nxt
        cur[parts[-1]] = value
        self._written = True

    def _result_or_empty(self):
        if not self._written or self._value is None:
            raise RuntimeError("EDGE_TRANSFORM_EMPTY: transform target was never written")
        return self._value


class _SqUnixConnection(_sq_http_client.HTTPConnection):
    def connect(self):
        self.sock = _sq_socket.socket(_sq_socket.AF_UNIX, _sq_socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(_sq_os.environ.get("SQ_HELPER_SOCKET", ""))


class _SqUnixHandler(_sq_url.HTTPHandler):
    def http_open(self, req):
        return self.do_open(_SqUnixConnection, req)


_sq_opener = _sq_url.build_opener(_SqUnixHandler)


def _sq_post_json(path, body, timeout=15.0):
    """Minimal AF_UNIX POST; never raises; returns parsed JSON or None."""
    sock = _sq_os.environ.get("SQ_HELPER_SOCKET")
    if not sock:
        return None
    try:
        req = _sq_url.Request(
            "http://sq-helper" + path,
            data=_sq_json.dumps(body).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer " + (
                    _sq_os.environ.get("SQ_HELPER_BEARER")
                    or _sq_os.environ.get("SQVM_HELPER_TOKEN") or ""),
            },
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


def _sq_post_transform_result(value):
    """Post transform result on helper channel when available; always print stdout marker."""
    if _sq_os.environ.get("SQ_HELPER_SOCKET"):
        _sq_post_json("/v1/helper/result", {
            "ok": True,
            "items": [{"portValue": "__sq_edge_transform__", "result": value}],
        })
    # Unit tests / harvest path — single line, no other payload dumps.
    _sq_sys.stdout.write("SQ_EDGE_TRANSFORM_RESULT:" + _sq_json.dumps(value) + "\n")
    _sq_sys.stdout.flush()


_SQ_TRANSFORM_INPUT_B64 = "__SQ_TRANSFORM_INPUT__"
try:
    _sq_inbound = _sq_json.loads(
        _sq_b64.b64decode(_SQ_TRANSFORM_INPUT_B64).decode("utf-8")
    ) if _SQ_TRANSFORM_INPUT_B64 else None
except Exception:
    _sq_inbound = None

source = _TransformSource(_sq_inbound)
target = _TransformTarget()


def _sq_transform_excepthook(typ, exc, tb):
    """Uncaught user-script errors become fail(...). Body stays column-0 for raw inject."""
    if typ is SystemExit or typ is KeyboardInterrupt or not isinstance(exc, Exception):
        return _sq_sys.__excepthook__(typ, exc, tb)
    try:
        if isinstance(exc, StudioQuackError):
            studioquack.fail(exc.errorCode, exc.message, exc.data)
        else:
            studioquack.fail(
                "TRANSFORM_FAILED",
                type(exc).__name__ + ": " + str(exc),
                {"exceptionType": type(exc).__name__},
            )
    except SystemExit:
        raise
    except Exception:
        _sq_sys.__excepthook__(typ, exc, tb)


_sq_sys.excepthook = _sq_transform_excepthook

# === user script injected below (GenerateScriptService) ===
__SQ_TRANSFORM_USER_SCRIPT__
# === end user script ===

try:
    _sq_post_transform_result(target._result_or_empty())
except StudioQuackError as e:
    studioquack.fail(e.errorCode, e.message, e.data)
except Exception as e:
    studioquack.fail(
        "TRANSFORM_FAILED",
        type(e).__name__ + ": " + str(e),
        {"exceptionType": type(e).__name__},
    )
