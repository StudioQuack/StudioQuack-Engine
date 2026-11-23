#!/usr/bin/env python3
"""HTTP-over-UDS helper broker; stdin/stdout are newline-delimited daemon frames."""
import base64
import http.server
import json
import os
import queue
import socketserver
import stat
import sys
import threading
import time

SOCKET_PATH = os.environ.get("SQ_HELPER_SOCKET", "")
PREFIX = "/v1/helper/"
# No wait limit on a relayed request: an LLM generation or a parked studioquack.execute may take
# hours.
BROKER_VERSION = "3"
# Chunk the base64 body so no frame ever nears the pipe capacity, like the small log frames.
FRAME_BODY_SAFE = 16384  # base64 chars per frame -> each wire frame stays ~17 KiB
_pending, _pending_lock, _stdout_lock = {}, threading.Lock(), threading.Lock()
_next_id = 0
# Set once the daemon side of stdin is gone: nothing will ever answer a parked request.
_pipe_closed = threading.Event()
_PIPE_CLOSED_BODY = json.dumps({
    "error": "helper_channel_closed",
    "detail": "the engine side of the helper channel closed before answering",
}).encode("utf-8")


def _log(message):
    """Never write diagnostics, headers, bodies, or bearers to stdout."""
    sys.stderr.write("[sq-helper-broker] " + message + "\n")
    sys.stderr.flush()


def _id():
    global _next_id
    with _pending_lock:
        _next_id += 1
        return str(_next_id)


def _frame(frame):
    """
    The write loops on the binary buffer: through podman exec the broker's stdout is relayed
    by conmon through a 64 KiB pipe, and a single TextIOWrapper.write of a larger frame
    short-writes under ``python3 -u``. Short writes and BlockingIOError (non-blocking relay)
    are both retried until the whole frame is out.
    """
    data = (json.dumps(frame, separators=(",", ":")) + "\n").encode("utf-8")
    with _stdout_lock:
        out = sys.stdout.buffer
        view = memoryview(data)
        while view:
            try:
                written = out.write(view)
            except BlockingIOError:
                written = 0
            if not written:
                time.sleep(0.02)
                continue
            view = view[written:]
        out.flush()


def _send_request(frame, body_b64):
    """
    A body whose base64 would push the frame near the pipe capacity is split: a header frame
    (empty body_b64 + body_chunks=N) followed by N ordered {id, body_part} frames. The
    daemon reassembles by id; the single ordered pipe plus the pump-thread reassembly keep
    the parts in order.
    """
    if len(body_b64) <= FRAME_BODY_SAFE:
        frame["body_b64"] = body_b64
        _frame(frame)
        return
    parts = [body_b64[i:i + FRAME_BODY_SAFE] for i in range(0, len(body_b64), FRAME_BODY_SAFE)]
    frame["body_b64"] = ""
    frame["body_chunks"] = len(parts)
    _frame(frame)
    for part in parts:
        _frame({"id": frame["id"], "body_part": part})


def _path_without_query(path):
    if not path:
        return ""
    q = path.find("?")
    return path if q < 0 else path[:q]


def _decode_b64(value):
    if not isinstance(value, str):
        raise ValueError()
    return base64.b64decode(value, validate=True)


def _responses():
    """The only stdin reader; malformed and late frames cannot corrupt another request."""
    for line in sys.stdin:
        try:
            frame = json.loads(line)
            request_id, status = frame["id"], frame["status"]
            if not isinstance(request_id, str) or not isinstance(status, int):
                raise ValueError()
            stream = "chunk_b64" in frame or "done" in frame
            if stream:
                chunk = _decode_b64(frame.get("chunk_b64") or "")
                item = {
                    "status": status,
                    "chunk": chunk,
                    "done": bool(frame.get("done", False)),
                    "contentType": frame.get("contentType") or "",
                }
            else:
                body_b64 = frame["body_b64"]
                if not isinstance(body_b64, str):
                    raise ValueError()
                item = None
                body = _decode_b64(body_b64)
        except Exception:
            _log("dropped malformed response frame")
            continue
        with _pending_lock:
            waiter = _pending.get(request_id)
        if waiter is None:
            _log("dropped response for unknown request")
            continue
        if stream:
            waiter["chunks"].put(item)
            waiter["event"].set()
        else:
            waiter["status"], waiter["body"] = status, body
            waiter["event"].set()
    _log("response pipe closed")
    _release_pending()


def _release_pending():
    """The daemon is gone: answer every parked request instead of letting it wait forever."""
    _pipe_closed.set()
    with _pending_lock:
        waiters = list(_pending.values())
    for waiter in waiters:
        if waiter["event"].is_set():
            # Mid-stream: end the body so the SDK sees the stream close.
            waiter["chunks"].put({"status": 502, "chunk": b"", "done": True,
                                  "contentType": "text/event-stream"})
        else:
            waiter["status"], waiter["body"] = 502, _PIPE_CLOSED_BODY
            waiter["event"].set()


class _Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version, server_version, sys_version = "HTTP/1.1", "StudioQuackHelperBroker/1", ""

    def log_message(self, _format, *_args):
        return

    def do_GET(self): self._relay()
    def do_POST(self): self._relay()
    def do_PUT(self): self._relay()
    def do_DELETE(self): self._relay()
    def do_PATCH(self): self._relay()

    def _relay(self):
        if not self.path.startswith(PREFIX) or not self.path[len(PREFIX):]:
            return self._reply(404, b"")
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 0: raise ValueError()
            body = self.rfile.read(length)
        except Exception:
            return self._reply(400, b"")
        auth = self.headers.get("Authorization", "")
        path = self.path[len(PREFIX):]
        request_id = _id()
        waiter = {
            "event": threading.Event(),
            "status": 502,
            "body": b"",
            "chunks": queue.Queue(),
            "path": path,
        }
        with _pending_lock:
            _pending[request_id] = waiter
        if _pipe_closed.is_set():
            with _pending_lock:
                _pending.pop(request_id, None)
            return self._reply(502, _PIPE_CLOSED_BODY)
        try:
            frame = {"id": request_id, "method": self.command, "path": path,
                     "bearer": auth[len("Bearer "):] if auth.startswith("Bearer ") else ""}
            # Optional; absent means the caller did not send Accept (buffered llm helpers).
            accept = (self.headers.get("Accept") or "").strip()
            if accept:
                frame["accept"] = accept
            _send_request(frame, base64.b64encode(body).decode("ascii"))
            waiter["event"].wait()
            if not waiter["chunks"].empty():
                self._stream(waiter)
            else:
                self._reply(waiter["status"], waiter["body"])
        except (BrokenPipeError, ConnectionError, OSError):
            return
        except Exception:
            _log("request relay failed")
            try:
                self._reply(502, b"")
            except Exception:
                return
        finally:
            with _pending_lock:
                _pending.pop(request_id, None)

    def _stream(self, waiter):
        """Write SSE/chunked body: headers on first frame, close on done=true."""
        headers_sent = False
        while True:
            item = waiter["chunks"].get()
            ct = item["contentType"] or "text/event-stream"
            if not headers_sent:
                # A single done=true JSON error stays Content-Length (byte-familiar to SDKs).
                if item["done"] and not ct.lower().startswith("text/event-stream"):
                    return self._reply(item["status"], item["chunk"])
                self.send_response(item["status"])
                self.send_header("Content-Type", ct)
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                headers_sent = True
            chunk = item["chunk"]
            if chunk:
                self.wfile.write(chunk)
                self.wfile.flush()
            if item["done"]:
                return

    def _reply(self, status, body):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if body:
            self.wfile.write(body)


def main():
    if not SOCKET_PATH:
        _log("SQ_HELPER_SOCKET is not set")
        return 2
    try:
        if stat.S_ISSOCK(os.lstat(SOCKET_PATH).st_mode):
            os.unlink(SOCKET_PATH)
        else:
            raise RuntimeError("socket path is not a socket")
    except FileNotFoundError:
        pass
    server = _Server(SOCKET_PATH, _Handler)
    os.chmod(SOCKET_PATH, 0o666)
    # Version banner on stderr (never stdout): the daemon WARN-drains it, so the daemon log
    # proves which broker build a given container is actually running.
    _log(f"broker up version={BROKER_VERSION} socket={SOCKET_PATH}")
    threading.Thread(target=_responses, daemon=True).start()
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
        try:
            os.unlink(SOCKET_PATH)
        except FileNotFoundError:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
