#!/usr/bin/env python3
"""
Sq-python-runner — the long-lived, pre-imported Python worker that lives inside a warm-
pool container. It imports the operator's preload modules ONCE at warm time, then serves
each dispatched job by forking a child that inherits those already-imported modules via
copy-on-write.
"""
import base64
import json
import os
import runpy
import select
import signal
import socket
import sys
import time
from collections import deque

SOCKET_PATH = os.environ.get("SQ_RUNNER_SOCKET", "/tmp/sq-python-runner.sock")
READY_PATH = os.environ.get("SQ_RUNNER_READY", "/tmp/sq-python-runner.ready")
PRELOADS = [m.strip() for m in (os.environ.get("SQ_RUNNER_PRELOADS", "") or "").split(",") if m.strip()]
WORKDIR = os.environ.get("SQ_RUNNER_WORKDIR", "/sandbox")

# Parent-only: the listen socket (so a cancel can be accepted while pipes drain) and the
# in-flight child. The parent never runs user code; these are only for SIGTERM + reap.
_listen_sock = None
_child_pid = None
_child_request_id = ""
# Connections accepted mid-job that were not cancels — served after the current child reaps.
_deferred = []
# Bounded so a cancel flood cannot grow RSS. Ids only; never store reason (may be user text).
_cancelled_ids = deque(maxlen=64)


def _import_candidates(name):
    """The import name to try for a manifest/package name: the raw name, then hyphen->underscore."""
    seen = []
    for cand in (name, name.replace("-", "_")):
        if cand and cand not in seen:
            seen.append(cand)
    return seen


def preload_modules():
    """Import each preload module once in the parent so forked children inherit them (COW)."""
    import importlib
    ok = failed = 0
    for name in PRELOADS:
        t0 = time.perf_counter()
        imported = False
        for cand in _import_candidates(name):
            try:
                importlib.import_module(cand)
                imported = True
                ok += 1
                sys.stderr.write("[sq-runner] preload.ok %-40s %7.1f ms\n"
                                 % (cand, (time.perf_counter() - t0) * 1000.0))
                break
            except Exception:
                continue
        if not imported:
            failed += 1
            sys.stderr.write("[sq-runner] preload.skip %s\n" % name)
    sys.stderr.write("[sq-runner] preload done: ok=%d failed=%d\n" % (ok, failed))
    sys.stderr.flush()


def _read_line(conn):
    """Read one newline-terminated JSON request off the socket (requests are a single line)."""
    chunks = []
    while True:
        chunk = conn.recv(65536)
        if not chunk:
            break
        chunks.append(chunk)
        if b"\n" in chunk:
            break
    data = b"".join(chunks).split(b"\n", 1)[0]
    return json.loads(data.decode("utf-8")) if data else None


def _parse_env_file(text):
    """
    SecretResolver.renderDotenv writes KEY='value' with '\\'' for an embedded quote, and
    values may span lines, so a plain split at '=' would hand the script the quotes as part
    of its secret.
    """
    env = {}
    text = text or ""
    i, n = 0, len(text)
    while i < n:
        eq = text.find("=", i)
        nl = text.find("\n", i)
        if eq < 0 or (0 <= nl < eq):
            i = n if nl < 0 else nl + 1
            continue
        key = text[i:eq].strip()
        i = eq + 1
        parts = []
        if i < n and text[i] == "'":
            while i < n:
                if text[i] == "'":
                    close = text.find("'", i + 1)
                    if close < 0:
                        parts.append(text[i + 1:])
                        i = n
                        break
                    parts.append(text[i + 1:close])
                    i = close + 1
                elif text.startswith("\\'", i):
                    parts.append("'")
                    i += 2
                else:
                    break
            end = text.find("\n", i)
            i = n if end < 0 else end + 1
            value = "".join(parts)
        else:
            end = text.find("\n", i)
            value = text[i:n if end < 0 else end].strip()
            i = n if end < 0 else end + 1
        if key and not key.startswith("#"):
            env[key] = value
    return env


FILES_DIR = os.path.join(WORKDIR, "files")


def _write_files(files):
    """Materialise parameter/attachment files with the cold bootstrap's rules (plain basenames)."""
    import shutil
    shutil.rmtree(FILES_DIR, ignore_errors=True)
    if not files:
        return
    os.makedirs(FILES_DIR, exist_ok=True)
    for f in files:
        name = base64.b64decode((f or {}).get("name_b64") or "").decode("utf-8")
        if not name:
            raise ValueError("sandbox.files: empty name")
        if "/" in name or name in (".", ".."):
            raise ValueError("sandbox.files: refused slashed name")
        with open(os.path.join(FILES_DIR, name), "wb") as out:
            out.write(base64.b64decode(f.get("content_b64") or ""))


def _request_id(req):
    """Accept camelCase or snake_case; blank/non-str is ignored."""
    if not isinstance(req, dict):
        return ""
    rid = req.get("requestId")
    if not isinstance(rid, str) or not rid.strip():
        rid = req.get("request_id")
    return rid.strip() if isinstance(rid, str) else ""


def _is_cancel_req(req):
    """True for {"op":"cancel"} or {"cancel":true} — existing request-object style."""
    if not isinstance(req, dict):
        return False
    if req.get("cancel") is True:
        return True
    op = req.get("op")
    return isinstance(op, str) and op.strip().lower() == "cancel"


def _record_cancelled(rid):
    if rid and rid not in _cancelled_ids:
        _cancelled_ids.append(rid)


def _already_cancelled(rid):
    return bool(rid) and rid in _cancelled_ids


def _sigterm_child(pid):
    """Best-effort SIGTERM; ESRCH means the child already exited."""
    if not pid:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except (ProcessLookupError, OSError):
        pass


def _apply_cancel(req, pid=None):
    """
    When both the cancel and the in-flight job named an id and they disagree, only record —
    do not SIGTERM the next job on a stale cancel.
    """
    rid = _request_id(req)
    _record_cancelled(rid)
    target = pid if pid is not None else _child_pid
    if rid and _child_request_id and rid != _child_request_id:
        return
    _sigterm_child(target)


def _cancel_ok_resp():
    # Same shape as a job response plus ok=true so a naive client can still read exitCode.
    return {"ok": True, "exitCode": 0, "stdout_b64": "", "stderr_b64": ""}


def _send_json(conn, obj):
    conn.sendall((json.dumps(obj) + "\n").encode("utf-8"))


def _exit_code_from_system_exit(se):
    return se.code if isinstance(se.code, int) else (0 if se.code is None else 1)


def _ns_attr(ns, name):
    return getattr(ns, name, None) if ns is not None else None


def _is_studioquack_error(exc, ns):
    """Duck-type: errorCode attr, class name, or isinstance against ns.StudioQuackError."""
    if hasattr(exc, "errorCode"):
        return True
    if type(exc).__name__ == "StudioQuackError":
        return True
    cls = _ns_attr(ns, "StudioQuackError")
    if cls is None:
        sq = _ns_attr(ns, "studioquack")
        cls = getattr(sq, "StudioQuackError", None) if sq is not None else None
    if cls is None:
        return False
    try:
        return isinstance(exc, cls)
    except TypeError:
        return False


def _resolve_fail(ns):
    """studioquack.fail, else a module-level fail. Missing → None (traceback fallback)."""
    sq = _ns_attr(ns, "studioquack")
    fail = getattr(sq, "fail", None) if sq is not None else None
    if not callable(fail):
        fail = _ns_attr(ns, "fail")
    return fail if callable(fail) else None


def _fold_uncaught(exc):
    """
    Never write portValue / {"error":...}. SystemExit from fail is the intended exit — do
    not print a second traceback.
    """
    ns = sys.modules.get("__main__")
    fail = _resolve_fail(ns)
    if fail is None:
        import traceback
        traceback.print_exc()
        sys.stderr.flush()
        os._exit(1)
    if not _is_studioquack_error(exc, ns) and not isinstance(exc, Exception):
        # KeyboardInterrupt / GeneratorExit: not a script Exception — do not invent a typed code.
        import traceback
        traceback.print_exc()
        sys.stderr.flush()
        os._exit(1)
    try:
        if _is_studioquack_error(exc, ns):
            code = getattr(exc, "errorCode", None) or type(exc).__name__
            message = getattr(exc, "message", None)
            if message is None:
                message = str(exc)
            data = getattr(exc, "data", None)
            fail(code, message, data)
        else:
            fail("SANDBOX_FAILED", "%s: %s" % (type(exc).__name__, exc),
                 {"exceptionType": type(exc).__name__})
    except SystemExit as se:
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(_exit_code_from_system_exit(se))
    except BaseException:
        pass  # fail itself blew up; still exit 1 without a second traceback
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(1)


def run_child(req):
    """Fork a child to run one job; return (exitCode, stdout_bytes, stderr_bytes)."""
    script_name = req.get("script_name") or "main.py"
    script_b64 = req.get("script_b64") or ""
    env_overlay = req.get("env") or {}
    env_file = _parse_env_file(base64.b64decode(req.get("env_file_b64") or "").decode("utf-8")
                               if req.get("env_file_b64") else "")

    script_path = os.path.join(WORKDIR, script_name)
    with open(script_path, "wb") as f:
        f.write(base64.b64decode(script_b64))
    _write_files(req.get("files"))

    out_r, out_w = os.pipe()
    err_r, err_w = os.pipe()
    pid = os.fork()
    if pid == 0:  # child
        try:
            os.close(out_r)
            os.close(err_r)
            os.dup2(out_w, 1)
            os.dup2(err_w, 2)
            os.close(out_w)
            os.close(err_w)
            # Per-job env overlay: the dispatch env (helper token/url, PYTHONPATH, ...) then the
            # resolved secrets. Applied only in the child so the warm parent stays clean between jobs.
            for k, v in env_overlay.items():
                os.environ[str(k)] = str(v)
            for k, v in env_file.items():
                os.environ[str(k)] = str(v)
            os.chdir(WORKDIR)
            if WORKDIR not in sys.path:
                sys.path.insert(0, WORKDIR)
            runpy.run_path(script_path, run_name="__main__")
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(0)
        except SystemExit as se:
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(_exit_code_from_system_exit(se))
        except BaseException as exc:
            # Typed fail via the preamble — never a stdout portValue / {"error":...} line.
            _fold_uncaught(exc)
            os._exit(1)

    # parent — track the child so a concurrent cancel connection can SIGTERM it.
    global _child_pid, _child_request_id
    os.close(out_w)
    os.close(err_w)
    _child_pid = pid
    _child_request_id = _request_id(req)
    try:
        if _already_cancelled(_child_request_id):
            _sigterm_child(pid)
        stdout, stderr, status = _wait_child(pid, out_r, err_r)
    finally:
        _child_pid = None
        _child_request_id = ""
    code = os.waitstatus_to_exitcode(status) if hasattr(os, "waitstatus_to_exitcode") else (
        os.WEXITSTATUS(status) if os.WIFEXITED(status) else 1)
    return code, stdout, stderr


def _accept_mid_job(pid):
    """Accept one backlog connection while a child runs: cancel now, or defer a job."""
    try:
        conn, _ = _listen_sock.accept()
    except OSError:
        return
    try:
        req = _read_line(conn)
        if req and _is_cancel_req(req):
            _apply_cancel(req, pid)
            _send_json(conn, _cancel_ok_resp())
            conn.close()
            return
        # Preserve pipelined jobs: do not reject, serve after this child reaps.
        _deferred.append((conn, req))
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


def _wait_child(pid, out_r, err_r):
    """Drain both pipes (select) and accept cancel ops until the child closes stdout/stderr."""
    stdout = bytearray()
    stderr = bytearray()
    fds = {out_r, err_r}
    while fds:
        watch = list(fds)
        if _listen_sock is not None:
            watch.append(_listen_sock)
        try:
            ready, _, _ = select.select(watch, [], [])
        except InterruptedError:
            continue
        if _listen_sock is not None and _listen_sock in ready:
            _accept_mid_job(pid)
        for fd in (out_r, err_r):
            if fd not in fds or fd not in ready:
                continue
            try:
                chunk = os.read(fd, 65536)
            except InterruptedError:
                continue
            if not chunk:
                os.close(fd)
                fds.discard(fd)
            elif fd == out_r:
                stdout.extend(chunk)
            else:
                stderr.extend(chunk)
    while True:
        try:
            _, status = os.waitpid(pid, 0)
            break
        except InterruptedError:
            continue
    return bytes(stdout), bytes(stderr), status


def handle_conn(conn, req=None):
    try:
        if req is None:
            req = _read_line(conn)
        if not req:
            return
        if _is_cancel_req(req):
            _apply_cancel(req)
            resp = _cancel_ok_resp()
        else:
            code, stdout, stderr = run_child(req)
            resp = {
                "exitCode": code,
                "stdout_b64": base64.b64encode(stdout).decode("ascii"),
                "stderr_b64": base64.b64encode(stderr).decode("ascii"),
            }
    except Exception as exc:  # never let one bad job kill the runner
        resp = {
            "exitCode": 1,
            "stdout_b64": "",
            "stderr_b64": base64.b64encode(("sq-runner error: " + repr(exc)).encode("utf-8")).decode("ascii"),
        }
    try:
        _send_json(conn, resp)
    except Exception:
        pass


def serve():
    global _listen_sock
    for p in (SOCKET_PATH, READY_PATH):
        try:
            os.unlink(p)
        except FileNotFoundError:
            pass
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(SOCKET_PATH)
    try:
        os.chmod(SOCKET_PATH, 0o666)
    except OSError:
        pass
    srv.listen(64)
    _listen_sock = srv

    # Import the heavy modules ONCE, then advertise readiness so the warm pool only hands out a slot
    # whose interpreter is already primed.
    preload_modules()
    with open(READY_PATH, "w", encoding="utf-8") as f:
        f.write("ready\n")

    while True:
        if _deferred:
            conn, req = _deferred.pop(0)
            with conn:
                handle_conn(conn, req)
            continue
        ready, _, _ = select.select([srv], [], [])
        if srv in ready:
            conn, _ = srv.accept()
            with conn:
                handle_conn(conn)


if __name__ == "__main__":
    serve()
