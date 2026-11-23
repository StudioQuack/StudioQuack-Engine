#!/usr/bin/env python3
"""
Sq-python-runner-client — the tiny, import-light process the daemon runs per job via
`podman exec` to reach the warm container's persistent runner. It imports nothing heavy
(only stdlib), so its own start cost is negligible; the expensive module imports live
once in the long-lived runner.
"""
import base64
import json
import os
import socket
import sys


def main():
    sock_path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/sq-python-runner.sock"
    script_name = sys.argv[2] if len(sys.argv) > 2 else "main.py"

    raw = sys.stdin.buffer.read()
    lines = raw.split(b"\n")
    script_b64 = lines[0].decode("ascii").strip() if len(lines) > 0 else ""
    env_file_b64 = lines[1].decode("ascii").strip() if len(lines) > 1 else ""
    try:
        file_count = int(lines[2].decode("ascii").strip()) if len(lines) > 2 else 0
    except ValueError:
        file_count = 0
    files = []
    for i in range(file_count):
        name_at, content_at = 3 + i * 2, 4 + i * 2
        files.append({
            "name_b64": lines[name_at].decode("ascii").strip() if len(lines) > name_at else "",
            "content_b64": lines[content_at].decode("ascii").strip() if len(lines) > content_at else "",
        })

    req = {
        "script_b64": script_b64,
        "env_file_b64": env_file_b64,
        "files": files,
        "env": {k: v for k, v in os.environ.items()},
        "script_name": script_name,
    }
    payload = (json.dumps(req) + "\n").encode("utf-8")

    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(sock_path)
    client.sendall(payload)

    chunks = []
    while True:
        chunk = client.recv(65536)
        if not chunk:
            break
        chunks.append(chunk)
    client.close()

    data = b"".join(chunks).split(b"\n", 1)[0]
    resp = json.loads(data.decode("utf-8")) if data else {}
    sys.stdout.buffer.write(base64.b64decode(resp.get("stdout_b64", "") or ""))
    sys.stdout.buffer.flush()
    sys.stderr.buffer.write(base64.b64decode(resp.get("stderr_b64", "") or ""))
    sys.stderr.buffer.flush()
    sys.exit(int(resp.get("exitCode", 1)))


if __name__ == "__main__":
    main()
