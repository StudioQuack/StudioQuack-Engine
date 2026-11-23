// === StudioQuack Edge Transform — CANONICAL EXAMPLES (do not fork; S5 copies this) === Copy a
// top-level attribute: target.fullName = source.userName Nested read → nested write. Typed fail /
// StudioQuackError (do not write errors onto target): studioquack.fail("VALIDATION_FAILED", "bad
// payload"); throw new StudioQuackError("VALIDATION_FAILED", "bad payload", { field: "id" }).

class TransformMissingPathError extends Error {
  constructor(message) {
    super(message);
    this.name = "TransformMissingPathError";
  }
}

class TransformDeniedError extends Error {
  constructor(message) {
    super(message);
    this.name = "TransformDeniedError";
  }
}

function _sqRequireErrorCode(errorCode) {
  const code = errorCode == null ? "" : String(errorCode).trim();
  if (!code) {
    throw new Error("errorCode is required");
  }
  if (code.toUpperCase() === "SQ_SKIP") {
    throw new Error("errorCode 'SQ_SKIP' is reserved");
  }
  return code;
}

function _sqIsStateful() {
  const v = (process.env.SQ_STATEFUL_RUNNER || "").trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

class StudioQuackError extends Error {
  constructor(errorCode, message, data) {
    const code = _sqRequireErrorCode(errorCode);
    const msg = message == null ? "" : String(message);
    super(msg || code);
    this.name = "StudioQuackError";
    this.errorCode = code;
    this.message = msg;
    this.data = data;
  }
}

globalThis.StudioQuackError = StudioQuackError;

function getSecret() {
  throw new TransformDeniedError("denied: getSecret");
}

// Blocking helper-channel transport. Bearer never on argv, never logged.

function _sqHelperWorkerMain() {
  const http = require("http");
  const https = require("https");
  const net = require("net");
  let raw = "";
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", function (c) {
    raw += c;
  });
  process.stdin.on("end", function () {
    function finish(obj) {
      try {
        process.stdout.write(JSON.stringify(obj || {}));
      } catch (_e) {
        process.stdout.write("{}");
      }
    }
    function parseBody(b) {
      if (!b) return {};
      try {
        return JSON.parse(b);
      } catch (_e) {
        return {};
      }
    }
    var spec;
    try {
      spec = JSON.parse(raw || "{}");
    } catch (_e) {
      finish({ error: "bad stdin" });
      return;
    }
    var timeoutMs = spec.timeoutMs > 0 ? spec.timeoutMs : 15000;
    var method = String(spec.method || "GET").toUpperCase();
    try {
      if (spec.socket) {
        var payload = spec.bodyText || "";
        var req =
          method +
          " " +
          String(spec.path || "/") +
          " HTTP/1.1\r\nHost: sq-helper\r\n";
        if (spec.bearer) {
          req += "Authorization: Bearer " + spec.bearer + "\r\n";
        }
        if (method !== "GET") {
          req += "Content-Type: application/json\r\n";
          req += "Content-Length: " + Buffer.byteLength(payload) + "\r\n";
        } else {
          req += "Content-Length: 0\r\n";
        }
        req += "Connection: close\r\n\r\n";
        if (method !== "GET") req += payload;
        var client = net.createConnection(spec.socket);
        client.setTimeout(timeoutMs);
        var buf = "";
        client.on("data", function (d) {
          buf += d;
        });
        client.on("end", function () {
          var split = buf.indexOf("\r\n\r\n");
          var head = split >= 0 ? buf.slice(0, split) : buf;
          var body = split >= 0 ? buf.slice(split + 4) : "";
          var m = /HTTP\/1\.\d (\d+)/.exec(head);
          finish({ status: m ? Number(m[1]) : 0, json: parseBody(body) });
        });
        client.on("error", function (e) {
          finish({ error: String((e && e.message) || e) });
        });
        client.on("timeout", function () {
          try {
            client.destroy();
          } catch (_e) {}
          finish({ error: "timeout" });
        });
        client.write(req);
        client.end();
      } else {
        var u = new URL(spec.url);
        var lib = u.protocol === "https:" ? https : http;
        var payload2 = spec.bodyText ? spec.bodyText : null;
        var headers = {};
        if (spec.headers) {
          Object.keys(spec.headers).forEach(function (k) {
            headers[k] = spec.headers[k];
          });
        }
        if (payload2 != null) headers["Content-Length"] = Buffer.byteLength(payload2);
        var r = lib.request(
          {
            method: method,
            hostname: u.hostname,
            port: u.port || (u.protocol === "https:" ? 443 : 80),
            path: (u.pathname || "/") + (u.search || ""),
            headers: headers,
          },
          function (res) {
            var b = "";
            res.on("data", function (c) {
              b += c;
            });
            res.on("end", function () {
              finish({ status: res.statusCode, json: parseBody(b) });
            });
          }
        );
        r.on("error", function (e) {
          finish({ error: String((e && e.message) || e) });
        });
        r.setTimeout(timeoutMs, function () {
          r.destroy(new Error("timeout"));
        });
        if (payload2 != null) r.write(payload2);
        r.end();
      }
    } catch (e) {
      finish({ error: String((e && e.message) || e) });
    }
  });
}

const _SQ_HELPER_WORKER = "(" + _sqHelperWorkerMain.toString() + ")();";

function _sqHelperUrl() {
  return (process.env.SQVM_HELPER_URL || "").replace(/\/+$/, "");
}

function _sqHelperRequest(method, path, body, timeoutMs) {
  const verb = String(method || "GET").toUpperCase();
  const bodyText = verb === "GET" || body === undefined || body === null ? "" : JSON.stringify(body);
  const urlBase = _sqHelperUrl();
  let spec;
  if (urlBase) {
    const headers = { Authorization: "Bearer " + (process.env.SQVM_HELPER_TOKEN || "") };
    if (verb !== "GET") headers["Content-Type"] = "application/json";
    spec = {
      method: verb,
      url: urlBase + path,
      headers: headers,
      bodyText: verb === "GET" ? "" : bodyText,
      timeoutMs: timeoutMs,
    };
  } else if (process.env.SQ_HELPER_SOCKET) {
    spec = {
      method: verb,
      path: path,
      socket: process.env.SQ_HELPER_SOCKET,
      bearer: process.env.SQ_HELPER_BEARER || process.env.SQVM_HELPER_TOKEN || "",
      bodyText: verb === "GET" ? "" : bodyText,
      timeoutMs: timeoutMs,
    };
  } else {
    return null;
  }
  try {
    const { spawnSync } = require("child_process");
    const result = spawnSync(process.execPath, ["-e", _SQ_HELPER_WORKER], {
      input: JSON.stringify(spec),
      encoding: "utf8",
      timeout: (timeoutMs > 0 ? timeoutMs : 15000) + 5000,
      windowsHide: true,
      stdio: ["pipe", "pipe", "pipe"],
    });
    if (!result || result.stdout == null) return null;
    const parsed = JSON.parse(String(result.stdout || "{}"));
    if (!parsed || parsed.error) return null;
    return parsed.json != null ? parsed.json : {};
  } catch (_e) {
    return null;
  }
}

function _sqPostJson(path, body) {
  return _sqHelperRequest("POST", path, body, 15000);
}

function _sqGetJson(path, timeoutMs) {
  return _sqHelperRequest("GET", path, undefined, timeoutMs || 5000);
}

const _sqCancelCallbacks = [];
let _sqCancelFired = false;
let _sqProcessCancelled = false;
let _sqCancelPollAt = 0;
let _sqSigtermInstalled = false;

function _sqRunCancelCallbacks() {
  if (_sqCancelFired) return;
  _sqCancelFired = true;
  const cbs = _sqCancelCallbacks.slice();
  for (let i = 0; i < cbs.length; i++) {
    try {
      cbs[i]();
    } catch (_e) {}
  }
}

function _sqMarkProcessCancelled() {
  _sqProcessCancelled = true;
  _sqRunCancelCallbacks();
}

function _sqEnsureSigterm() {
  if (_sqSigtermInstalled) return;
  _sqSigtermInstalled = true;
  try {
    process.on("SIGTERM", function () {
      _sqMarkProcessCancelled();
    });
  } catch (_e) {}
}

function _sqPollCancelled() {
  if (_sqProcessCancelled) return true;
  const now = Date.now();
  if (now - _sqCancelPollAt < 1000) return false;
  _sqCancelPollAt = now;
  const resp = _sqGetJson("/v1/helper/cancelled", 5000);
  if (resp && (resp.cancelled || resp.canceled)) {
    _sqMarkProcessCancelled();
    return true;
  }
  return false;
}

const studioquack = {
  execute() {
    throw new TransformDeniedError("denied: studioquack.execute");
  },
  get llm() {
    throw new TransformDeniedError("denied: studioquack.llm");
  },
  connect() {
    throw new TransformDeniedError("denied: studioquack.connect");
  },
  getSecret() {
    throw new TransformDeniedError("denied: getSecret");
  },
  fail(errorCode, message, data) {
    const code = _sqRequireErrorCode(errorCode);
    const msg = message == null ? "" : String(message);
    const payload = { errorCode: code };
    if (message != null) payload.message = msg;
    if (data != null) payload.data = data;
    try {
      process.stderr.write("SQ_FAIL " + code + ": " + msg + "\n");
    } catch (_e) {}
    _sqPostJson("/v1/helper/fail", payload);
    if (_sqIsStateful()) return;
    process.exit(1);
  },
  escalate(escalationCode, message, data) {
    const code = escalationCode == null ? "" : String(escalationCode).trim();
    if (!code) throw new Error("escalationCode is required");
    const body = { escalationCode: code };
    if (message != null) body.message = String(message);
    if (data != null) body.data = data;
    return _sqPostJson("/v1/helper/escalate", body);
  },
  sendSignal(signalName, scope) {
    const name = signalName == null ? "" : String(signalName).trim();
    if (!name) throw new Error("signalName is required");
    if (name.indexOf("__sq_") === 0) throw new Error("signalName must not start with '__sq_'");
    return _sqPostJson("/v1/helper/signal", { signalName: name, scope: scope || "global" });
  },
  sendMessage(messageName, correlationKey) {
    const name = messageName == null ? "" : String(messageName).trim();
    if (!name) throw new Error("messageName is required");
    const body = { messageName: name };
    if (correlationKey !== undefined && correlationKey !== null) {
      body.correlationKey = correlationKey;
    }
    return _sqPostJson("/v1/helper/message", body);
  },
  isCancelled() {
    _sqEnsureSigterm();
    if (_sqProcessCancelled) return true;
    return _sqPollCancelled();
  },
  onCancel(callback) {
    _sqEnsureSigterm();
    if (typeof callback !== "function") return;
    const already = _sqProcessCancelled;
    _sqCancelCallbacks.push(callback);
    if (already) {
      try {
        callback();
      } catch (_e) {}
      return;
    }
    _sqPollCancelled();
  },
  handleCancel(requestId, _reason) {
    _sqMarkProcessCancelled();
    return { ok: true };
  },
};

globalThis.studioquack = studioquack;

function _sqSplitPath(path) {
  if (path == null) return [];
  if (Array.isArray(path)) return path.map(String);
  const s = String(path).trim();
  if (!s) return [];
  return s.split(".").filter(Boolean);
}

function _sqResolve(root, parts, soft, defaultValue) {
  let cur = root;
  const walked = [];
  for (const part of parts) {
    walked.push(part);
    const dotted = walked.join(".");
    if (cur != null && typeof cur === "object" && !Array.isArray(cur) && Object.prototype.hasOwnProperty.call(cur, part)) {
      cur = cur[part];
    } else if (cur != null && Array.isArray(cur) && String(Number(part)) === String(part) && cur[Number(part)] !== undefined) {
      cur = cur[Number(part)];
    } else {
      if (soft) return defaultValue;
      throw new TransformMissingPathError("missing path '" + dotted + "'");
    }
  }
  return cur;
}

function _sqMakeSource(root) {
  function view(parts) {
    const current = _sqResolve(root, parts, false);
    return new Proxy(
      function () {},
      {
        get(_t, prop) {
          if (prop === "get") {
            return (path, defaultValue) =>
              _sqResolve(current, _sqSplitPath(path), true, defaultValue === undefined ? null : defaultValue);
          }
          if (prop === "valueOf" || prop === Symbol.toPrimitive) {
            return () => current;
          }
          if (prop === "toJSON") {
            return () => current;
          }
          if (prop === "then") {
            // Avoid being treated as a Promise.
            return undefined;
          }
          if (typeof prop === "symbol") {
            return undefined;
          }
          return view(parts.concat([String(prop)]));
        },
        apply() {
          return current;
        },
      }
    );
  }
  return new Proxy(
    {},
    {
      get(_t, prop) {
        if (prop === "get") {
          return (path, defaultValue) =>
            _sqResolve(root, _sqSplitPath(path), true, defaultValue === undefined ? null : defaultValue);
        }
        if (typeof prop === "symbol") return undefined;
        return view([String(prop)]);
      },
    }
  );
}

function _sqMakeTarget() {
  const state = { value: null, written: false };
  function ensureObj() {
    if (!state.written || state.value == null || typeof state.value !== "object" || Array.isArray(state.value)) {
      state.value = {};
      state.written = true;
    }
  }
  function setPath(parts, value) {
    if (!parts.length) {
      state.value = value;
      state.written = true;
      return;
    }
    // Unwrap source proxy leaves when assigned.
    if (value != null && typeof value === "function" && typeof value.valueOf === "function") {
      try {
        const unwrapped = value.valueOf();
        if (unwrapped !== value) value = unwrapped;
      } catch (_e) {
        /* keep value */
      }
    }
    ensureObj();
    let cur = state.value;
    for (let i = 0; i < parts.length - 1; i++) {
      const p = parts[i];
      if (cur[p] == null || typeof cur[p] !== "object" || Array.isArray(cur[p])) {
        cur[p] = {};
      }
      cur = cur[p];
    }
    cur[parts[parts.length - 1]] = value;
    state.written = true;
  }
  function writer(parts) {
    return new Proxy(
      {},
      {
        get(_t, prop) {
          if (typeof prop === "symbol") return undefined;
          return writer(parts.concat([String(prop)]));
        },
        set(_t, prop, value) {
          setPath(parts.concat([String(prop)]), value);
          return true;
        },
      }
    );
  }
  const api = {
    set(path, value) {
      setPath(_sqSplitPath(path), value);
    },
    replace(obj) {
      state.value = obj;
      state.written = true;
    },
    _resultOrEmpty() {
      if (!state.written || state.value === null || state.value === undefined) {
        throw new Error("EDGE_TRANSFORM_EMPTY: transform target was never written");
      }
      return state.value;
    },
  };
  return new Proxy(api, {
    get(t, prop) {
      if (prop in t) return t[prop];
      if (typeof prop === "symbol") return undefined;
      return writer([String(prop)]);
    },
    set(_t, prop, value) {
      setPath([String(prop)], value);
      return true;
    },
  });
}

function _sqPostTransformResult(value) {
  if (_sqHelperUrl() || process.env.SQ_HELPER_SOCKET) {
    _sqPostJson("/v1/helper/result", {
      ok: true,
      items: [{ portValue: "__sq_edge_transform__", result: value }],
    });
  }
  process.stdout.write("SQ_EDGE_TRANSFORM_RESULT:" + JSON.stringify(value) + "\n");
}

let _sqInbound = null;
try {
  const _enc = "__SQ_TRANSFORM_INPUT__";
  _sqInbound = _enc ? JSON.parse(Buffer.from(_enc, "base64").toString("utf-8")) : null;
} catch (_e) {
  _sqInbound = null;
}

const source = _sqMakeSource(_sqInbound);
const target = _sqMakeTarget();

try {
  // === user script injected below ===
  __SQ_TRANSFORM_USER_SCRIPT__
  // === end user script ===
  const _sqTransformResult = target._resultOrEmpty();
  _sqPostTransformResult(_sqTransformResult);
} catch (e) {
  if (e instanceof StudioQuackError) studioquack.fail(e.errorCode, e.message, e.data);
  else studioquack.fail("TRANSFORM_FAILED", (e && e.name ? e.name : "Error") + ": " + (e && e.message ? e.message : String(e)), {exceptionType: e && e.name ? e.name : "Error"});
}
