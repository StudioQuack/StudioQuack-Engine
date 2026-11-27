// StudioQuack in-sandbox Node.js helper. Prepended to the user's node script by
// GenerateScriptService; the "__SQ_CONTEXT__" token is replaced with the Base64 execution context.
//
// Typed BPMN errors / boundary events (do not return {"error":...} as a port value):
//   throw new StudioQuackError("Error_BadInput", "a and b must be numbers");
//   studioquack.fail("Error_BadInput", "a and b must be numbers");
//   studioquack.escalate("SLA_BREACH");
//   if (studioquack.isCancelled()) { /* cooperative cancel */ }
//
// Same method surface as the Python and Java helpers (context readers, execute / executeStream,
// connect, engineIP, getSecret, llm.*, llmLocal, fail / escalate / signal / message, cancel).
// Cross-engine calls (envId) need the target engine's Cross-environment grant.

let _sqCtx = {};
try {
  const _enc = "__SQ_CONTEXT__";
  _sqCtx = _enc ? JSON.parse(Buffer.from(_enc, "base64").toString("utf-8")) : {};
} catch (e) {
  _sqCtx = {};
}
if (!_sqCtx || typeof _sqCtx !== "object" || Array.isArray(_sqCtx)) {
  _sqCtx = {};
}

// Preload: require operator-flagged libraries (SQ_PRELOAD_MODULES, injected by the daemon from the
// dependency manifest's preload=true entries) before the user script, so every script finds them
// loaded. Best-effort: an unavailable or mis-named module is skipped so a stray entry never breaks
// a run.
for (const _sqPre of String(process.env.SQ_PRELOAD_MODULES || "").split(",").map((m) => m.trim()).filter(Boolean)) {
  try {
    require(_sqPre);
  } catch (e) {
    // skipped
  }
}
// __SQ_EDGE_TRANSFORM_HOOK__

function _sqEmit(obj) {
  process.stdout.write("sqEvent:::" + JSON.stringify(obj) + "\n");
}

// Raising it becomes POST /v1/helper/fail.

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

/** One OUTPUT-port binding for returnResult (Python StudioQuackHelper.ResultItem parity). */
class ResultItem {
  constructor(port, value) {
    this.portValue = String(port);
    this.port = this.portValue;
    this.result = value;
    this.value = value;
  }
}

globalThis.ResultItem = ResultItem;

// Blocking helper-channel transport (no npm deps). Bearer never goes on argv and is never logged.

/** Child body: reads {method,url|socket,headers|bearer,bodyText} from stdin. */
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
    // -1 is _SQ_NO_TIME_LIMIT (this function is serialised, so it cannot see the constant):
    // setTimeout(0) disables the socket timeout.
    var timeoutMs = spec.timeoutMs > 0 ? spec.timeoutMs : (spec.timeoutMs === -1 ? 0 : 15000);
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

/** Synchronous exchange with no time limit (a sub-process may run for hours). */
const _SQ_NO_TIME_LIMIT = -1;

function _sqHelperUrl() {
  return (process.env.SQVM_HELPER_URL || "").replace(/\/+$/, "");
}

/** Returns parsed JSON or null. Never throws, never logs the bearer. */
function _sqHelperRequest(method, path, body, timeoutMs) {
  const reply = _sqHelperExchange(method, path, body, timeoutMs);
  if (!reply) return null;
  return reply.json != null ? reply.json : {};
}

/** Blocking GET/POST returning {status, json}, or null when the channel is unreachable. */
function _sqHelperExchange(method, path, body, timeoutMs) {
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
      timeout: timeoutMs === _SQ_NO_TIME_LIMIT ? undefined : (timeoutMs > 0 ? timeoutMs : 15000) + 5000,
      windowsHide: true,
      stdio: ["pipe", "pipe", "pipe"],
    });
    if (!result || result.stdout == null) return null;
    const parsed = JSON.parse(String(result.stdout || "{}"));
    if (!parsed || parsed.error) return null;
    return parsed;
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

// Non-blocking helper-channel client for calls whose result the script awaits (llm.*, connect,
// engineIP, executeStream).

function _sqHelperTarget() {
  const base = _sqHelperUrl();
  if (base) return { url: base, bearer: process.env.SQVM_HELPER_TOKEN || "" };
  if (process.env.SQ_HELPER_SOCKET) {
    return {
      socket: process.env.SQ_HELPER_SOCKET,
      bearer: process.env.SQ_HELPER_BEARER || process.env.SQVM_HELPER_TOKEN || "",
    };
  }
  return null;
}

function _sqDescribeTarget(target) {
  return target ? target.url || target.socket : "nothing (SQ_HELPER_SOCKET and SQVM_HELPER_URL are unset)";
}

/** Sends one request and resolves to the response stream; rejects on a transport fault. */
function _sqHelperOpen(method, path, body, timeoutMs, accept) {
  return new Promise(function (resolve, reject) {
    const target = _sqHelperTarget();
    if (!target) {
      reject(new Error("no helper channel: " + _sqDescribeTarget(null)));
      return;
    }
    const verb = String(method || "GET").toUpperCase();
    const payload = verb === "GET" || body === undefined || body === null ? null : JSON.stringify(body);
    const headers = { Authorization: "Bearer " + target.bearer };
    if (payload !== null) {
      headers["Content-Type"] = "application/json";
      headers["Content-Length"] = Buffer.byteLength(payload);
    }
    if (accept) headers.Accept = accept;
    let lib = require("http");
    let opts;
    if (target.url) {
      const u = new URL(target.url + path);
      if (u.protocol === "https:") lib = require("https");
      opts = {
        method: verb,
        hostname: u.hostname,
        port: u.port || (u.protocol === "https:" ? 443 : 80),
        path: u.pathname + u.search,
        headers: headers,
      };
    } else {
      headers.Host = "sq-helper";
      opts = { method: verb, socketPath: target.socket, path: path, headers: headers };
    }
    const req = lib.request(opts, resolve);
    req.on("error", reject);
    if (timeoutMs > 0) {
      req.setTimeout(timeoutMs, function () {
        req.destroy(new Error("timed out after " + timeoutMs + " ms"));
      });
    }
    if (payload !== null) req.write(payload);
    req.end();
  });
}

/** Buffered request → {status, json, text}; rejects on a transport fault. */
function _sqHelperCall(method, path, body, timeoutMs) {
  return _sqHelperOpen(method, path, body, timeoutMs).then(function (res) {
    return new Promise(function (resolve, reject) {
      let text = "";
      res.setEncoding("utf8");
      res.on("data", function (c) {
        text += c;
      });
      res.on("end", function () {
        let json = null;
        try {
          json = text ? JSON.parse(text) : null;
        } catch (_e) {
          json = null;
        }
        resolve({ status: res.statusCode || 0, json: json, text: text });
      });
      res.on("error", reject);
    });
  });
}

/** Fail-loud call (Python _sq_request_or_raise): transport faults and non-2xx reject, naming the call. */
async function _sqRequestOrThrow(method, path, body, timeoutMs, label) {
  let reply;
  try {
    reply = await _sqHelperCall(method, path, body, timeoutMs);
  } catch (e) {
    throw new Error(
      label + ": could not reach the daemon helper channel at " + _sqDescribeTarget(_sqHelperTarget()) +
        " (" + ((e && e.message) || e) + ")"
    );
  }
  if (reply.status < 200 || reply.status >= 300) {
    throw new Error(label + " failed: status=" + reply.status + " body=" + reply.text.slice(0, 500));
  }
  return reply.json;
}

/** SSE frames ({event, data}) from a response stream; comment lines are skipped. */
async function* _sqSseFrames(res) {
  let buf = "";
  let event = "";
  let data = [];
  res.setEncoding("utf8");
  for await (const chunk of res) {
    buf += chunk;
    let nl;
    while ((nl = buf.indexOf("\n")) >= 0) {
      let line = buf.slice(0, nl);
      buf = buf.slice(nl + 1);
      if (line.endsWith("\r")) line = line.slice(0, -1);
      if (line === "") {
        if (event || data.length) yield { event: event, data: data.join("\n") };
        event = "";
        data = [];
      } else if (line.startsWith("event:")) {
        event = line.slice(6).trim();
      } else if (line.startsWith("data:")) {
        data.push(line.slice(5).replace(/^ /, ""));
      }
    }
  }
  if (event || data.length) yield { event: event, data: data.join("\n") };
}

function _sqParseMaybeJson(text) {
  try {
    return JSON.parse(text);
  } catch (_e) {
    return text;
  }
}

function _sqResolvePayloadPath(obj, path, fallback) {
  if (obj === null || obj === undefined) return fallback;
  const re = /([^.\[\]]+)|\[(\d+)\]|\[(?:"([^"]*)"|'([^']*)')\]/g;
  let current = obj;
  let matched = false;
  let m;
  while ((m = re.exec(path)) !== null) {
    matched = true;
    const prop = m[1];
    const idx = m[2];
    const dq = m[3];
    const sq = m[4];
    if (idx !== undefined) {
      const i = Number(idx);
      if (Array.isArray(current) && i >= 0 && i < current.length) {
        current = current[i];
      } else {
        return fallback;
      }
    } else {
      const key = prop !== undefined ? prop : (dq !== undefined ? dq : sq);
      if (current !== null && typeof current === "object" && Object.prototype.hasOwnProperty.call(current, key)) {
        current = current[key];
      } else {
        return fallback;
      }
    }
  }
  return matched ? current : fallback;
}

/**
 * Deliver a BPMN throw event (fail / escalate / signal / message): helper channel when reachable,
 * otherwise the equivalent `sqEvent:::{kind, ...body}` stdout marker the daemon folds at exit.
 * Returns the channel's JSON reply, or `{ok: true, deferred: true}` when the marker path was used.
 */
function _sqThrowEvent(kind, path, body) {
  const reply = _sqPostJson(path, body);
  if (reply !== null) return reply;
  _sqEmit(Object.assign({ kind: kind }, body));
  return { ok: true, deferred: true };
}

// Cancel cooperation: SIGTERM is lazy; GET /cancelled is cached-positive, ≤1/s. This preamble has
// no HTTP server — handleCancel is the inbound hook.

const _sqCancelCallbacks = [];
let _sqCancelFired = false;
let _sqProcessCancelled = false;
const _sqCancelledRequests = new Set();
let _sqCancelPollAt = 0;
let _sqSigtermInstalled = false;
let _sqCurrentRequestId = null;
let _sqInFail = false;

function _sqCurrentIsCancelled() {
  if (_sqProcessCancelled) return true;
  const rid = _sqCurrentRequestId;
  return rid != null && _sqCancelledRequests.has(String(rid));
}

function _sqRunCancelCallbacks() {
  if (_sqCancelFired) return;
  _sqCancelFired = true;
  const cbs = _sqCancelCallbacks.slice();
  for (let i = 0; i < cbs.length; i++) {
    try {
      cbs[i]();
    } catch (_e) {
      /* callbacks never throw outward */
    }
  }
}

function _sqMarkProcessCancelled() {
  _sqProcessCancelled = true;
  _sqRunCancelCallbacks();
}

function _sqMarkRequestCancelled(requestId) {
  const rid = String(requestId);
  _sqCancelledRequests.add(rid);
  if (!_sqIsStateful() || rid === String(_sqCurrentRequestId || "")) {
    _sqRunCancelCallbacks();
  }
}

function _sqEnsureSigterm() {
  if (_sqSigtermInstalled) return;
  _sqSigtermInstalled = true;
  try {
    process.on("SIGTERM", function () {
      _sqMarkProcessCancelled();
    });
  } catch (_e) {
    /* ignore */
  }
}

function _sqPollCancelled() {
  if (_sqCurrentIsCancelled()) return true;
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

function _sqHandleUncaught(err) {
  // Re-entry guard: never convert a crash into returnResult / port output.
  if (_sqInFail) {
    if (!_sqIsStateful()) {
      try {
        process.exit(1);
      } catch (_e) {}
    }
    return;
  }
  try {
    if (err instanceof StudioQuackError) {
      studioquack.fail(err.errorCode, err.message, err.data);
    } else {
      const name = err && err.name ? String(err.name) : "Error";
      const message = err && err.message != null ? String(err.message) : String(err);
      // Keep the stack in the container log before folding to a typed failure: the boundary
      // handler gets the code, the operator still gets the trace.
      try {
        process.stderr.write(String((err && err.stack) || name + ": " + message) + "\n");
      } catch (_e) {}
      studioquack.fail("SANDBOX_FAILED", name + ": " + message, { exceptionType: name });
    }
  } catch (_e) {
    if (!_sqIsStateful()) {
      try {
        process.exit(1);
      } catch (_x) {}
    }
  }
}

let _sqUnhandledInstalled = false;
function _sqInstallUnhandledOnce() {
  if (_sqUnhandledInstalled) return;
  _sqUnhandledInstalled = true;
  process.on("uncaughtException", function (err) {
    _sqHandleUncaught(err);
  });
  process.on("unhandledRejection", function (reason) {
    const err = reason instanceof Error ? reason : new Error(String(reason));
    _sqHandleUncaught(err);
  });
}

// ---------------------------------------------------------------------------
// Execution-context readers — same names and semantics as the Python and Java helpers.
// ---------------------------------------------------------------------------

function _sqEc() {
  const inner = _sqCtx.executionContext;
  return inner && typeof inner === "object" && !Array.isArray(inner) ? inner : _sqCtx;
}

function _sqObj(v) {
  return v && typeof v === "object" && !Array.isArray(v) ? v : null;
}

function _sqList(v) {
  return Array.isArray(v) ? v.slice() : [];
}

function _sqNode() {
  return _sqObj(_sqEc().currentNode) || {};
}

function _sqUpsert(node, listKey, keyField, portValue, value) {
  if (!Array.isArray(node[listKey])) node[listKey] = [];
  const list = node[listKey];
  for (let i = 0; i < list.length; i++) {
    if (_sqObj(list[i]) && list[i][keyField] === portValue) {
      list[i].value = value;
      return;
    }
  }
  const entry = {};
  entry[keyField] = portValue;
  entry.value = value;
  list.push(entry);
}

function _sqUnquoteDotenv(raw) {
  if (raw.length < 2 || raw[0] !== '"' || raw[raw.length - 1] !== '"') return raw;
  const body = raw.slice(1, -1);
  const map = { n: "\n", r: "\r", t: "\t", '"': '"', "\\": "\\" };
  let out = "";
  for (let i = 0; i < body.length; i++) {
    if (body[i] === "\\" && i + 1 < body.length) {
      out += map[body[i + 1]] !== undefined ? map[body[i + 1]] : body[i + 1];
      i++;
    } else {
      out += body[i];
    }
  }
  return out;
}

function _sqExecuteBody(portValue, nodeRequest, nodeId, envId) {
  const body = { portValue: String(portValue), payload: nodeRequest === undefined ? null : nodeRequest };
  if (nodeId != null && nodeId !== "") body.nodeId = String(nodeId);
  if (envId != null && envId !== "") body.envId = String(envId);
  return body;
}

function _sqExtractContent(envelope) {
  const env = _sqObj(envelope);
  if (!env) return "";
  if ("content" in env) return env.content == null ? "" : String(env.content);
  const first = Array.isArray(env.choices) ? _sqObj(env.choices[0]) : null;
  if (!first) return "";
  const msg = _sqObj(first.message);
  if (msg) return msg.content == null ? "" : String(msg.content);
  return first.text != null ? String(first.text) : "";
}

// Studioquack.llm — the engine's provider through the daemon proxy (the key never enters the
// container).

function _sqSplitEnv(opts) {
  const o = Object.assign({}, opts || {});
  const env = o.env !== undefined && o.env !== null ? o.env : o.envId;
  delete o.env;
  delete o.envId;
  return { o: o, env: env == null || env === "" ? null : String(env) };
}

function _sqModelBody(model, params, withRepo) {
  const body = {};
  Object.keys(params || {}).forEach(function (k) {
    if (params[k] != null) body[k] = params[k];
  });
  if (model != null) {
    if (body.model === undefined) body.model = model;
    if (body.model_id === undefined) body.model_id = model;
    if (withRepo && body.repo === undefined) body.repo = model;
  }
  return body;
}

const _sqLlm = {
  /** POST /v1/helper/llm/completion → the provider envelope ({messages | prompt, model, env, ...params}). */
  completion(opts) {
    const split = _sqSplitEnv(opts);
    const o = split.o;
    const timeoutMs = o.timeoutMs > 0 ? o.timeoutMs : 0;
    delete o.timeoutMs;
    const body = {};
    if (o.messages != null) body.messages = o.messages;
    else if (o.prompt != null) body.messages = [{ role: "user", content: o.prompt }];
    if (o.prompt != null) body.prompt = o.prompt;
    if (o.model) body.model = o.model;
    Object.keys(o).forEach(function (k) {
      if (k !== "messages" && k !== "prompt" && k !== "model" && o[k] != null) body[k] = o[k];
    });
    if (split.env) body.envId = split.env;
    return _sqRequestOrThrow("POST", "/v1/helper/llm/completion", body, timeoutMs, "studioquack.llm.completion");
  },

  /** POST /v1/helper/llm/chat/completions → assistant text, or the envelope with returnEnvelope: true. */
  async chat(opts) {
    const split = _sqSplitEnv(opts);
    const o = split.o;
    const returnEnvelope = Boolean(o.returnEnvelope);
    const timeoutMs = o.timeoutMs > 0 ? o.timeoutMs : 0;
    delete o.returnEnvelope;
    delete o.timeoutMs;
    const body = {};
    if (o.model) body.model = o.model;
    if (o.messages != null) body.messages = o.messages;
    else if (o.prompt != null) body.messages = [{ role: "user", content: o.prompt }];
    if (o.temperature != null) body.temperature = o.temperature;
    if (o.maxTokens != null) body.max_tokens = o.maxTokens;
    Object.keys(o).forEach(function (k) {
      const known = ["model", "messages", "prompt", "temperature", "maxTokens"].indexOf(k) >= 0;
      if (!known && !(k in body) && o[k] != null) body[k] = o[k];
    });
    if (split.env) body.envId = split.env;
    const envelope = await _sqRequestOrThrow(
      "POST", "/v1/helper/llm/chat/completions", body, timeoutMs, "studioquack.llm.chat");
    return returnEnvelope ? envelope : _sqExtractContent(envelope);
  },

  /** Alias of chat(). */
  local(opts) {
    return _sqLlm.chat(opts);
  },

  models() {
    return _sqRequestOrThrow("GET", "/v1/helper/llm/models", null, 30000, "studioquack.llm.models");
  },

  load(model, params) {
    return _sqRequestOrThrow("POST", "/v1/helper/llm/load", _sqModelBody(model, params, false), 0,
      "studioquack.llm.load");
  },

  unload(model, params) {
    return _sqRequestOrThrow("POST", "/v1/helper/llm/unload", _sqModelBody(model, params, false), 0,
      "studioquack.llm.unload");
  },

  /** Starts a model download; poll downloadStatus() for progress. */
  download(model, params) {
    return _sqRequestOrThrow("POST", "/v1/helper/llm/download", _sqModelBody(model, params, true), 0,
      "studioquack.llm.download");
  },

  downloadStatus(jobId) {
    const path = "/v1/helper/llm/download/status" + (jobId ? "?job_id=" + encodeURIComponent(String(jobId)) : "");
    return _sqRequestOrThrow("GET", path, null, 30000, "studioquack.llm.downloadStatus");
  },
};

// Studioquack.connect — a loopback listener inside the container relayed through the daemon to a
// NAMED stateful instance (Python _SqTunnel parity). The instance's address is never exposed.

const _sqTunnels = new Map();

async function _sqOpenStream(containerName, port) {
  const label = "studioquack.connect('" + containerName + "', " + port + ")";
  const resp = await _sqRequestOrThrow(
    "POST", "/v1/helper/connect/open", { containerName: containerName, port: port }, 30000, label);
  const streamId = resp && resp.streamId;
  if (!streamId) {
    throw new Error("studioquack.connect: the daemon returned no streamId for " + containerName + ":" + port);
  }
  return streamId;
}

function _sqCloseStream(streamId) {
  return _sqHelperCall("POST", "/v1/helper/connect/close", { streamId: streamId }, 15000).catch(function () {});
}

async function _sqServeTunnel(sock, tunnel) {
  let streamId;
  try {
    streamId = await _sqOpenStream(tunnel.containerName, tunnel.targetPort);
  } catch (_e) {
    sock.destroy();
    return;
  }
  let done = false;
  const finish = function () {
    if (done) return;
    done = true;
    _sqCloseStream(streamId);
    sock.destroy();
  };
  let uplink = Promise.resolve();
  sock.on("data", function (data) {
    uplink = uplink.then(function () {
      if (done) return undefined;
      return _sqRequestOrThrow("POST", "/v1/helper/connect/send",
        { streamId: streamId, dataB64: data.toString("base64") }, 60000, "studioquack.connect send")
        .catch(finish);
    });
  });
  // Client finished sending: give the instance's answer up to 5 s, as the Python tunnel does.
  sock.on("end", function () {
    uplink.then(function () {
      setTimeout(finish, 5000).unref();
    });
  });
  sock.on("error", finish);
  sock.on("close", finish);
  while (!done && !tunnel.closed) {
    let resp;
    try {
      resp = await _sqRequestOrThrow("POST", "/v1/helper/connect/recv",
        { streamId: streamId, timeoutMs: 2000 }, 32000, "studioquack.connect recv");
    } catch (_e) {
      finish();
      return;
    }
    const chunk = (resp && resp.dataB64) || "";
    if (chunk && !done) sock.write(Buffer.from(chunk, "base64"));
    if (resp && resp.eof) {
      sock.end();
      finish();
      return;
    }
  }
  finish();
}

async function _sqOpenTunnel(containerName, port) {
  // Probe first so a wrong name/port fails here, not later as a reset inside a driver.
  await _sqCloseStream(await _sqOpenStream(containerName, port));
  const net = require("net");
  const tunnel = {
    containerName: containerName,
    targetPort: port,
    host: "127.0.0.1",
    port: 0,
    closed: false,
    url(scheme, path) {
      return (scheme || "http") + "://" + this.host + ":" + this.port + (path || "");
    },
    close() {
      this.closed = true;
      server.close();
    },
  };
  const server = net.createServer(function (sock) {
    _sqServeTunnel(sock, tunnel);
  });
  await new Promise(function (resolve, reject) {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  // The listener alone must not keep a finished script alive.
  server.unref();
  tunnel.port = server.address().port;
  return tunnel;
}

const studioquack = {
  // ---- short accessors ----------------------------------------------------
  fullContext: () => _sqCtx,
  currentNode: () => _sqEc().currentNode,
  currentNodeId: () => studioquack.getCurrentNodeId(),
  workflowExecutionId: () => studioquack.getWorkflowExecutionId(),
  parameters: () => (Array.isArray(_sqCtx.parameters) ? _sqCtx.parameters : studioquack.getParameters()),
  connections: () => (Array.isArray(_sqCtx.connections) ? _sqCtx.connections : studioquack.getConnections()),
  inputs: () => {
    const out = {};
    studioquack.getInput().forEach((item) => {
      if (!_sqObj(item)) return;
      const key = item.targetPortValue || item.portValue;
      if (key) out[key] = item.value;
    });
    return out;
  },
  input: (key, def) => {
    const v = studioquack.inputs()[key];
    return v === undefined ? def : v;
  },

  // ---- execution context ------------------------------------------------------
  getExecutionContext: () => _sqEc(),
  setExecutionContext(ctx) {
    const next = _sqObj(ctx) ? Object.assign({}, ctx) : {};
    if (_sqObj(_sqCtx.executionContext)) _sqCtx.executionContext = next;
    else _sqCtx = next;
    return next;
  },
  updateExecutionContext(patch) {
    const ec = _sqEc();
    if (_sqObj(patch)) Object.keys(patch).forEach((k) => (ec[k] = patch[k]));
    return ec;
  },
  setExecutionContextValue(key, value) {
    const ec = _sqEc();
    if (key && typeof key === "string") ec[key] = value;
    return ec;
  },
  updateInput(portValue, value) {
    const node = _sqObj(_sqEc().currentNode);
    if (node) _sqUpsert(node, "inputList", "targetPortValue", portValue, value);
    return _sqEc();
  },
  setOutput(portValue, value) {
    const node = _sqObj(_sqEc().currentNode);
    if (node) _sqUpsert(node, "outputList", "portValue", portValue, value);
    return _sqEc();
  },
  /** POST the (patched) context to /v1/helper/transform-context and return a snapshot. */
  returnExecutionContext() {
    const snapshot = Object.assign({}, _sqEc());
    _sqPostJson("/v1/helper/transform-context", { executionContext: snapshot });
    return snapshot;
  },
  getWorkflowExecutionId() {
    const v = _sqEc().workflowExecutionId;
    return v == null ? "" : String(v);
  },
  getCurrentNode: () => Object.assign({}, _sqNode()),
  getCurrentNodeId() {
    let v = _sqNode().id;
    if (v == null) v = _sqEc().currentNodeId;
    return v == null ? "" : String(v);
  },
  getEdgeTransformListForCurrentNode: () => _sqList(_sqNode().edgeTransformListForCurrentNode),

  // ---- inputs / outputs ---------------------------------------------------------
  getInput: () => _sqList(_sqNode().inputList),
  /** Values bound to an input port (optionally from one source node); schema defaults when empty. */
  getInputValuesByPortValue(portValue, nodeId) {
    const out = [];
    studioquack.getInput().forEach((item) => {
      if (!_sqObj(item) || item.targetPortValue !== portValue) return;
      if (nodeId != null && item.nodeId !== nodeId) return;
      out.push(item.value);
    });
    return out.length ? out : studioquack.setDefaultInputValuesByPortValue(portValue);
  },
  setDefaultInputValuesByPortValue(portValue) {
    const schema = studioquack.getPortSchemaByName(portValue);
    const defaults = {};
    _sqList(schema.children).forEach((child) => {
      if (_sqObj(child) && child.shortCode) defaults[child.shortCode] = null;
    });
    return Object.keys(defaults).length ? [defaults] : [];
  },
  getOutput: () => _sqList(_sqNode().outputList),
  getSpecListForNodeOutput: () => _sqList(_sqNode().specListForNodeOutput),

  // ---- template / ports / parameters -------------------------------------------
  getTemplate: () => Object.assign({}, _sqObj(_sqNode().data) || {}),
  getPortByValue(portValue) {
    const port = _sqList(studioquack.getTemplate().ports).find((p) => _sqObj(p) && p.value === portValue);
    return port || null;
  },
  getPortValueByName(portValue) {
    const port = studioquack.getPortByValue(portValue);
    return port ? (port.value !== undefined ? port.value : {}) : {};
  },
  getPortSchemaByName(portValue) {
    const port = studioquack.getPortByValue(portValue);
    if (!port) return {};
    if (typeof port.schema === "string") {
      try {
        return _sqObj(JSON.parse(port.schema)) || {};
      } catch (_e) {
        return {};
      }
    }
    return _sqObj(port.schema) || {};
  },
  getParameters: () => _sqList(studioquack.getTemplate().configurationParameters),
  getParameterByShortCode(shortCode) {
    const p = studioquack.getParameters().find((x) => _sqObj(x) && x.shortCode === shortCode);
    return p || null;
  },
  getParameterValueByShortCode(shortCode, returnValue) {
    const p = studioquack.getParameterByShortCode(shortCode);
    if (p) return p.value !== undefined ? p.value : {};
    return returnValue === undefined ? null : returnValue;
  },

  // ---- connections -------------------------------------------------------------
  getConnections: () => _sqList(_sqNode().connectionList),
  findConnectionByPortValue(portValue) {
    const c = studioquack.getConnections().find((x) => _sqObj(x) && x.portValue === portValue);
    return c || null;
  },
  getConnectionDetailListByPortValue(portValue) {
    const c = studioquack.findConnectionByPortValue(portValue);
    return c ? _sqList(c.connectionDetailList) : [];
  },
  findFirstConnectionNodeIdByPortValue(portValue) {
    const first = studioquack.getConnectionDetailListByPortValue(portValue)[0];
    return _sqObj(first) && first.nodeId !== undefined ? first.nodeId : null;
  },
  getDucktapeConfigurations(nodeId, sourcePortName, targetPortName) {
    const currentId = studioquack.getCurrentNodeId();
    return studioquack.getEdgeTransformListForCurrentNode().filter((cfg) => {
      if (!_sqObj(cfg)) return false;
      const forward = cfg.sourceNodeId === currentId && cfg.sourcePortShortCode === sourcePortName &&
        cfg.targetNodeId === nodeId && cfg.targetPortShortCode === targetPortName;
      const backward = cfg.sourceNodeId === nodeId && cfg.sourcePortShortCode === targetPortName &&
        cfg.targetNodeId === currentId && cfg.targetPortShortCode === sourcePortName;
      return forward || backward;
    });
  },
  getConnectionDetailParameters: (detail) => (_sqObj(detail) ? _sqList(detail.configurationParameters) : []),
  getConnectionDetailParameterByShortCode(configurationParameters, shortCode) {
    const cp = _sqList(configurationParameters).find((x) => _sqObj(x) && x.shortCode === shortCode);
    return cp ? cp.value : null;
  },

  /** Secret from the per-job env (SQVM_SECRET__<KEY>) or SQVM_SECRETS_FILE. Never log the value. */
  getSecret(key) {
    if (!key || typeof key !== "string") return null;
    const envKey = "SQVM_SECRET__" + key.toUpperCase();
    if (Object.prototype.hasOwnProperty.call(process.env, envKey)) return process.env[envKey];
    const path = process.env.SQVM_SECRETS_FILE || "";
    if (!path) return null;
    try {
      const fs = require("fs");
      if (!fs.statSync(path).isFile()) return null;
      const lines = fs.readFileSync(path, "utf8").split("\n");
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        if (!line || line[0] === "#" || line.indexOf("=") < 0) continue;
        const eq = line.indexOf("=");
        if (line.slice(0, eq) === envKey) return _sqUnquoteDotenv(line.slice(eq + 1));
      }
    } catch (_e) {
      return null;
    }
    return null;
  },

  // ---- side effects --------------------------------------------------------------
  metric: (name, value) => _sqEmit({ kind: "metric", name: name, value: value }),
  log: (message, level) => _sqEmit({ kind: "log", level: level || "info", message: String(message) }),

  /**
   * EmitPartial(channel, delta, port?) pushes a chunk to a caller waiting on executeStream (POST
   * /v1/helper/emit-partial); returns true when delivered.
   */
  emitPartial(channel, delta, port) {
    if (arguments.length < 2) return _sqEmit({ kind: "partial", chunk: channel });
    const body = { channel: channel || "answer", delta: delta };
    if (port) body.port = port;
    const reply = _sqHelperExchange("POST", "/v1/helper/emit-partial", body, 15000);
    return Boolean(reply && reply.status >= 200 && reply.status < 300 && reply.json && reply.json.delivered);
  },

  /**
   * FUNCTION-port call (POST /v1/helper/execute): runs the sub-process wired to `portValue` on
   * this engine, or on engine `envId` when that engine grants Cross-environment execute. Resolves
   * to the result, or [] when the channel is unreachable or the call is refused (never rejects).
   */
  execute(portValue, nodeRequest, nodeId, envId) {
    const body = _sqExecuteBody(portValue, nodeRequest, nodeId, envId);
    const reply = _sqHelperExchange("POST", "/v1/helper/execute", body, _SQ_NO_TIME_LIMIT);
    if (!reply || !(reply.status >= 200 && reply.status < 300) || !reply.json) {
      return Promise.resolve([]);
    }
    return Promise.resolve(reply.json.result === undefined ? [] : reply.json.result);
  },

  /** Streaming execute: yields each partial, then the complete payload. Rejects on transport or error frames. */
  async *executeStream(portValue, nodeRequest, nodeId, envId) {
    const label = "studioquack.executeStream('" + String(portValue) + "')";
    let res;
    try {
      res = await _sqHelperOpen("POST", "/v1/helper/execute/stream",
        _sqExecuteBody(portValue, nodeRequest, nodeId, envId), 0, "text/event-stream");
    } catch (e) {
      throw new Error(label + ": could not reach the daemon helper channel at " +
        _sqDescribeTarget(_sqHelperTarget()) + " (" + ((e && e.message) || e) + ")");
    }
    if (res.statusCode < 200 || res.statusCode >= 300) {
      let text = "";
      res.setEncoding("utf8");
      for await (const c of res) text += c;
      throw new Error(label + " failed: status=" + res.statusCode + " body=" + text.slice(0, 500));
    }
    for await (const frame of _sqSseFrames(res)) {
      const value = _sqParseMaybeJson(frame.data);
      if (frame.event === "partial" || frame.event === "complete") yield value;
      else if (frame.event === "error") throw new Error((_sqObj(value) && value.message) || String(frame.data));
      else if (frame.event === "done") return;
    }
  },

  /** Loopback tunnel to a NAMED stateful instance → {host, port, url(scheme?, path?), close()}. */
  async connect(containerName, port) {
    const key = String(containerName) + ":" + Number(port);
    const existing = _sqTunnels.get(key);
    if (existing && !existing.closed) return existing;
    const tunnel = await _sqOpenTunnel(String(containerName), Number(port));
    _sqTunnels.set(key, tunnel);
    return tunnel;
  },

  /** Current private-network address of a same-owner engine (for Network Access Control ports). */
  async engineIP(engineId) {
    const label = "studioquack.engineIP('" + String(engineId) + "')";
    const resp = await _sqRequestOrThrow("POST", "/v1/helper/engine-ip", { engineId: String(engineId) }, 15000, label);
    if (!resp || !resp.ip) throw new Error(label + ": the daemon returned no address");
    return resp.ip;
  },

  llm: _sqLlm,

  /** Chat completion on this engine's provider; rejects on an empty messages list. Returns the envelope. */
  async llmLocal(messages, model, tools, params) {
    if (!Array.isArray(messages) || messages.length === 0) {
      throw new Error("llmLocal requires a non-empty messages array");
    }
    return _sqLlm.completion(Object.assign({}, params || {}, { messages: messages, model: model, tools: tools }));
  },

  /**
   * Bind OUTPUT ports: stdout marker per port plus POST /v1/helper/result (as Python / Java).
   * Accepts ResultItem objects, {port, value} objects, [port, value] tuples, or a {port: value} map.
   */
  returnResult: (items) => {
    const wire = (v) => (typeof v === "string" ? v : JSON.stringify(v));
    const normalised = [];
    if (items instanceof ResultItem) items = [items];
    if (items && !Array.isArray(items) && typeof items === "object") {
      Object.keys(items).forEach((port) => normalised.push({ port: port, value: wire(items[port]) }));
    } else {
      (items || []).forEach((item) => {
        if (Array.isArray(item)) {
          normalised.push({ port: String(item[0]), value: wire(item[1]) });
        } else if (_sqObj(item)) {
          const port = item.port !== undefined ? item.port : item.portValue;
          const value = item.value !== undefined ? item.value : item.result;
          normalised.push({ port: String(port), value: wire(value) });
        }
      });
    }
    normalised.forEach((item) => {
      process.stdout.write("portValue:" + item.port + ":::result:" + item.value + "\n");
    });
    _sqPostJson("/v1/helper/result", { ok: true, items: normalised });
  },

  // Typed BPMN error / boundary events — Delivery: helper channel first (POST over $SQVM_HELPER_URL
  // / $SQ_HELPER_SOCKET); when that is unreachable — a Node image carries no Python, so the in-
  // container broker that binds the socket may not be running. Exactly one of the two paths fires,
  // so a signal is never broadcast twice.

  /** POST /v1/helper/fail (or stdout marker), write SQ_FAIL to stderr; exit 1 unless SQ_STATEFUL_RUNNER. */
  fail(errorCode, message, data) {
    const code = _sqRequireErrorCode(errorCode);
    const msg = message == null ? "" : String(message);
    const payload = { errorCode: code };
    if (message != null) payload.message = msg;
    if (data != null) payload.data = data;
    try {
      process.stderr.write("SQ_FAIL " + code + ": " + msg + "\n");
    } catch (_e) {}
    _sqInFail = true;
    try {
      _sqThrowEvent("fail", "/v1/helper/fail", payload);
    } finally {
      _sqInFail = false;
    }
    if (_sqIsStateful()) return;
    process.exit(1);
  },

  /** POST /v1/helper/escalate (or stdout marker) — non-interrupting; the step keeps running. */
  escalate(escalationCode, message, data) {
    const code = escalationCode == null ? "" : String(escalationCode).trim();
    if (!code) throw new Error("escalationCode is required");
    const body = { escalationCode: code };
    if (message != null) body.message = String(message);
    if (data != null) body.data = data;
    return _sqThrowEvent("escalate", "/v1/helper/escalate", body);
  },

  /** POST /v1/helper/signal {signalName, scope} (or stdout marker). Names under __sq_ are refused. */
  sendSignal(signalName, scope) {
    const name = signalName == null ? "" : String(signalName).trim();
    if (!name) throw new Error("signalName is required");
    if (name.indexOf("__sq_") === 0) throw new Error("signalName must not start with '__sq_'");
    return _sqThrowEvent("signal", "/v1/helper/signal", { signalName: name, scope: scope || "global" });
  },

  /** POST /v1/helper/message {messageName, correlationKey?} (or stdout marker). Omit key when undefined. */
  sendMessage(messageName, correlationKey) {
    const name = messageName == null ? "" : String(messageName).trim();
    if (!name) throw new Error("messageName is required");
    const body = { messageName: name };
    if (correlationKey !== undefined && correlationKey !== null) {
      body.correlationKey = correlationKey;
    }
    return _sqThrowEvent("message", "/v1/helper/message", body);
  },

  // ---- provider webhook ingress & send_and_wait helpers -------------------------

  /** True when invoked by an inbound webhook on the node's webhook_key. */
  isWebhookInvocation() {
    const mode = _sqEc().invocationMode;
    return typeof mode === "string" && mode.trim().toLowerCase() === "ingress";
  },

  /** Returns normalized {method, path, query, queryMulti, headers, rawBody, rawBodyBase64, rawBodyEncoding, body} or null. */
  getWebhookRequest() {
    if (!studioquack.isWebhookInvocation()) return null;
    const req = _sqObj(_sqEc().webhookRequest);
    if (!req) return null;
    const out = Object.assign({}, req);
    const hdrs = {};
    if (_sqObj(req.headers)) {
      Object.keys(req.headers).forEach((k) => {
        hdrs[String(k).trim().toLowerCase()] = req.headers[k];
      });
    }
    out.headers = hdrs;
    return out;
  },

  /** Case-insensitive lookup of an inbound webhook HTTP header. */
  getWebhookHeader(name, defaultValue) {
    const fallback = defaultValue === undefined ? null : defaultValue;
    if (!name || typeof name !== "string") return fallback;
    const req = studioquack.getWebhookRequest();
    if (!req || !req.headers) return fallback;
    const key = name.trim().toLowerCase();
    return Object.prototype.hasOwnProperty.call(req.headers, key) ? req.headers[key] : fallback;
  },

  /**
   * Read parsed webhook body (JSON or application/x-www-form-urlencoded) or resolve a dot/bracket path
   * such as 'value[0].resourceData' or 'entry[0].changes[0].value.messages[0]'.
   */
  getWebhookPayload(path, defaultValue) {
    const fallback = defaultValue === undefined ? null : defaultValue;
    const req = studioquack.getWebhookRequest();
    if (!req) return fallback;
    let payload = req.body !== undefined ? req.body : null;
    if (payload === null && typeof req.rawBody === "string" && req.rawBody.trim() !== "") {
      const ct = String(studioquack.getWebhookHeader("content-type", "") || "").toLowerCase();
      if (ct.indexOf("application/x-www-form-urlencoded") >= 0) {
        const params = new URLSearchParams(req.rawBody);
        const parsed = {};
        for (const [k, v] of params.entries()) {
          if (!Object.prototype.hasOwnProperty.call(parsed, k)) parsed[k] = v;
          else if (Array.isArray(parsed[k])) parsed[k].push(v);
          else parsed[k] = [parsed[k], v];
        }
        payload = parsed;
      } else if (ct.indexOf("json") >= 0 || /^[\s]*[{\[]/.test(req.rawBody)) {
        try {
          payload = JSON.parse(req.rawBody);
        } catch (_e) {
          payload = null;
        }
      }
    }
    if (path === undefined || path === null || String(path).trim() === "") {
      return payload === null ? fallback : payload;
    }
    return _sqResolvePayloadPath(payload, String(path), fallback);
  },

  /**
   * Constant-time HMAC-SHA256 verification against exact rawBody bytes (or payloadOverride).
   * Accepts either a header name (e.g. 'X-Hub-Signature-256') or the raw signature header value.
   */
  verifyHmacSha256(secret, signatureHeader, prefix, payloadOverride) {
    if (secret == null || !signatureHeader) return false;
    const secretBuf = Buffer.isBuffer(secret) ? secret : Buffer.from(String(secret), "utf8");
    if (secretBuf.length === 0) return false;
    const hdrVal = studioquack.getWebhookHeader(String(signatureHeader));
    let candidate = String(hdrVal !== null ? hdrVal : signatureHeader).trim();
    const pfx = prefix === undefined ? "sha256=" : (prefix === null ? "" : String(prefix));
    if (pfx) {
      if (!candidate.startsWith(pfx)) return false;
      candidate = candidate.slice(pfx.length).trim();
    }
    if (!candidate) return false;
    let msgBuf;
    if (payloadOverride !== undefined && payloadOverride !== null) {
      msgBuf = Buffer.isBuffer(payloadOverride) ? payloadOverride : Buffer.from(String(payloadOverride), "utf8");
    } else {
      const req = studioquack.getWebhookRequest() || {};
      if (typeof req.rawBodyBase64 === "string" && req.rawBodyBase64.length > 0) {
        msgBuf = Buffer.from(req.rawBodyBase64, "base64");
      } else {
        msgBuf = Buffer.from(req.rawBody == null ? "" : String(req.rawBody), "utf8");
      }
    }
    const crypto = require("crypto");
    const expected = crypto.createHmac("sha256", secretBuf).update(msgBuf).digest("hex").toLowerCase();
    const expectedBuf = Buffer.from(expected, "utf8");
    const actualBuf = Buffer.from(candidate.toLowerCase(), "utf8");
    if (expectedBuf.length !== actualBuf.length) return false;
    return crypto.timingSafeEqual(expectedBuf, actualBuf);
  },

  /** Send an immediate verification handshake reply (no run start, no quota charge). */
  respondWebhookChallenge(body, status, contentType) {
    const payload = {
      status: status == null ? 200 : Number(status),
      contentType: contentType ? String(contentType) : "text/plain",
      body: body == null ? "" : String(body),
    };
    _sqThrowEvent("webhook_challenge", "/v1/helper/webhook-challenge", payload);
    if (_sqIsStateful()) return Object.assign({ ok: true }, payload);
    process.exit(0);
  },

  /** Emit 1..100 normalized inbound webhook events [{idempotencyKey?, correlationKey?, payload}] to the engine. */
  emitWebhookEvents(events) {
    let rawList;
    if (events == null) rawList = [];
    else if (Array.isArray(events)) rawList = events;
    else if (_sqObj(events)) rawList = [events];
    else throw new TypeError("emitWebhookEvents expects an array of event objects");
    const normalised = [];
    const limit = Math.min(rawList.length, 100);
    for (let i = 0; i < limit; i++) {
      const item = _sqObj(rawList[i]);
      if (!item) continue;
      const ev = {};
      const idem = item.idempotencyKey !== undefined ? item.idempotencyKey : item.idempotency_key;
      const corr = item.correlationKey !== undefined ? item.correlationKey : item.correlation_key;
      if (idem != null && String(idem).trim() !== "") ev.idempotencyKey = String(idem).trim();
      if (corr != null && String(corr).trim() !== "") ev.correlationKey = String(corr).trim();
      ev.payload = item.payload !== undefined ? item.payload : {};
      normalised.push(ev);
    }
    _sqThrowEvent("webhook_events", "/v1/helper/webhook-events", { events: normalised });
    return { ok: true, count: normalised.length };
  },

  /** Reject an unauthorized/invalid webhook cleanly (no quota charge). */
  rejectWebhook(status, reason) {
    const payload = {
      status: status == null ? 401 : Number(status),
      reason: reason ? String(reason) : "unauthorized",
    };
    _sqThrowEvent("webhook_reject", "/v1/helper/webhook-reject", payload);
    if (_sqIsStateful()) return Object.assign({ ok: true }, payload);
    process.exit(0);
  },

  /** Record the Phase-1 send_and_wait correlation key that will wake the parked reply catch. */
  setCorrelationKey(key) {
    const trimmed = key == null ? "" : String(key).trim();
    if (!trimmed) throw new Error("correlationKey is required");
    _sqThrowEvent("correlation_key", "/v1/helper/correlation-key", { correlationKey: trimmed });
    return trimmed;
  },

  isCancelled() {
    _sqEnsureSigterm();
    if (_sqCurrentIsCancelled()) return true;
    return _sqPollCancelled();
  },

  /** Register a callback run once when cancel is observed. Callbacks never throw. */
  onCancel(callback) {
    _sqEnsureSigterm();
    if (typeof callback !== "function") return;
    const already = _sqCurrentIsCancelled();
    _sqCancelCallbacks.push(callback);
    if (already) {
      try {
        callback();
      } catch (_e) {}
      return;
    }
    _sqPollCancelled();
  },

  /**
   * Stateful runner — bind the current request id so handleCancel / isCancelled target this invoke.
   * Does not replace the baked execution context.
   */
  setRequestContext(ctx) {
    if (ctx && typeof ctx === "object") {
      const rid = ctx.requestId || ctx.request_id;
      _sqCurrentRequestId = rid == null || String(rid).trim() === "" ? null : String(rid);
    } else {
      _sqCurrentRequestId = null;
    }
    if (!_sqProcessCancelled) _sqCancelFired = false;
    return studioquack;
  },

  /** True when urlPath (query ignored) is the stateful cancel route: {SQ_APP_PATH or /invoke}/cancel, or /cancel. */
  isCancelPath(urlPath) {
    const path = String(urlPath || "").split("?")[0];
    const app = (process.env.SQ_APP_PATH || "/invoke").replace(/\/+$/, "");
    return path === app + "/cancel" || path === "/cancel";
  },

  /** Inbound cancel hook (no HTTP server in this preamble). */
  handleCancel(requestId, _reason) {
    if (requestId == null || String(requestId).trim() === "") {
      _sqMarkProcessCancelled();
      return { ok: true };
    }
    const rid = String(requestId).trim();
    _sqMarkRequestCancelled(rid);
    if (_sqCurrentRequestId == null) {
      _sqMarkProcessCancelled();
    }
    return { ok: true };
  },

  /** Run fn; uncaught / rejected errors become fail(...) — never returnResult. */
  run(fn) {
    if (typeof fn !== "function") return undefined;
    try {
      const out = fn();
      if (out && typeof out.then === "function") {
        return Promise.resolve(out).then(undefined, function (err) {
          _sqHandleUncaught(err);
        });
      }
      return out;
    } catch (err) {
      _sqHandleUncaught(err);
      return undefined;
    }
  },

  wrapRequest(fn) {
    if (typeof fn !== "function") return fn;
    return function sqWrappedRequest() {
      const self = this;
      const args = arguments;
      return studioquack.run(function () {
        return fn.apply(self, args);
      });
    };
  },
};

globalThis.studioquack = studioquack;
_sqInstallUnhandledOnce();
