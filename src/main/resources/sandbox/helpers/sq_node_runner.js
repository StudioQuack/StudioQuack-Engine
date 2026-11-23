#!/usr/bin/env node
// Sq-node-runner — the long-lived, pre-loaded Node.js process inside a JavaScript/TypeScript warm-
// pool container (the Node counterpart of sq_python_runner.py). It requires the operator's preload
// modules ONCE at warm time, then runs the single job its container receives IN THIS PROCESS, so
// every preloaded module is already in the require cache when the script asks for it.
'use strict';

const fs = require('node:fs');
const net = require('node:net');
const path = require('node:path');
const Module = require('node:module');
const { pathToFileURL } = require('node:url');

const BASE = process.env.SQ_RUNNER_BASE || '/tmp/sq-node-runner';
const SOCKET_PATH = BASE + '.sock';
const READY_PATH = BASE + '.ready';
const LOG_PATH = BASE + '.log';
const WORKDIR = process.env.SQ_RUNNER_WORKDIR || '/sandbox';
const FILES_DIR = path.join(WORKDIR, 'files');
const PRELOADS = String(process.env.SQ_RUNNER_PRELOADS || '')
  .split(',').map((m) => m.trim()).filter(Boolean);

let jobStarted = false;
let jobRequestId = '';
// A cancel that arrives before its job (bounded; ids only, never the reason text).
const cancelledIds = [];
let cancelledBeforeAnyJob = false;

function log(line) {
  try {
    fs.appendFileSync(LOG_PATH, '[sq-node-runner] ' + line + '\n');
  } catch (_) {
    // Diagnostics are best-effort.
  }
}

function elapsedMs(t0) {
  return (Number(process.hrtime.bigint() - t0) / 1e6).toFixed(1);
}

// A package that prints while loading must not leak into the job's stdout, so console output during
// preload is redirected to the log. Native fd-level writes still reach the FIFO (rare, harmless).
async function preloadModules() {
  const stdoutWrite = process.stdout.write;
  const stderrWrite = process.stderr.write;
  const toLog = (chunk, encoding, cb) => {
    log('preload.output ' + String(chunk).trimEnd());
    const done = typeof encoding === 'function' ? encoding : cb;
    if (typeof done === 'function') done();
    return true;
  };
  process.stdout.write = toLog;
  process.stderr.write = toLog;
  let ok = 0;
  let failed = 0;
  try {
    for (const name of PRELOADS) {
      const t0 = process.hrtime.bigint();
      if (await loadModule(name)) {
        ok++;
        log('preload.ok ' + name + ' ' + elapsedMs(t0) + ' ms');
      } else {
        failed++;
        log('preload.skip ' + name);
      }
    }
  } finally {
    process.stdout.write = stdoutWrite;
    process.stderr.write = stderrWrite;
  }
  log('preload done: ok=' + ok + ' failed=' + failed);
}

// require() resolves through NODE_PATH exactly as the job's own require() will, so the job gets the
// cached instance. An ES module with top-level await refuses require() and is imported instead.
async function loadModule(name) {
  try {
    require(name);
    return true;
  } catch (e) {
    if (e && (e.code === 'ERR_REQUIRE_ASYNC_MODULE' || e.code === 'ERR_REQUIRE_ESM')) {
      try {
        await import(pathToFileURL(require.resolve(name)).href);
        return true;
      } catch (_) {
        return false;
      }
    }
    return false;
  }
}

// Decodes the daemon's dotenv rendering (SecretResolver.renderDotenv): KEY='value' with '\'' for
// an embedded quote; values may span lines.
function parseEnvFile(text) {
  const env = {};
  let i = 0;
  const n = text.length;
  while (i < n) {
    const eq = text.indexOf('=', i);
    const nl = text.indexOf('\n', i);
    if (eq < 0 || (nl >= 0 && nl < eq)) {
      i = nl < 0 ? n : nl + 1;
      continue;
    }
    const key = text.slice(i, eq).trim();
    i = eq + 1;
    let value = '';
    if (text[i] === "'") {
      while (i < n) {
        if (text[i] === "'") {
          const close = text.indexOf("'", i + 1);
          if (close < 0) {
            value += text.slice(i + 1);
            i = n;
            break;
          }
          value += text.slice(i + 1, close);
          i = close + 1;
        } else if (text[i] === '\\' && text[i + 1] === "'") {
          value += "'";
          i += 2;
        } else {
          break;
        }
      }
      const end = text.indexOf('\n', i);
      i = end < 0 ? n : end + 1;
    } else {
      const end = text.indexOf('\n', i);
      value = text.slice(i, end < 0 ? n : end).trim();
      i = end < 0 ? n : end + 1;
    }
    if (key && !key.startsWith('#')) {
      env[key] = value;
    }
  }
  return env;
}

// Same rules as the cold bootstrap: the stale files dir goes, names are plain basenames.
function writeFiles(files) {
  fs.rmSync(FILES_DIR, { recursive: true, force: true });
  if (!Array.isArray(files) || files.length === 0) {
    return;
  }
  fs.mkdirSync(FILES_DIR, { recursive: true });
  for (const f of files) {
    const name = Buffer.from(String((f && f.name_b64) || ''), 'base64').toString('utf8');
    if (!name) throw new Error('sandbox.files: empty name');
    if (name.includes('/') || name === '.' || name === '..') {
      throw new Error('sandbox.files: refused slashed name');
    }
    fs.writeFileSync(path.join(FILES_DIR, name), Buffer.from(String(f.content_b64 || ''), 'base64'));
  }
}

function requestIdOf(req) {
  const rid = typeof req.requestId === 'string' && req.requestId.trim() ? req.requestId : req.request_id;
  return typeof rid === 'string' ? rid.trim() : '';
}

function isCancel(req) {
  return req.cancel === true || (typeof req.op === 'string' && req.op.trim().toLowerCase() === 'cancel');
}

// Lays the job out as the cold bootstrap would: script + files in the tmpfs, the exact environment
// `podman exec` gave the client plus the resolved secrets (never written to disk), cwd, argv.
function prepareJob(req) {
  const scriptName = path.basename(String(req.script_name || 'main.js'));
  const scriptPath = path.join(WORKDIR, scriptName);
  fs.mkdirSync(WORKDIR, { recursive: true });
  fs.writeFileSync(scriptPath, Buffer.from(String(req.script_b64 || ''), 'base64'));
  writeFiles(req.files);
  const jobEnv = {};
  for (const [k, v] of Object.entries(req.env || {})) jobEnv[k] = String(v);
  if (req.env_file_b64) {
    Object.assign(jobEnv, parseEnvFile(Buffer.from(String(req.env_file_b64), 'base64').toString('utf8')));
  }
  for (const k of Object.keys(process.env)) delete process.env[k];
  Object.assign(process.env, jobEnv);
  process.chdir(WORKDIR);
  process.argv = [process.argv[0], scriptPath];
  return scriptPath;
}

function reply(conn, obj) {
  try {
    conn.write(JSON.stringify(obj) + '\n');
  } catch (_) {
    // The client went away; nothing to report to.
  }
}

function onRequest(server, conn, req) {
  if (isCancel(req)) {
    const rid = requestIdOf(req);
    reply(conn, { ok: true, exitCode: 0, stdout_b64: '', stderr_b64: '' });
    conn.end();
    if (!jobStarted) {
      if (rid) {
        cancelledIds.push(rid);
        if (cancelledIds.length > 64) cancelledIds.shift();
      } else {
        cancelledBeforeAnyJob = true;
      }
    } else if (!rid || !jobRequestId || rid === jobRequestId) {
      setImmediate(() => process.kill(process.pid, 'SIGTERM'));
    }
    return;
  }
  if (jobStarted) {
    reply(conn, { ok: false, error: 'sq-node-runner: this warm slot already ran its job' });
    conn.end();
    return;
  }
  jobStarted = true;
  jobRequestId = requestIdOf(req);
  try {
    prepareJob(req);
  } catch (e) {
    reply(conn, { ok: false, error: String((e && e.message) || e) });
    conn.end();
    return;
  }
  reply(conn, { ok: true, accepted: true });
  // Neither the listener nor this connection may keep the process alive: it must end when the
  // script's own work ends, exactly like `node main.js`.
  server.unref();
  conn.unref();
  const cancelled = cancelledBeforeAnyJob || (jobRequestId && cancelledIds.includes(jobRequestId));
  // Outside any try/catch, so a throwing script is a genuinely uncaught error (the helper's
  // handlers, or Node's own print-and-exit-1) - never swallowed by the runner.
  setImmediate(() => {
    Module.runMain();
    if (cancelled) process.kill(process.pid, 'SIGTERM');
  });
}

function onConnection(server, conn) {
  let buf = '';
  conn.setEncoding('utf8');
  conn.on('error', () => {});
  conn.on('data', (chunk) => {
    if (buf === null) return;
    buf += chunk;
    const nl = buf.indexOf('\n');
    if (nl < 0) return;
    const line = buf.slice(0, nl);
    buf = null;
    let req;
    try {
      req = JSON.parse(line);
    } catch (_) {
      reply(conn, { ok: false, error: 'sq-node-runner: malformed request' });
      conn.end();
      return;
    }
    onRequest(server, conn, req || {});
  });
}

async function main() {
  for (const p of [SOCKET_PATH, READY_PATH]) {
    try {
      fs.unlinkSync(p);
    } catch (_) {
      // Not there yet.
    }
  }
  // Load the heavy modules ONCE, then advertise readiness so the warm pool only hands out a slot
  // whose process is already primed.
  await preloadModules();
  const server = net.createServer((conn) => onConnection(server, conn));
  server.on('error', (e) => {
    log('server.error ' + e.message);
    process.exit(1);
  });
  server.listen(SOCKET_PATH, () => {
    try {
      fs.chmodSync(SOCKET_PATH, 0o666);
    } catch (_) {
      // Same uid as every exec in the container; the mode is belt and braces.
    }
    fs.writeFileSync(READY_PATH, 'ready\n');
  });
}

main().catch((e) => {
  log('fatal ' + String((e && e.stack) || e));
  process.exit(1);
});
