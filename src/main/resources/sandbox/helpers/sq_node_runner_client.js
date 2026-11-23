#!/usr/bin/env node
// It loads nothing but Node built-ins, so its own start cost is the bare interpreter.
// Contract identical to the cold bootstrap (ContainerBootstrap), so the daemon's capture path is
// unchanged.
'use strict';

const fs = require('node:fs');
const net = require('node:net');
const { spawn } = require('node:child_process');

const BASE = process.argv[2] || '/tmp/sq-node-runner';
const SCRIPT_NAME = process.argv[3] || 'main.js';
// The runner's sh parent writes <base>.exit right after the job process ends; the FIFOs reach EOF a
// moment earlier, so the status file is polled briefly.
const EXIT_WAIT_MS = 5000;

function parsePayload(buf) {
  const lines = buf.toString('latin1').split('\n');
  const count = parseInt((lines[2] || '').trim(), 10) || 0;
  const files = [];
  for (let i = 0; i < count; i++) {
    files.push({ name_b64: (lines[3 + i * 2] || '').trim(), content_b64: (lines[4 + i * 2] || '').trim() });
  }
  return { script_b64: (lines[0] || '').trim(), env_file_b64: (lines[1] || '').trim(), files };
}

// Each FIFO is drained by a `cat` writing straight to our stdout/stderr. A FIFO read inside Node
// parks a thread-pool thread that blocks process exit until EOF; `cat` ends at EOF (the job
// process and anything it spawned are gone) and can simply be killed when no job ran.
function drain(fifoPath, destFd) {
  const fd = fs.openSync(fifoPath, 'r');
  const cat = spawn('cat', [], { stdio: [fd, destFd, 'inherit'] });
  fs.closeSync(fd);
  const done = new Promise((resolve) => {
    cat.on('exit', resolve);
    cat.on('error', resolve);
  });
  return { cat, done };
}

function readReply(conn) {
  return new Promise((resolve) => {
    let buf = '';
    conn.setEncoding('utf8');
    conn.on('data', (chunk) => {
      buf += chunk;
      const nl = buf.indexOf('\n');
      if (nl < 0) return;
      try {
        resolve(JSON.parse(buf.slice(0, nl)));
      } catch (_) {
        resolve(null);
      }
    });
    conn.on('close', () => resolve(null));
    conn.on('error', () => resolve(null));
  });
}

function connect(socketPath) {
  return new Promise((resolve, reject) => {
    const conn = net.connect(socketPath, () => resolve(conn));
    conn.once('error', reject);
  });
}

async function exitStatus() {
  const deadline = Date.now() + EXIT_WAIT_MS;
  while (Date.now() < deadline) {
    try {
      const code = parseInt(fs.readFileSync(BASE + '.exit', 'utf8').trim(), 10);
      if (!Number.isNaN(code)) return code;
    } catch (_) {
      // Not written yet.
    }
    await new Promise((r) => setTimeout(r, 10));
  }
  process.stderr.write('sq-node-runner: the job ended without an exit status\n');
  return 1;
}

const drains = [];

function fail(message) {
  for (const d of drains) d.cat.kill('SIGKILL');
  process.stderr.write(message + '\n', () => process.exit(1));
}

async function main() {
  const job = parsePayload(fs.readFileSync(0));
  job.env = { ...process.env };
  job.script_name = SCRIPT_NAME;

  // Socket first: a runner that is gone fails here at once, while opening a FIFO nobody writes to
  // would block. Both drains exist before the job is sent, so none of its output can be lost.
  const conn = await connect(BASE + '.sock');
  drains.push(drain(BASE + '.out', 1), drain(BASE + '.err', 2));
  const replyPromise = readReply(conn);
  conn.write(JSON.stringify(job) + '\n');
  const reply = await replyPromise;
  if (!reply || reply.ok !== true) {
    fail((reply && reply.error) || 'sq-node-runner: the runner refused the job');
    return;
  }
  await Promise.all(drains.map((d) => d.done));
  conn.destroy();
  process.exitCode = await exitStatus();
}

main().catch((e) => fail('sq-node-runner-client: ' + String((e && e.message) || e)));
