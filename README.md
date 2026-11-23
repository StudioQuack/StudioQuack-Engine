# StudioQuack Engine

[![License: Source-Available](https://img.shields.io/badge/license-Source--Available%20(not%20open%20source)-red)](LICENSE)
[![Java](https://img.shields.io/badge/Java-25-informational)](#requirements)

> **Source-available, not open source.** This repository is public so that
> customers, auditors, and security researchers can inspect the code that runs
> on their hosts. It is licensed under the
> [StudioQuack Source-Available License 1.0](LICENSE). You may read and review
> the code; you may **not** use it commercially, redistribute it, modify it for
> use, build a competing product from it, or connect a modified or
> self-compiled build to StudioQuack.

**StudioQuack Engine** (**SQS**) is the self-hosted execution engine daemon that
runs StudioQuack workflows **on your own host**. The StudioQuack control plane
designs graphs and triggers runs; StudioQuack Engine owns execution: it deploys
the workflow, runs the embedded BPMN engine, starts a Podman sandbox for every
node, resolves secrets just-in-time, and streams logs and status back over a
private WireGuard overlay.

## What runs where

| Component | Where it runs | Operated by |
| --- | --- | --- |
| Designer, catalog, run history | StudioQuack control plane | StudioQuack |
| **StudioQuack Engine (SQS)** (this repository) | **Your host** (bare metal, VM, or your cloud account) | **You** |
| Built-in node library ([SQ-Builtin-Node-Library](https://github.com/StudioQuack/SQ-Builtin-Node-Library)) | One always-on container on your host, built and supervised by SQS | Platform-managed |
| Custom node images and script nodes | Podman containers started by SQS on your host | You |

SQS talks to the control plane only over an mTLS-secured WireGuard overlay with
per-environment credentials. Deploy and start-run commands come in; sandbox
lifecycle events, logs, and run status go out.

## How nodes execute

SQS chooses each node's container from the node's image, never from a per-node
flag:

| Node | Runs in | When nothing is running |
| --- | --- | --- |
| Built-in node | The built-in node library's system instance | SQS cold-starts that same platform instance and keeps it running |
| Custom OCI image | A running stateful instance of that image | SQS cold-starts a stateful instance of the image on demand and keeps it running |
| Script node (language image) | A warm-pool container of that image, when one is ready | SQS cold-starts a container for that job and removes it afterwards |

A cold start that fails fails the node; SQS never silently switches to another
kind of container. The warm pool is empty until you set a per-image size in
Engine Settings → Settings → Warm Pool.

The built-in node library is platform-managed: on first boot SQS downloads the
signed library release, verifies it, builds the image with Podman, and starts
its single instance before the first run. It supervises that instance,
replaces it by drain when a new release is staged, and does not start a run
until the library is ready. Engine Settings shows its logs but does not let you
stop, start or exec into it.

## Features

- Embedded BPMN engine (Activiti): workflows execute entirely on your host.
- Podman/OCI containers for every node: a platform-managed built-in node
  library, stateful instances for custom images, and a warm pool for script
  nodes.
- Just-in-time secret resolution from the OS secret store (Keychain, DPAPI,
  libsecret); secrets never touch a sandbox's disk and are redacted from logs.
- Encrypted embedded H2 persistence; no external database required.
- Optional local Docling server for document conversion.
- Daemon-to-daemon calls between your own environments over the same overlay.

## Installation — Official Builds only

Install SQS with the official StudioQuack installer from your StudioQuack
account. The installer provisions the JRE, Podman, WireGuard tooling, and
optional Docling, and enrolls the daemon into your environment.

**Only Official Builds** — binaries compiled, signed, and distributed by
StudioQuack through the installer or the in-product update mechanism — may
connect to StudioQuack. Connecting a modified or self-compiled build, or any
software that emulates SQS, is unauthorized and a material breach of the
[LICENSE](LICENSE) (§5) and the
[Terms of Use](https://www.studioquack.com/terms); StudioQuack blocks such
connections and may suspend the affected environments and accounts.

### Requirements

- Java 25, [Podman](https://podman.io/) 4+, WireGuard tooling (`wg`, `wg-quick`)
- A StudioQuack account with an environment to enroll the daemon into

## Reviewing the source

You may compile the source locally to review or audit it (LICENSE §2(a)–(b)):

```bash
mvn -q -DskipTests package
```

A binary you compile yourself is an **Unofficial Build**. Run it only in an
environment isolated from StudioQuack, never enroll it, do not share it, and
delete it when your review ends.

```
src/main/java/com/studioquack/sqs/
  channel/     enrollment, WireGuard interface, heartbeat, inbound overlay listener
  engine/      Activiti facade, BPMN generation, deployment, dispatch delegate
  sandbox/     Podman execution, warm pool, stateful instances, helper channel, secret resolver, redactor
  builtinlib/  built-in node library build, supervision and drained updates
  secrets/     OS secret-store custody
  command/     command handlers invoked by the control plane
  maintenance/ self-update and uninstall
```

## License summary

This summary is for convenience only; the [LICENSE](LICENSE) governs.

| You may | You may not |
| --- | --- |
| Read, evaluate, and audit the source code | Use the code, or anything derived from it, for any commercial purpose |
| Compile it in isolation for review and security research | Redistribute, publish, mirror, or sublicense it, in source or binary form |
| Run Official Builds with your StudioQuack account under the [Terms of Use](https://www.studioquack.com/terms) | Modify it for use, or build a competing workflow-automation product, service, or engine from it — including a “clean-room” rewrite based on it |
| Report vulnerabilities under the [safe harbor](SECURITY.md#safe-harbor-for-security-research) | Connect any modified or self-compiled build to StudioQuack |
| | Remove notices, circumvent license or integrity checks, or use the code to train AI models |

For rights this license does not grant, contact **legal@studioquack.com**.

## Contributions

This repository is read-only for transparency, evaluation, and security review.
**Both pull requests and public GitHub issues are disabled**, and unsolicited
pull requests are automatically closed without review.

- **Bug reports and general support:** **support@studioquack.com**
- **Security vulnerabilities:** **security@studioquack.com** (see [SECURITY.md](SECURITY.md))
- **Licensing and legal inquiries:** **legal@studioquack.com**

Any feedback you submit is licensed to StudioQuack under [LICENSE](LICENSE) §6.

## Security

Report vulnerabilities privately to **security@studioquack.com** — never in a
public issue. See [SECURITY.md](SECURITY.md) for the disclosure process, the
research safe harbor, and the daemon's security model.

## Data protection and privacy

- **Your workflow data is processed on your host.** When a node you configure
  sends data to a third party (an AI provider, a SaaS API, an export sink),
  that transfer is your processing decision.
- **What leaves your host to StudioQuack:** run status, sandbox lifecycle
  events, and node-level execution records (status, timings, logs, and node
  input/output values) so that Monitoring can display them. StudioQuack
  processes that copy as your processor under the Data Processing Agreement and
  retains it as described in the
  [Privacy Policy](https://www.studioquack.com/privacy). Logs are redacted for
  known secret formats before transmission, but redaction is best-effort: do
  not write personal data or secrets to logs.
- **On the host,** the embedded store keeps run history for a bounded window
  and purges each run once the control plane acknowledges it; deleting the
  environment removes the daemon's local state.
- **You remain the controller** (or covered entity) under GDPR, UK GDPR, HIPAA,
  and similar laws for processing on your host and for the Monitoring copy held
  on your behalf. StudioQuack's obligations are set out in the
  [Terms of Use](https://www.studioquack.com/terms), the
  [Privacy Policy](https://www.studioquack.com/privacy), and the Data Processing
  Agreement.

## Trademarks

“StudioQuack” and “Duckbot” are trademarks of StudioQuack, Inc. The license
grants no right to use them (LICENSE §8): do not use them in the name of any
product, service, domain, package, repository, or fork, or in any way that
suggests endorsement.

## Third-party software and notices

Third-party components remain under their own licenses: [NOTICE.md](NOTICE.md),
[THIRD-PARTY.txt](THIRD-PARTY.txt), and the full license texts in
[THIRD-PARTY-LICENSES.txt](THIRD-PARTY-LICENSES.txt). Every Official Build
carries these files under `META-INF/`.

Copyright (c) 2026 StudioQuack, Inc. All rights reserved.
