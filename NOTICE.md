# Notice

StudioQuack Engine
Copyright (c) 2026 StudioQuack, Inc. All rights reserved.

StudioQuack Engine (SQS) is licensed under the StudioQuack Source-Available
License, Version 1.0 ([LICENSE](LICENSE)). It is not open-source software.

This file is an attribution notice for third-party components. It is not a
license. Each component listed here remains under its own license, which is
independent of the StudioQuack license. Do not remove this file,
[LICENSE](LICENSE), [THIRD-PARTY.txt](THIRD-PARTY.txt), or
[THIRD-PARTY-LICENSES.txt](THIRD-PARTY-LICENSES.txt) from any copy
(LICENSE §3(f)).

“StudioQuack” and “Duckbot” are trademarks of StudioQuack, Inc. This notice
grants no trademark rights (LICENSE §8).

## Third-party libraries

SQS links, unmodified, against the libraries inventoried in
[THIRD-PARTY.txt](THIRD-PARTY.txt) (compile and runtime scope; regenerate with
`mvn org.codehaus.mojo:license-maven-plugin:add-third-party -Dlicense.useMissingFile`,
which applies the overrides in `src/license/THIRD-PARTY.properties`). The full
text of every applicable license, and the notices of components that do not
ship their own, are in
[THIRD-PARTY-LICENSES.txt](THIRD-PARTY-LICENSES.txt). Every distributed SQS
binary carries `LICENSE`, this file, `THIRD-PARTY.txt`, and
`THIRD-PARTY-LICENSES.txt` under `META-INF/`. Licenses of note:

| Component | License | Notes |
| --- | --- | --- |
| Spring Boot, Spring Framework, Spring Security, Spring Data | Apache-2.0 | |
| Activiti (embedded BPMN engine) | Apache-2.0 | |
| Hibernate ORM | LGPL-2.1-or-later | Used unmodified as a separately distributed library via Spring Data JPA; the LGPL does not extend to SQS. Source: https://hibernate.org/orm; StudioQuack will also supply the complete corresponding source of the shipped version on request to legal@studioquack.com for three years from each release (LGPL-2.1 §6(c)). It ships as a separate jar inside the binary, so you may replace or modify it and reverse engineer it to debug such modifications (LGPL §6); LICENSE §9 preserves that right. |
| OpenJDK Nashorn (`org.openjdk.nashorn:nashorn-core`) | GPL-2.0 with Classpath Exception | Used unmodified as an independent module under the Classpath Exception; the GPL does not extend to SQS. Corresponding source: the `-sources.jar` of the same version on Maven Central and https://github.com/openjdk/nashorn. StudioQuack will also supply the source on request to legal@studioquack.com for three years from each release (GPL-2.0 §3(b)). |
| Logback (classic, core) | EPL-1.0 / LGPL-2.1 (dual) | Taken under EPL-1.0. Used unmodified; source: https://github.com/qos-ch/logback. |
| H2 Database Engine | MPL-2.0 / EPL-1.0 (dual) | Taken under MPL-2.0. Used unmodified; source: https://github.com/h2database/h2database. |
| Jakarta EE APIs (annotation, transaction, mail) | EPL-2.0 / GPL-2.0 with Classpath Exception (dual) | Taken under EPL-2.0. Used unmodified; source: https://github.com/jakartaee. |
| Bouncy Castle (bcprov, bctls, bcpkix) | Bouncy Castle Licence (MIT-style) | |
| ASM, ANTLR 4 runtime | BSD-3-Clause | Notices in THIRD-PARTY-LICENSES.txt. |
| Jolt (`com.bazaarvoice.jolt:jolt-core`) | Apache-2.0 | The published POM omits license metadata; upstream is Apache-2.0 (https://github.com/bazaarvoice/jolt), recorded in `src/license/THIRD-PARTY.properties`. |
| jnanoid | MIT | |
| Apache Commons Lang / Collections | Apache-2.0 | |
| Project Lombok | MIT | Annotation processor (compile-time only, provided scope). |

No component under a strong copyleft license (GPL without the Classpath
Exception, AGPL, SSPL) or a non-commercial-only license is included in the
runtime. The weak-copyleft components above are used unmodified as separable
libraries; this file and THIRD-PARTY.txt discharge their attribution and
source-availability duties.

## Mesh agent provisioned with the daemon (`sq-net-agent`, `sq-net`)

The StudioQuack installer provisions a white-label build of the open-source
Tailscale client (BSD-3-Clause, upstream tag pinned in the build) as
`sq-net-agent` / `sq-net`. The name “Tailscale” is used descriptively only;
Tailscale Inc. does not endorse or sponsor StudioQuack (BSD-3-Clause,
clause 3). Notable components of that binary:

- **Tailscale client** — BSD-3-Clause. Modified (branding, control URL,
  telemetry removal); copyright headers retained.
- **wireguard-go** — MIT. **gVisor netstack** — Apache-2.0.
  **golang.zx2c4.com/wintun** (Go bindings) — MIT.
- Remaining Go modules — MIT, Apache-2.0, BSD, or ISC only; the build fails on
  any other license. The per-module report ships as `THIRD-PARTY-NOTICES`
  next to the binaries.
- **Wintun driver, `wintun.dll` (Windows only)** — WireGuard LLC “Prebuilt
  Binaries License”, not the GPL-2.0 that covers the Wintun source tree.
  StudioQuack ships the official, Authenticode-signed 0.14.1 DLL byte-for-byte
  (SHA-256 verified at build time), never rebuilds or renames it, calls it only
  through the `wintun.h` API, and distributes it only with the agent that uses
  it. Its license text ships as `prebuilt-binaries-license.txt` with every
  Windows build.

## Runtime tools invoked on the host

The daemon invokes, but this repository does not bundle, the following tools.
The StudioQuack installer provisions them separately; each keeps its own
license:

- **Podman** (Apache-2.0) — container runtime for node sandboxes.
- **WireGuard tools** (`wg`, `wg-quick`; GPL-2.0) — installed from the
  operating system's package repositories and invoked as separate processes
  over their command-line interface; no code is linked and StudioQuack does not
  redistribute them.
- **Java runtime, JRE 25+** (OpenJDK: GPL-2.0 with Classpath Exception) —
  installed from the operating system's package repositories or Adoptium; not
  redistributed by StudioQuack.
- **Docling** (MIT), optional — local document-conversion server.
