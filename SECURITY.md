# Security Policy — StudioQuack Engine

## Reporting a vulnerability

Public GitHub issues are disabled for this repository — do **not** report
security vulnerabilities publicly. Email **security@studioquack.com** with:

- a description of the issue and the affected release or commit;
- steps to reproduce, or a proof of impact that contains no live secrets and no
  data belonging to anyone other than you;
- the expected impact (sandbox escape, secret exfiltration, cross-environment
  or cross-customer access, control-plane impersonation, denial of service).

StudioQuack acknowledges receipt within **3 business days**, sends a status
update at least every **7 days**, and aims to ship a fix within **90 days** of
a confirmed report. Sandbox-escape, secret-exfiltration, and cross-tenant
issues are treated as urgent. Please keep the issue confidential until a fix is
released or a disclosure date has been agreed with you. Reporters who wish to
be named are credited in the advisory.

## Supported versions

Only the latest **Official Build** — a binary signed and distributed by
StudioQuack through the installer or the in-product update mechanism — is
supported. Self-compiled or modified builds are Unofficial Builds: they are
unsupported and must never be connected to the StudioQuack Platform
(LICENSE §5).

## Safe harbor for security research

LICENSE §2(c) permits good-faith security research on the Software.
StudioQuack considers research **authorized** and will not pursue legal action
against it when you:

1. test only an installation and environments that you own or are authorized
   in writing to test, under your own StudioQuack account;
2. do not access, modify, or retain data belonging to any other person, and
   stop and report immediately if you encounter such data;
3. do not degrade the StudioQuack Platform or its users (no denial-of-service,
   load testing, spam, social engineering, or physical attacks);
4. keep any Unofficial Build you produce for research isolated from the
   Platform (LICENSE §2(b), §5); and
5. report the finding to security@studioquack.com and keep it confidential as
   described above.

Testing the shared Platform beyond your own account requires prior written
authorization from security@studioquack.com.

## Security model

StudioQuack Engine (SQS) runs on a host that StudioQuack does not control, so
the daemon is always assumed compromisable. Securing that host is the
operator's responsibility; StudioQuack's responsibility is to ensure that a
compromised host cannot harm anyone else.

### Invariants a change must not break

- **Blast radius is one environment.** Each environment has its own bearer,
  mTLS identity, CA, and key material; credentials from one environment never
  authenticate as another.
- **No shared secrets across customers.** No global CA, cross-customer token,
  or shared encryption key.
- **The control plane re-authorizes every action.** Network reachability is
  never authorization.
- **Revocation is atomic.** Locking or deleting an environment invalidates
  every credential the daemon holds.
- **Secrets are resolved just-in-time, by reference.** They are never written
  to disk in plaintext, never placed on sandbox layers, never returned on
  output ports, and always redacted before logging.
- **Sandboxes are isolated and egress is governed.** One node container cannot
  read another's filesystem, network namespace, or helper token, and the SSRF
  guard and egress policy apply to every outbound call.
