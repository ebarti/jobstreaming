# Security policy

JobStreaming is an alpha, best-effort integration library. Job boards can change
private endpoints, tokens, rate limits, and markup without notice. Adapter drift or a
board becoming unavailable is normally a compatibility issue, not a security
vulnerability.

## Supported versions

Security fixes are made on the latest released `0.0.x` version. Older alpha releases
are not supported.

| Version | Supported |
|---|---|
| Latest `0.0.x` release | Yes |
| Older releases | No |

## Report a vulnerability

Do not open a public issue for a suspected vulnerability. Use the repository's
[private vulnerability report](https://github.com/ebarti/jobstreaming/security/advisories/new).
If that flow is unavailable, contact the maintainer privately through the
[project owner profile](https://github.com/ebarti) before disclosing details.

Include the affected version, impact, reproduction steps, and any suggested
mitigation. Do not include live job-board credentials, personal data, or secrets in a
report or fixture. You should receive an acknowledgement within seven days, but this
alpha project provides no response-time or remediation SLA.

## Credential and data boundary

JobStreaming does not provide shared board credentials. Operators are responsible for
obtaining, storing, rotating, and lawfully using any board-specific configuration.
Keep credentials out of source control and logs.

Job listings and board responses are untrusted external data. Consumers should escape
content before rendering it, validate URLs before following them, and apply their own
privacy and retention policies.
