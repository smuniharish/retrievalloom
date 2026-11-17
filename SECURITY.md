# Security policy

Thank you for helping keep RetrievalLoom and its users safe. This policy
explains which versions receive fixes and how to report a vulnerability
privately.

## Supported versions

| Version | Supported |
| --- | --- |
| 0.1.x | Yes: security fixes are released as 0.1.x patch versions |

Unreleased development code receives fixes before release.

## Reporting a vulnerability

Please report suspected vulnerabilities privately to
samamuniharish@gmail.com. Include a clear description, affected versions,
reproduction steps, potential impact, and any suggested mitigation. Do not
include credentials or sensitive production data.

You will receive an acknowledgment, an assessment of the impact, and
coordination on disclosure where possible. Please do not publish details until
a fix or mitigation is available.

## Deployment guidance

RetrievalLoom stores your documents, their chunks, graph data and retrieval
provenance in the metadata store and index backends you configure. As the
deployer, you are responsible for:

- deriving `Scope` values (tenant, namespace) from authenticated identity, never
  from untrusted request input, and installing an access policy where finer
  control is needed;
- protecting the metadata database file, index directories, and vector store
  with access control, encryption at rest, backups, and retention suited to the
  data;
- keeping vector store API keys and connection strings out of source control;
- loading only trusted model weights in their embedding, reranker, and NLI
  adapters; and
- reviewing the security and data-handling terms of external services.

See the [security page](docs/security.md) for what the library enforces and
what it leaves to the deployment.

## Security changes

When you contribute, document security-relevant changes to tenant isolation,
stored data, logging, and failure modes, and add focused tests.
