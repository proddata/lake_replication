# Documentation

## Start here

| You want to… | Read |
| --- | --- |
| Mirror tables on one PostgreSQL instance | [Single-instance guide](guides/local-iceberg.md) |
| Install the packaged beta | [Installer guide](../deploy/install/README.md) and [compatibility contract](../deploy/install/COMPATIBILITY.md) |
| Understand journaled versus direct publication | [Publication contracts](design/direct-publication.md) |
| Operate, monitor or recover a subscription | [Operations](guides/operations.md) and [upgrade/rollback](guides/upgrade-and-rollback.md) |
| Find what to implement next | [Active plans and task lists](roadmap/README.md) |
| Decide whether a candidate is releasable | [Release gates](roadmap/release.md) |

## Browse by purpose

| Section | What belongs here |
| --- | --- |
| [Guides](guides/README.md) | Current operating procedures and support boundaries |
| [Design](design/README.md) | Architecture, transaction/acknowledgement contracts and optional modes |
| [Performance](performance/README.md) | Measurements, tuning experiments, sizing and failed load envelopes |
| [Qualification](qualification/README.md) | Exact-candidate correctness, recovery and release reports |
| [Roadmap](roadmap/README.md) | Active implementation plans, security follow-up and release checklists |
| [Archive](archive/README.md) | Historical milestones, investigations and superseded plans |
| [Evidence](evidence/README.md) | Immutable machine-readable run artifacts and provenance |

The journaled pipeline is the packaged default beta. Direct publication is a development direction with an opt-in prototype; the [contract](design/direct-publication.md) separates intended support from what is implemented and qualified. Current release status is owned by the release gates. Historical reports and their unchecked tasks do not define current priorities.

For a new document, choose the owning section and add a link to its index. Keep active TODO-style plans in `roadmap/`. Preserve evidence bytes and historical provenance when moving authored documents. See [repository guidance](../AGENTS.md).
