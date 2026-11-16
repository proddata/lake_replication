# Repository guidance

## Product direction and status

Read [README.md](README.md) for the product overview and [docs/README.md](docs/README.md) for documentation navigation. The primary local deployment is a single PostgreSQL instance mirroring its own application tables into Iceberg. The journaled pipeline is the packaged default; direct publication is an opt-in experimental path with its own checkpoint/acknowledgement contract. Keep intended support, implemented behavior and qualified configurations distinct. Read [the publication contracts](docs/design/direct-publication.md) before changing either path.

## Plans and task lists

Active plans and TODO-style documents live in [docs/roadmap/](docs/roadmap/README.md). Start with [replication.md](docs/roadmap/replication.md), then follow its release, security and write-path links as relevant. Keep one owning checklist per topic, update status with supporting evidence, and index new plans there. Do not introduce root-level TODO files or competing roadmaps in arbitrary folders. Archived plans are historical context, not instructions to execute every unchecked box.

## Documentation organization

- `docs/guides/`: current setup, operating procedures and support boundaries.
- `docs/design/`: architecture, contracts and mode distinctions.
- `docs/performance/`: reproducible measurements and optimization reports, with limits and failed envelopes.
- `docs/qualification/`: candidate-specific correctness, recovery and release reports.
- `docs/roadmap/`: active plans, proposals and release/security task lists.
- `docs/archive/`: historical milestones, investigations and superseded planning context.
- `docs/evidence/`: retained run artifacts and provenance; preserve existing artifact bytes and recorded historical paths.

Use the project’s own architecture terms. Keep dependency names, required notices, actual historical API identifiers and necessary source citations accurate; avoid comparison-driven product naming. When moving documents, update relative links, indexes, source references and packaging `COPY` inputs. Keep evidence immutable; historical paths in artifacts refer to the captured source revision. Add documents to their section index instead of expanding the main README into a report catalog.

## Validation

Use checks appropriate to the change. Documentation reorganizations need local-link/index checks and verification of referenced packaging inputs. Code changes should run relevant tests and repository checks; native experiments must retain exact runtime/source identities and default journal-path regression coverage. Do not promote local experiment results into release/support claims without the corresponding qualification.
