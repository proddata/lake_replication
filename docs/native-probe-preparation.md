# Native beta.4 probe preparation

Signed candidate `77857809f37349951e7acc4e98c5a8a3aa519859` passes
[CI 40836527510](https://github.com/proddata/lake_replication/actions/runs/40836527510),
including checks and both native archives. Their checksums, architecture and
source revision verify locally. All 16 ARM64 production payload files are
byte-identical to the locally qualified candidate; AMD64 has identical SQL and
control files and the expected five different architecture-specific binaries.

[Probe 40836900198](https://github.com/proddata/lake_replication/actions/runs/40836900198)
was cancelled during preparation, before native replication results were
available. The 35-minute step limit incorrectly included cold compilation of
pg_lake/DuckDB. The earlier native run `40198597757` took 47–52 minutes for its
first lifecycle step, including preparation. The shorter limit was not justified.

The corrected workflow prepares the upstream-only image separately from the
replication tests. It caches the compressed image using the pg_lake source
revision, architecture, Docker recipe and preparation code as the key. A cache
hit verifies inputs and checksum before Docker load, then checks image ID and
architecture/source labels. Qualification still rejects prerequisite images
containing replication files. Image preparation streams compiler progress into
the retained log; exporting streams directly to gzip to avoid another complete
uncompressed image on disk. The local archive is removed after cache upload or
load, before the replication workload.

A cold preparation has a 60-minute limit; the actual managed/four-copy test step
has a 15-minute limit. The overall job ceiling is 90 minutes to accommodate the
first build and export. Subsequent probes reuse the image. These limits bound
execution; they do not imply that cold preparation is quick.

Local validation exports and reloads the existing upstream-only ARM64 image in
a roughly 1.9 GiB compressed archive, preserving its image ID and restoring its tag after that tag is removed. The clean CI ARM64
archive then passes all 65 checks in the managed admission suite, including abrupt staging-child
disconnect and exact-data recovery. Unit checks reject stale inputs, wrong
architectures, changed archives and incorrect source labels, and exercise live
build failure reporting. Both workflows pass actionlint.

[Machine-readable evidence](evidence/2026-10-29-native-probe-preparation.json)
retains the candidate/archive identities, cache manifest and local test inputs.
Native replication and final release gates remain pending; the cancelled attempt
provides no native performance pass.
