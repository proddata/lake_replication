# Evidence summaries and archive

Use the [qualification reports](../qualification/README.md) and [performance reports](../performance/README.md) for conclusions and scope. This directory keeps compact machine-readable summaries for current release, journaled and direct-publication work. Older experiment dumps are available from the archive instead of the normal checkout.

## What is retained

| Location | Contents |
| --- | --- |
| Summary JSON files here | Outcomes, reported metrics, failures, configuration and source/runtime identities |
| [Archive index](archive-index.json) | All 249 original records, their retention classification, byte lengths, SHA-256 hashes and pinned retrieval URLs |
| [Signed archive tag](https://github.com/proddata/lake_replication/tree/evidence/raw-2026-11-16/docs/evidence) | Complete original JSON artifacts and matching repository snapshot |

The original records totaled 49.8 MiB. The current checkout has 63 summaries plus the index, totaling about 2.4 MiB. The archived source-push and earlier investigation records remain linked from their authored reports using commit-pinned URLs.

Each summary starts with `_retention`, identifying its raw source commit, path, size and SHA-256. Fields marked `_omitted_detail` describe removed raw content and its canonical JSON hash; they are not the original raw values. Repeated source-file hashes, sampling timelines, full container inspections, large row/log dumps and successful assertion values are archived. Assertions retain counts and failed cases; profile windows retain numeric ranges grouped by phase. Reported aggregate metrics and unabridged scalar metadata remain unchanged. This compaction does not rerun or newly qualify an experiment.

## Retrieve an original

The raw snapshot is commit `378993de4c42042a6a29c3f1843b7b31776a20cb`, preserved independently of PR squash/branch deletion by signed tag `evidence/raw-2026-11-16`. Follow an index URL or use Git:

```sh
git fetch origin tag evidence/raw-2026-11-16
git show evidence/raw-2026-11-16:docs/evidence/2026-11-16-wal-direct-prelock.json > /tmp/prelock-raw.json
```

Verify the retrieved bytes against the index SHA-256. Paths and hashes inside the raw artifact describe its captured revision; local `.qualification/` paths may still require the original fixture environment. The archive tag preserves the committed evidence, not every uncommitted local log or service volume.

To reproduce the summary transformation, run the [compaction tool](../../tools/compact_evidence.py) with `--source-ref evidence/raw-2026-11-16`. It previews the retained set by default; `--apply` regenerates it. The tool verifies omitted-content hashes, assertion counts/failures, numeric window ranges and all unabridged values against the raw source.

## Retention policy

Commit concise results and provenance for work that supports current decisions. Keep failed configurations and the limits they establish. Store full logs, timelines, source snapshots and repeated fixture dumps in durable run/release artifact storage; record a stable URL and digest in the summary. For this cleanup, the signed Git archive provides the durable original snapshot.

Do not edit raw archived results to change a conclusion or recorded path. Derive a labeled summary instead. Keep historical artifacts indexed rather than copying every intermediate run into the current tree. Pruning the checkout does not shrink existing Git history; this change does not rewrite history.

Current release decisions remain in [release gates](../roadmap/release.md), not in the date or passing status of one evidence file.

[Documentation home](../README.md).
