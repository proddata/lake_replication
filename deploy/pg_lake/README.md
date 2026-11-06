# pg_lake prerequisite for the beta

`lake-replication-manifest-cleanup-v1` is a repository-maintained patchset for
pg_lake. It avoids rereading a manifest during cleanup when its deleted-entry
count is known to be zero and it contains live entries. Unknown, empty and
deleted-entry cases keep the original cleanup path. It contains no diagnostic
GUCs, timers or lake_replication worker changes.

The unguarded patch passes native ARM64 and AMD64 correctness, managed-worker,
pipeline and four-copy fairness checks in
[run 42104428685](https://github.com/proddata/lake_replication/actions/runs/42104428685).
Full qualification of the supported runtime build and the RC soak are still
required. Earlier failed trials remain in the
[investigation](../../docs/beta5-commit-internals.md).

The patchset manifest pins the patch checksum and all three source files before
and after patching. The accepted revisions are `8180647d4535` and `f609ecb17891`;
the relevant original files match exactly at these two commits. Native patch
verification currently covers `8180647d4535`. Compatibility qualification at
the second endpoint must be repeated with this patchset.

## Build the prerequisite image

Start with the source-pinned PostgreSQL 18 / pg_lake prerequisite image built by
this repository. Then build the runtime layer:

```sh
docker build --build-arg BASE_IMAGE=pg-lake-qualification-base:8180647d4535 \
  --tag pg-lake-runtime:beta5 deploy/pg_lake
```

The installer verifies the source revision, checks all affected source hashes,
rejects unrelated parent-tree changes, and rebuilds both `pg_lake_iceberg` and
`pg_lake_table` before installing either library. Rebuilding both is required
because they share an internal struct. The engine and DuckDB are unchanged.
The image contains the pg_lake prerequisite; install the lake_replication
release archive separately. This image is not a running database deployment.

Release archives contain this directory as `PG_LAKE_PREREQUISITE/`, including
the installer, patch, manifest and Dockerfile. From an unpacked archive, use
that directory as the Docker build context, or run
`python3 PG_LAKE_PREREQUISITE/patchset.py install --source /path/to/pg_lake`.
No remote source fetch is performed by the patchset tool.

Qualification builds this layer automatically over its verified upstream cache.
Its separate image identity includes the immutable upstream image ID and every
patchset input. Restoring the original cache does not imply that the patched
runtime was qualified. Qualification records both identities and verifies the
installed libraries against the marker before using the image. The historical
guarded/minimal diagnostics explicitly retain their original upstream base.

## Apply to an existing source build

Inspect an offline source tree first:

```sh
python3 deploy/pg_lake/patchset.py check --source /path/to/pg_lake
```

`state: before` means the expected original files; `state: after` means the exact
patch is already present. Any unsupported, modified or mixed state is rejected.
To apply sources without building or installing:

```sh
python3 deploy/pg_lake/patchset.py apply --source /path/to/pg_lake
```

For a host installation, stop PostgreSQL and its workers, retain the previous
libraries for rollback, and run the following as the installation owner, with
the intended PostgreSQL 18 `pg_config` on `PATH`:

```sh
python3 deploy/pg_lake/patchset.py install --source /path/to/pg_lake --jobs 2
```

Restart PostgreSQL after both libraries have been installed. Replace both
libraries together on rollback too; do not mix the internal layouts or replace
libraries underneath running backends. No extension SQL upgrade is required.

The installed marker is
`$(pg_config --sharedir)/pg_lake/lake-replication-patchset.json`. It records the
patchset, exact upstream revision, manifest/patch checksums and both library
checksums. An upstream commit alone does not identify this runtime. The
upstream submodule remains unchanged; this local patchset can be removed when
an upstream revision containing an equivalent reviewed fix is qualified.
