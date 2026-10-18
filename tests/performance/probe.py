#!/usr/bin/env python3
"""Small, repeatable source-to-Iceberg throughput and memory probe.

Requires the running development lab (make lab-up). Results are observations,
not a pass/fail performance gate; hardware and Docker limits matter.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-p", "lake-replication-lab",
           "-f", str(ROOT / "deploy/docker-compose/compose.yml")]
RUN = datetime.now(timezone.utc).strftime("perf_%Y%m%d_%H%M%S")
DEST_DB = RUN
SCHEMA = RUN
TABLE = RUN
PUBLICATION = f"{RUN}_pub"
SLOT = f"{RUN}_slot"
RESULTS = ROOT / ".lab" / RUN
BIN = ROOT / "target/release"


def command(args, *, input=None, timeout=300, check=True, env=None):
    result = subprocess.run(args, cwd=ROOT, input=input, text=True,
                            capture_output=True, timeout=timeout, env=env)
    if check and result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result


def sql(database, statement, *, check=True):
    return command(COMPOSE + ["exec", "-T", "postgres", "psql", "-XAt",
                      "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", database],
                   input=statement, check=check).stdout.strip()


def memory_bytes(value):
    match = re.match(r"\s*([0-9.]+)\s*([KMGT]?i?B|B)", value)
    if not match:
        raise ValueError(f"unrecognized Docker memory value: {value!r}")
    scale = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3,
             "TiB": 1024**4, "KB": 1000, "MB": 1000**2,
             "GB": 1000**3, "TB": 1000**4}
    return int(float(match[1]) * scale[match[2]])


class MemorySampler:
    def __init__(self, process, containers):
        self.process = process
        self.containers = containers
        self.worker_peak_bytes = 0
        self.container_peak_bytes = {name: 0 for name in containers}
        self.worker_samples = 0
        self.container_samples = 0
        self.stop_event = threading.Event()
        self.threads = [threading.Thread(target=self.sample_worker, daemon=True),
                        threading.Thread(target=self.sample_container, daemon=True)]

    def start(self):
        for thread in self.threads:
            thread.start()

    def stop(self):
        self.stop_event.set()
        for thread in self.threads:
            thread.join()
        return {"worker_peak_rss_bytes": self.worker_peak_bytes,
                **{f"{name}_container_peak_bytes": peak
                   for name, peak in self.container_peak_bytes.items()},
                "worker_samples": self.worker_samples,
                "container_samples": self.container_samples}

    def sample_worker(self):
        while not self.stop_event.is_set():
            if self.process.poll() is None:
                result = command(["ps", "-o", "rss=", "-p", str(self.process.pid)],
                                 timeout=10, check=False)
                if result.returncode == 0 and result.stdout.strip():
                    self.worker_peak_bytes = max(
                        self.worker_peak_bytes, int(result.stdout.strip()) * 1024)
                    self.worker_samples += 1
            self.stop_event.wait(0.1)

    def sample_container(self):
        while not self.stop_event.is_set():
            result = command(["docker", "stats", "--no-stream", "--format",
                              "{{.ID}} {{.MemUsage}}", *self.containers.values()],
                             timeout=15, check=False)
            if result.returncode == 0 and result.stdout.strip():
                for line in result.stdout.splitlines():
                    container_id, usage = line.split(" ", 1)
                    for name, full_id in self.containers.items():
                        if full_id.startswith(container_id):
                            self.container_peak_bytes[name] = max(
                                self.container_peak_bytes[name],
                                memory_bytes(usage.split("/")[0]))
                            self.container_samples += 1
                            break
            self.stop_event.wait(0.5)


class NoMemorySampler:
    def start(self):
        pass

    def stop(self):
        return None


def worker_env(subscription_id, **settings):
    env = os.environ.copy()
    env.update({"LAKE_SUBSCRIPTION_ID": str(subscription_id),
                "LAKE_DESTINATION_DATABASE": DEST_DB, **settings})
    return env


def run_worker(binary, subscription_id, **settings):
    return command([str(BIN / binary)], env=worker_env(subscription_id, **settings),
                   timeout=600).stdout.strip()


def publisher(subscription_id, containers, batch_rows, sample_memory):
    env = worker_env(subscription_id, LAKE_BATCH_MAX_TRANSACTIONS="1000",
                     LAKE_BATCH_MAX_ROWS=str(batch_rows),
                     LAKE_BATCH_MAX_BYTES="67108864", LAKE_MAX_BATCHES="10000")
    process = subprocess.Popen([str(BIN / "lake-replication-publisher")], cwd=ROOT,
                               env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True)
    sampler = MemorySampler(process, containers) if sample_memory else NoMemorySampler()
    started = time.monotonic()
    sampler.start()
    try:
        stdout, stderr = process.communicate(timeout=600)
        finished = time.monotonic()
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        raise RuntimeError("publisher exceeded 600 seconds")
    finally:
        memory = sampler.stop()
    if process.returncode:
        raise RuntimeError(stderr.strip())
    return finished - started, memory, stdout.count("published batch=")


def phase(name, subscription_id, containers, rows, chunk, payload_bytes,
          batch_rows, sample_memory):
    chunks = [(start, min(start + chunk - 1, rows + 1))
              for start in range(2, rows + 2, chunk)]
    receiver_log_path = RESULTS / f"{name}-receiver.log"
    with receiver_log_path.open("w+") as receiver_log:
        receiver = subprocess.Popen([str(BIN / "lake-replication-receiver")], cwd=ROOT,
                                    env=worker_env(subscription_id, LAKE_SLOT=SLOT,
                                                   LAKE_PUBLICATION=PUBLICATION,
                                                   LAKE_MAX_TRANSACTIONS=str(len(chunks))),
                                    stdout=receiver_log, stderr=subprocess.STDOUT, text=True)
        sampler = MemorySampler(receiver, containers) if sample_memory else NoMemorySampler()
        sampler.start()
        started = time.monotonic()
        try:
            statements = []
            for first, last in chunks:
                if name == "insert":
                    statements.append(f"INSERT INTO public.{TABLE} SELECT g, "
                                      f"repeat('x', {payload_bytes}) "
                                      f"FROM generate_series({first}, {last}) g;")
                else:
                    statements.append(f"UPDATE public.{TABLE} "
                                      f"SET note=repeat('u', {payload_bytes}) "
                                      f"WHERE id BETWEEN {first} AND {last};")
            sql("application", "\n".join(statements))
            source_seconds = time.monotonic() - started
            receiver.wait(timeout=300)
            staged_seconds = time.monotonic() - started
            receiver_log.flush()
            receiver_log.seek(0)
            stdout = receiver_log.read()
            if receiver.returncode:
                raise RuntimeError(stdout.strip())
        except BaseException:
            if receiver.poll() is None:
                receiver.terminate()
                try:
                    receiver.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    receiver.kill()
                    receiver.wait()
            raise
        finally:
            receiver_memory = sampler.stop()
    if stdout.count("staged xid=") + stdout.count("staged streamed xid=") != len(chunks):
        raise RuntimeError(f"receiver staged unexpected transaction count in {name} phase")
    published_seconds, publisher_memory, batches = publisher(
        subscription_id, containers, batch_rows, sample_memory)
    return {"rows": rows, "transactions": len(chunks),
            "source_seconds": round(source_seconds, 3),
            "source_rows_per_second": round(rows / source_seconds, 1),
            "staged_seconds_from_start": round(staged_seconds, 3),
            "staged_rows_per_second_from_start": round(rows / staged_seconds, 1),
            "publisher_seconds": round(published_seconds, 3),
            "publisher_rows_per_second": round(rows / published_seconds, 1),
            "published_batches": batches, "receiver_memory": receiver_memory,
            "publisher_memory": publisher_memory}


def cleanup(subscription_id):
    sql("application", f"""
        SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
         WHERE slot_name='{SLOT}';
        DROP PUBLICATION IF EXISTS {PUBLICATION};
        DROP TABLE IF EXISTS public.{TABLE};
    """, check=False)
    sql(DEST_DB, f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE", check=False)
    if subscription_id is not None:
        sql(DEST_DB, f"""
            DELETE FROM lake_replication.changes WHERE transaction_id IN
                (SELECT transaction_id FROM lake_replication.transactions
                  WHERE subscription_id={subscription_id});
            DELETE FROM lake_replication.transactions WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.batches WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.relations WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.checkpoints WHERE subscription_id={subscription_id};
            DELETE FROM lake_replication.subscriptions WHERE subscription_id={subscription_id};
        """, check=False)
    sql("application", f"DROP DATABASE IF EXISTS {DEST_DB} WITH (FORCE)", check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--insert-rows", type=int, default=1000)
    parser.add_argument("--update-rows", type=int, default=200)
    parser.add_argument("--rows-per-transaction", type=int, default=100)
    parser.add_argument("--batch-rows", type=int, default=500)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--skip-memory-sampling", action="store_true")
    args = parser.parse_args()
    sizes = (args.insert_rows, args.rows_per_transaction, args.batch_rows,
             args.payload_bytes)
    if min(sizes) <= 0 or args.update_rows < 0 or args.update_rows > args.insert_rows:
        parser.error("sizes must be positive; update rows must be between zero and insert rows")
    RESULTS.mkdir(parents=True, exist_ok=False)
    evidence = {"run": RUN, "configuration": vars(args), "status": "running"}
    subscription_id = None
    try:
        command(["cargo", "build", "--release", "--quiet", "--bins"], timeout=600)
        containers = {name: command(COMPOSE + ["ps", "-q", name]).stdout.strip()
                      for name in ("postgres", "pgduck")}
        if not all(containers.values()):
            raise RuntimeError("development lab is not running; run make lab-up")
        evidence["containers"] = containers
        sql("application", f"CREATE DATABASE {DEST_DB}")
        sql(DEST_DB, "CREATE EXTENSION pg_lake CASCADE; CREATE EXTENSION dblink; "
                     "CREATE EXTENSION lake_replication")
        identity = json.loads(sql("application", """
            SELECT json_build_object(
                'system_identifier', (SELECT system_identifier FROM pg_control_system()),
                'database_oid', (SELECT oid FROM pg_database WHERE datname=current_database()),
                'timeline', timeline_id) FROM pg_control_checkpoint()
        """))
        sql("application", f"""
            CREATE TABLE public.{TABLE} (id bigint PRIMARY KEY, note text NOT NULL);
            CREATE PUBLICATION {PUBLICATION} FOR TABLE public.{TABLE};
            SELECT slot_name FROM pg_create_logical_replication_slot('{SLOT}', 'pgoutput');
        """)
        subscription_id = int(sql(DEST_DB, f"""
            SELECT lake_replication.create_subscription(
                {identity['system_identifier']}, {identity['database_oid']},
                {identity['timeline']}, '{RUN}', '{SLOT}', ARRAY['{PUBLICATION}'])
        """))
        sql(DEST_DB, f"""
            CREATE SCHEMA {SCHEMA};
            CREATE TABLE {SCHEMA}.target (id bigint, note text) USING iceberg;
        """)
        sql("application", f"INSERT INTO public.{TABLE} VALUES (1, 'bootstrap')")
        run_worker("lake-replication-receiver", subscription_id,
                   LAKE_SLOT=SLOT, LAKE_PUBLICATION=PUBLICATION,
                   LAKE_MAX_TRANSACTIONS="1")
        relation = sql(DEST_DB, f"""
            SELECT remote_relation_id || ':' || schema_version
              FROM lake_replication.relations WHERE subscription_id={subscription_id}
        """).split(":")
        sql(DEST_DB, f"""
            SELECT lake_replication.assign_destination(
                {subscription_id}, {relation[0]}, {relation[1]}, '{SCHEMA}.target'::regclass)
        """)
        run_worker("lake-replication-publisher", subscription_id,
                   LAKE_BATCH_MAX_ROWS=str(args.batch_rows))
        baselines = command(["docker", "stats", "--no-stream", "--format",
                             "{{.ID}} {{.MemUsage}}", *containers.values()]).stdout
        for line in baselines.splitlines():
            container_id, usage = line.split(" ", 1)
            for name, full_id in containers.items():
                if full_id.startswith(container_id):
                    evidence[f"{name}_container_baseline_bytes"] = memory_bytes(
                        usage.split("/")[0])
                    break
        for name, rows in (("insert", args.insert_rows), ("update", args.update_rows)):
            if rows == 0:
                continue
            evidence[name] = phase(name, subscription_id, containers, rows,
                                   args.rows_per_transaction, args.payload_bytes,
                                   args.batch_rows, not args.skip_memory_sampling)
            print(f"{name}: source {evidence[name]['source_rows_per_second']} rows/s, "
                  f"stage {evidence[name]['staged_rows_per_second_from_start']} rows/s, "
                  f"publish {evidence[name]['publisher_rows_per_second']} rows/s", flush=True)
        source = sql("application", f"""
            SELECT count(*) || ':' || count(*) FILTER (WHERE note=repeat('u', {args.payload_bytes}))
              FROM public.{TABLE}
        """)
        target = sql(DEST_DB, f"""
            SELECT count(*) || ':' || count(*) FILTER (WHERE note=repeat('u', {args.payload_bytes}))
              FROM {SCHEMA}.target
        """)
        evidence["source_counts"] = source
        evidence["target_counts"] = target
        if source != target or source != f"{args.insert_rows + 1}:{args.update_rows}":
            raise RuntimeError(f"destination mismatch: source={source}, target={target}")
        evidence["status"] = "passed"
        return 0
    except Exception as exc:
        evidence["status"] = "error"
        evidence["error"] = str(exc)
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        (RESULTS / "results.json").write_text(json.dumps(evidence, indent=2) + "\n")
        cleanup(subscription_id)
        print(f"Evidence: {RESULTS / 'results.json'}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
