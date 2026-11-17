#!/usr/bin/env python3
"""
EV fleet telemetry consumer: Kafka -> S3 data lake.

Reads JSON events from Kafka and writes them to S3 as JSON files, Hive-style
partitioned by *event* time (UTC) so Glue/Athena can prune partitions:

    s3://<bucket>/<prefix>/year=YYYY/month=MM/day=DD/hour=HH/ev_telemetry_<ts>_<id>.json

Each file is newline-delimited JSON (one event per line). With BATCH_SIZE=1 every
Kafka message becomes its own file; larger batches mean fewer, bigger files.

Delivery is at-least-once: Kafka offsets are committed only after the files are
written, so a crash may re-write a few events. Deduplicate on `event_id` if needed.

Usage:
    python src/consumer.py                      # write to S3 (needs `aws configure`)
    python src/consumer.py --local-dir data     # write to local disk instead, same layout
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from config import load_settings

logger = logging.getLogger("consumer")


# --------------------------------------------------------------------------- #
# Writers
# --------------------------------------------------------------------------- #
class S3Writer:
    def __init__(self, bucket: str, region: str, profile: str = "") -> None:
        import boto3

        session = boto3.Session(profile_name=profile or None, region_name=region)
        self._s3 = session.client("s3")
        self.bucket = bucket

    def check_access(self) -> None:
        """Fail fast with a clear error if the bucket is missing or credentials are wrong."""
        self._s3.head_bucket(Bucket=self.bucket)

    def write(self, key: str, body: bytes) -> str:
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json")
        return f"s3://{self.bucket}/{key}"


class LocalWriter:
    def __init__(self, root: str) -> None:
        self.root = Path(root)

    def check_access(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def write(self, key: str, body: bytes) -> str:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return str(path)


# --------------------------------------------------------------------------- #
# Partitioning and buffering
# --------------------------------------------------------------------------- #
def event_time(event: dict) -> datetime:
    """Parse the event's ISO-8601 timestamp; fall back to 'now' if it's missing or bad."""
    raw = event.get("timestamp")
    if isinstance(raw, str):
        try:
            ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    logger.warning("Event %s has no valid timestamp; using current time", event.get("event_id"))
    return datetime.now(timezone.utc)


def partition_path(prefix: str, ts: datetime) -> str:
    return f"{prefix}/year={ts:%Y}/month={ts:%m}/day={ts:%d}/hour={ts:%H}"


class PartitionedBuffer:
    """Holds events grouped by their S3 partition until it's time to flush."""

    def __init__(self) -> None:
        self._records: dict[str, list[dict]] = defaultdict(list)
        self.count = 0
        self.started_at = time.monotonic()

    def add(self, partition: str, event: dict) -> None:
        if self.count == 0:
            self.started_at = time.monotonic()
        self._records[partition].append(event)
        self.count += 1

    def age_seconds(self) -> float:
        return time.monotonic() - self.started_at

    def drain(self) -> dict[str, list[dict]]:
        records, self._records, self.count = self._records, defaultdict(list), 0
        return records


def flush(buffer: PartitionedBuffer, writer, consumer, batch_size: int) -> int:
    """Write buffered events in files of at most `batch_size` lines, then commit offsets."""
    files_written = 0
    for partition, events in buffer.drain().items():
        for start in range(0, len(events), batch_size):
            chunk = events[start:start + batch_size]
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
            key = f"{partition}/ev_telemetry_{stamp}_{uuid.uuid4().hex[:8]}.json"
            body = "".join(json.dumps(e, separators=(",", ":")) + "\n" for e in chunk)
            location = writer.write(key, body.encode("utf-8"))
            files_written += 1
            logger.debug("Wrote %d event(s) to %s", len(chunk), location)
    # Only commit after every file landed: if a write fails we re-read these events on restart.
    consumer.commit()
    return files_written


# --------------------------------------------------------------------------- #
# Main loop
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sink EV telemetry from Kafka into S3.")
    parser.add_argument("--local-dir", default=None,
                        help="write files under this local folder instead of S3 (for testing)")
    parser.add_argument("--verbose", action="store_true", help="log every file written")
    return parser.parse_args()


class GracefulStop:
    """Ctrl+C / SIGTERM sets a flag; the loop exits after finishing the current poll."""

    def __init__(self) -> None:
        self.requested = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, frame) -> None:  # noqa: ARG002
        if self.requested:  # second Ctrl+C: give up immediately
            raise KeyboardInterrupt
        logger.info("Stop requested, finishing current batch... (Ctrl+C again to force)")
        self.requested = True


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")
    for noisy in ("kafka", "botocore", "boto3", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    stop = GracefulStop()

    settings = load_settings()

    if args.local_dir:
        writer = LocalWriter(args.local_dir)
        destination = f"{Path(args.local_dir).resolve()}/{settings.s3_prefix}/"
    else:
        if not settings.s3_bucket:
            raise SystemExit("S3_BUCKET is not set. Add it to .env or use --local-dir.")
        writer = S3Writer(settings.s3_bucket, settings.aws_region, settings.aws_profile)
        destination = f"s3://{settings.s3_bucket}/{settings.s3_prefix}/"
    writer.check_access()

    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=[s.strip() for s in settings.kafka_bootstrap_servers.split(",")],
        group_id=settings.kafka_group_id,
        auto_offset_reset="earliest",  # a brand-new consumer group starts from the oldest data
        enable_auto_commit=False,      # we commit manually after a successful write
    )
    logger.info("Consuming '%s' from %s -> %s (batch_size=%d, flush every %.0fs)",
                settings.kafka_topic, settings.kafka_bootstrap_servers, destination,
                settings.batch_size, settings.flush_interval_seconds)

    buffer = PartitionedBuffer()
    events_total = files_total = bad_total = 0
    last_report = time.monotonic()

    try:
        while not stop.requested:
            polled = consumer.poll(timeout_ms=1000, max_records=500)
            for messages in polled.values():
                for message in messages:
                    try:
                        event = json.loads(message.value)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        bad_total += 1
                        logger.error("Skipping malformed message at %s[%d]@%d",
                                     message.topic, message.partition, message.offset)
                        continue
                    buffer.add(partition_path(settings.s3_prefix, event_time(event)), event)
                    events_total += 1

            # Flush only between polls: consumer.commit() commits everything the last
            # poll returned, so every one of those events must be written first.
            if buffer.count and (buffer.count >= settings.batch_size
                                 or buffer.age_seconds() >= settings.flush_interval_seconds):
                files_total += flush(buffer, writer, consumer, settings.batch_size)

            if time.monotonic() - last_report >= 30:
                logger.info("%d events consumed, %d files written, %d malformed",
                            events_total, files_total, bad_total)
                last_report = time.monotonic()
    finally:
        if stop.requested and buffer.count:
            files_total += flush(buffer, writer, consumer, settings.batch_size)
        consumer.close(autocommit=False)
        logger.info("Done. %d events consumed, %d files written, %d malformed.",
                    events_total, files_total, bad_total)


if __name__ == "__main__":
    main()
