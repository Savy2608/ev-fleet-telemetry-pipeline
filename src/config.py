"""Central configuration, read from environment variables (or a local .env file)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# boto3 reads AWS_PROFILE from the environment on its own; an empty value
# (e.g. "AWS_PROFILE=" in .env) makes it look for a profile named "" and crash.
if not os.environ.get("AWS_PROFILE", "").strip():
    os.environ.pop("AWS_PROFILE", None)


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


@dataclass(frozen=True)
class Settings:
    # Kafka
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_group_id: str
    # Producer
    num_vehicles: int
    min_interval_seconds: float
    max_interval_seconds: float
    # Consumer / S3
    s3_bucket: str
    s3_prefix: str
    aws_region: str
    aws_profile: str
    batch_size: int
    flush_interval_seconds: float


def load_settings() -> Settings:
    return Settings(
        kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        kafka_topic=_env("KAFKA_TOPIC", "ev-telemetry"),
        kafka_group_id=_env("KAFKA_GROUP_ID", "ev-telemetry-s3-sink"),
        num_vehicles=int(_env("NUM_VEHICLES", "20")),
        min_interval_seconds=float(_env("MIN_INTERVAL_SECONDS", "1.0")),
        max_interval_seconds=float(_env("MAX_INTERVAL_SECONDS", "2.0")),
        s3_bucket=_env("S3_BUCKET"),
        s3_prefix=_env("S3_PREFIX", "raw/ev_telemetry").strip("/"),
        aws_region=_env("AWS_REGION", "us-east-2"),
        aws_profile=_env("AWS_PROFILE"),
        batch_size=max(1, int(_env("BATCH_SIZE", "1"))),
        flush_interval_seconds=float(_env("FLUSH_INTERVAL_SECONDS", "10")),
    )
