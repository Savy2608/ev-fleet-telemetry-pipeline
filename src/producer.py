#!/usr/bin/env python3
"""
EV fleet telemetry producer.

Simulates a fleet of electric vehicles driving around Boston, parking and
charging at a handful of stations, and streams one JSON reading per vehicle
to a Kafka topic every 1-2 seconds.

A few vehicles have a "faulty cooling system" and periodically spike above the
45 °C battery-temperature threshold, so there is something to find in Athena.

Usage:
    python src/producer.py                          # stream to Kafka until Ctrl+C
    python src/producer.py --dry-run --ticks 3      # print JSON to stdout, no Kafka needed
    python src/producer.py --dry-run --ticks 500 --no-sleep > sample.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from config import load_settings

logger = logging.getLogger("producer")

# --------------------------------------------------------------------------- #
# Simulation parameters
# --------------------------------------------------------------------------- #
OVERHEAT_THRESHOLD_C = 45.0
AMBIENT_TEMP_C = 22.0

# Vehicles with a simulated faulty battery-cooling system overheat far more often.
FAULTY_VEHICLES = {"EV-007", "EV-014"}
FAULTY_SPIKE_PROBABILITY_PER_S = 0.06
RANDOM_SPIKE_PROBABILITY_PER_S = 0.0005

# Battery drain/charge rates are ~20x real life so a demo shows full cycles in minutes.
LOW_BATTERY_PCT = 20.0

# Bounding box (roughly Greater Boston) that vehicles roam inside.
LAT_MIN, LAT_MAX = 42.30, 42.40
LON_MIN, LON_MAX = -71.15, -71.02


@dataclass(frozen=True)
class Station:
    station_id: str
    name: str
    lat: float
    lon: float
    ports: int
    dc_fast: bool
    popularity: float  # relative weight when a vehicle picks where to charge


# Small, popular stations (ST-BOS-02, ST-CAM-01) will regularly be over capacity.
# Keep `ports` in sync with the capacity CTE in sql/athena_queries.sql.
STATIONS = [
    Station("ST-BOS-01", "Back Bay Garage", 42.3497, -71.0786, 4, True, 1.0),
    Station("ST-BOS-02", "Fenway Lot", 42.3467, -71.0972, 2, False, 2.5),
    Station("ST-BOS-03", "Seaport Hub", 42.3510, -71.0440, 6, True, 1.5),
    Station("ST-BOS-04", "Roxbury Crossing", 42.3313, -71.0953, 2, False, 0.6),
    Station("ST-CAM-01", "Kendall Square", 42.3629, -71.0901, 2, True, 2.0),
    Station("ST-SOM-01", "Assembly Row", 42.3925, -71.0773, 4, True, 0.8),
]
STATION_BY_ID = {s.station_id: s for s in STATIONS}


class Mode:
    DRIVING = "DRIVING"
    PARKED = "PARKED"
    CHARGING = "CHARGING"


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# --------------------------------------------------------------------------- #
# Vehicle model
# --------------------------------------------------------------------------- #
@dataclass
class Vehicle:
    vehicle_id: str
    lat: float
    lon: float
    battery_level: float
    mode: str = Mode.PARKED
    speed_mph: float = 0.0
    heading_rad: float = 0.0
    station_id: str | None = None
    core_temp_c: float = AMBIENT_TEMP_C
    heat_spike_c: float = 0.0  # transient extra heat from a cooling fault
    seconds_fully_charged: float = 0.0

    @property
    def is_faulty(self) -> bool:
        return self.vehicle_id in FAULTY_VEHICLES

    @property
    def battery_temp_c(self) -> float:
        return self.core_temp_c + self.heat_spike_c

    @property
    def charging_status(self) -> str:
        if self.mode != Mode.CHARGING:
            return "NOT_CHARGING"
        if self.battery_level >= 100.0:
            return "FULLY_CHARGED"
        return "FAST_CHARGING" if STATION_BY_ID[self.station_id].dc_fast else "CHARGING"

    # ---- state transitions ------------------------------------------------ #
    def step(self, dt: float, rng: random.Random) -> None:
        if self.mode == Mode.DRIVING:
            self._drive(dt, rng)
        elif self.mode == Mode.PARKED:
            self._park(dt, rng)
        else:
            self._charge(dt)
        self._update_temperature(dt, rng)

    def _drive(self, dt: float, rng: random.Random) -> None:
        self.speed_mph = _clamp(self.speed_mph + rng.gauss(0, 6), 0.0, 75.0)
        self.heading_rad += rng.gauss(0, 0.3)

        miles = self.speed_mph * dt / 3600.0
        self.lat += miles / 69.0 * math.cos(self.heading_rad)
        self.lon += miles / (69.0 * math.cos(math.radians(self.lat))) * math.sin(self.heading_rad)
        if not (LAT_MIN <= self.lat <= LAT_MAX and LON_MIN <= self.lon <= LON_MAX):
            self.heading_rad += math.pi  # bounce back into the service area
            self.lat = _clamp(self.lat, LAT_MIN, LAT_MAX)
            self.lon = _clamp(self.lon, LON_MIN, LON_MAX)

        self.battery_level -= (0.02 + self.speed_mph * 0.002) * dt
        self.battery_level = max(self.battery_level, 0.0)

        if self.battery_level < LOW_BATTERY_PCT:
            self._start_charging(rng)
        elif rng.random() < 0.01 * dt:
            self.mode, self.speed_mph = Mode.PARKED, 0.0

    def _park(self, dt: float, rng: random.Random) -> None:
        self.speed_mph = 0.0
        if self.battery_level < 50 and rng.random() < 0.02 * dt:
            self._start_charging(rng)
        elif rng.random() < 0.05 * dt:
            self.mode, self.speed_mph = Mode.DRIVING, 10.0

    def _start_charging(self, rng: random.Random) -> None:
        # No reservation system: drivers pick by popularity, so busy stations overflow.
        station = rng.choices(STATIONS, weights=[s.popularity for s in STATIONS])[0]
        self.mode = Mode.CHARGING
        self.station_id = station.station_id
        self.speed_mph = 0.0
        self.seconds_fully_charged = 0.0
        self.lat = station.lat + rng.uniform(-0.0003, 0.0003)
        self.lon = station.lon + rng.uniform(-0.0003, 0.0003)

    def _charge(self, dt: float) -> None:
        if self.battery_level >= 100.0:
            self.battery_level = 100.0
            self.seconds_fully_charged += dt
            if self.seconds_fully_charged > 15:  # driver comes back and leaves
                self.mode, self.station_id, self.speed_mph = Mode.DRIVING, None, 15.0
            return
        rate = 0.5 if STATION_BY_ID[self.station_id].dc_fast else 0.2  # % per second
        if self.battery_level > 80:
            rate *= 0.4  # charging tapers near full, like real batteries
        self.battery_level = min(100.0, self.battery_level + rate * dt)

    def _update_temperature(self, dt: float, rng: random.Random) -> None:
        if self.mode == Mode.DRIVING:
            target = AMBIENT_TEMP_C + 5 + self.speed_mph * 0.12
        elif self.charging_status == "FAST_CHARGING":
            target = AMBIENT_TEMP_C + 16
        elif self.charging_status == "CHARGING":
            target = AMBIENT_TEMP_C + 8
        else:
            target = AMBIENT_TEMP_C
        self.core_temp_c += (target - self.core_temp_c) * min(1.0, 0.1 * dt) + rng.gauss(0, 0.2)

        # Fault injection: a sudden heat spike that decays over ~10-20 seconds.
        p = FAULTY_SPIKE_PROBABILITY_PER_S if self.is_faulty else RANDOM_SPIKE_PROBABILITY_PER_S
        if self.heat_spike_c < 1.0 and rng.random() < p * dt:
            self.heat_spike_c = rng.uniform(14.0, 24.0)
        else:
            self.heat_spike_c *= 0.93 ** dt

    # ---- output ----------------------------------------------------------- #
    def to_event(self) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "vehicle_id": self.vehicle_id,
            "timestamp": _utc_now_iso(),
            "battery_level": round(self.battery_level, 1),
            "battery_temp_c": round(self.battery_temp_c, 1),
            "speed_mph": round(self.speed_mph, 1),
            "charging_status": self.charging_status,
            "station_id": self.station_id,
            "location": {"lat": round(self.lat, 6), "lon": round(self.lon, 6)},
        }


def build_fleet(num_vehicles: int, rng: random.Random) -> list[Vehicle]:
    fleet = []
    for i in range(1, num_vehicles + 1):
        vehicle = Vehicle(
            vehicle_id=f"EV-{i:03d}",
            lat=rng.uniform(LAT_MIN, LAT_MAX),
            lon=rng.uniform(LON_MIN, LON_MAX),
            battery_level=rng.uniform(25, 95),
            heading_rad=rng.uniform(0, 2 * math.pi),
            core_temp_c=AMBIENT_TEMP_C + rng.uniform(0, 8),
        )
        roll = rng.random()
        if roll < 0.6:
            vehicle.mode, vehicle.speed_mph = Mode.DRIVING, rng.uniform(10, 50)
        elif roll < 0.85:
            vehicle.mode = Mode.PARKED
        else:
            vehicle._start_charging(rng)
        fleet.append(vehicle)
    return fleet


# --------------------------------------------------------------------------- #
# Sinks
# --------------------------------------------------------------------------- #
class StdoutSink:
    """Prints one JSON object per line. Logs go to stderr, so stdout stays clean."""

    def send(self, event: dict) -> None:
        print(json.dumps(event), flush=False)

    def flush(self) -> None:
        sys.stdout.flush()

    def close(self) -> None:
        self.flush()


class KafkaSink:
    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        from kafka import KafkaProducer  # imported here so --dry-run works without Kafka

        self.topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=[s.strip() for s in bootstrap_servers.split(",")],
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",  # wait for the broker to persist the record
            retries=5,
            linger_ms=20,  # tiny batching window: one tick = 20 records in a few requests
        )

    def send(self, event: dict) -> None:
        # Keying by vehicle_id keeps each vehicle's readings ordered within one partition.
        future = self._producer.send(self.topic, key=event["vehicle_id"], value=event)
        future.add_errback(lambda exc: logger.error("Failed to deliver %s: %s", event["event_id"], exc))

    def flush(self) -> None:
        self._producer.flush(timeout=10)

    def close(self) -> None:
        self._producer.flush(timeout=10)
        self._producer.close()


# --------------------------------------------------------------------------- #
# Main loop
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream simulated EV fleet telemetry to Kafka.")
    parser.add_argument("--dry-run", action="store_true", help="print events to stdout instead of Kafka")
    parser.add_argument("--ticks", type=int, default=None, help="stop after N ticks (default: run forever)")
    parser.add_argument("--no-sleep", action="store_true", help="don't wait between ticks (fast simulation)")
    parser.add_argument("--seed", type=int, default=None, help="random seed for reproducible runs")
    return parser.parse_args()


def _raise_keyboard_interrupt(signum, frame):  # noqa: ARG001
    raise KeyboardInterrupt


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)

    settings = load_settings()
    rng = random.Random(args.seed)
    fleet = build_fleet(settings.num_vehicles, rng)

    if args.dry_run:
        sink = StdoutSink()
        logger.info("Dry run: printing %d vehicles' telemetry to stdout", len(fleet))
    else:
        sink = KafkaSink(settings.kafka_bootstrap_servers, settings.kafka_topic)
        logger.info("Streaming %d vehicles to topic '%s' at %s",
                    len(fleet), settings.kafka_topic, settings.kafka_bootstrap_servers)

    tick, total_sent, dt = 0, 0, 1.0
    try:
        while args.ticks is None or tick < args.ticks:
            overheating = []
            for vehicle in fleet:
                vehicle.step(dt, rng)
                event = vehicle.to_event()
                sink.send(event)
                if event["battery_temp_c"] > OVERHEAT_THRESHOLD_C:
                    overheating.append(f'{event["vehicle_id"]}={event["battery_temp_c"]}°C')
            sink.flush()

            tick += 1
            total_sent += len(fleet)
            if overheating:
                logger.warning("tick %d: OVERHEAT %s", tick, ", ".join(overheating))
            if tick % 10 == 0:
                logger.info("tick %d: %d events sent so far", tick, total_sent)

            # The next tick simulates exactly the time we wait now.
            dt = rng.uniform(settings.min_interval_seconds, settings.max_interval_seconds)
            if not args.no_sleep:
                time.sleep(dt)
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        sink.close()
        logger.info("Done. %d events sent in %d ticks.", total_sent, tick)


if __name__ == "__main__":
    main()
