"""Replay the saved Assessment 2 holdout as JSON-array Kafka messages."""

import argparse
import json
import math
import sys
import time
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path

import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parent
PAUSE_SECONDS = 5
MAX_PAYLOAD_BYTES = 900_000

SOURCE_COLUMNS = [
    "trip_distance",
    "trip_duration_minutes",
    "passenger_count",
    "tpep_pickup_datetime",
    "RatecodeID",
    "pickup_borough",
    "dropoff_borough",
    "VendorID",
    "fare_amount",
]


def finite_float_or_none(value):
    """Preserve finite numbers and encode missing/nonfinite values as null."""
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def pickup_timestamp_as_utc(value):
    """Preserve the instant stored in Spark's timestamp Parquet column."""
    if not isinstance(value, datetime):
        raise ValueError("The saved pickup timestamp is missing or invalid.")
    if value.tzinfo is None:
        # Direct Parquet reads expose Spark's stored UTC instant.
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def prepare_record(row):
    """Keep source values; feature engineering belongs to the saved model."""
    record = {
        "trip_distance": finite_float_or_none(row["trip_distance"]),
        "trip_duration_minutes": finite_float_or_none(
            row["trip_duration_minutes"]
        ),
        "passenger_count": finite_float_or_none(row["passenger_count"]),
        "tpep_pickup_datetime": pickup_timestamp_as_utc(
            row["tpep_pickup_datetime"]
        ),
        "RatecodeID": finite_float_or_none(row["RatecodeID"]),
        "pickup_borough": row["pickup_borough"],
        "dropoff_borough": row["dropoff_borough"],
        "VendorID": (
            None if row["VendorID"] is None else int(row["VendorID"])
        ),
        "fare_amount": finite_float_or_none(row["fare_amount"]),
    }
    for column in ["trip_distance", "trip_duration_minutes", "fare_amount"]:
        if record[column] is None:
            raise ValueError(f"Saved record has an invalid {column}.")
    return record


def iter_record_batches(parquet_files, batch_size):
    """Read bounded batches and carry incomplete batches across files."""
    pending = []
    for path in parquet_files:
        parquet_file = pq.ParquetFile(
            path,
            coerce_int96_timestamp_unit="us",
        )
        try:
            missing = set(SOURCE_COLUMNS) - set(parquet_file.schema_arrow.names)
            if missing:
                raise ValueError(f"{path.name} is missing columns: {sorted(missing)}")
            for arrow_batch in parquet_file.iter_batches(
                batch_size=batch_size,
                columns=SOURCE_COLUMNS,
            ):
                for row in arrow_batch.to_pylist():
                    pending.append(prepare_record(row))
                    if len(pending) == batch_size:
                        yield pending
                        pending = []
        finally:
            parquet_file.close()
    if pending:
        yield pending


def log_event(action, **fields):
    """Write one structured log entry to stdout."""
    print(json.dumps({"action": action, **fields}, allow_nan=False), flush=True)


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-path", type=Path,
        default=PROJECT_ROOT / "data/stream_data.parquet",
    )
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="a2_taxi_events")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument(
        "--max-batches", type=int, default=60,
        help="Maximum batches to replay; use 0 for the complete holdout.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Inspect JSON batches locally without Kafka or timing pauses.",
    )
    args = parser.parse_args(argv)
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive.")
    if args.max_batches < 0:
        parser.error("--max-batches must be zero or positive.")
    return args


def main(argv=None):
    args = parse_arguments(argv)
    parquet_files = sorted(args.data_path.rglob("part-*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No saved Parquet part files in {args.data_path}.")

    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers.split(","),
            client_id="ito5202-a2-producer",
            acks="all",
            max_in_flight_requests_per_connection=1,
            max_request_size=1_048_576,
            request_timeout_ms=20_000,
            delivery_timeout_ms=30_000,
            max_block_ms=20_000,
            allow_auto_create_topics=False,
        )

    source_batches = iter_record_batches(parquet_files, args.batch_size)
    batches = (
        islice(source_batches, args.max_batches)
        if args.max_batches else source_batches
    )
    record_count = 0
    batch_count = 0
    previous_batch_started = None
    started = time.perf_counter()
    log_event(
        "started", dry_run=args.dry_run, topic=args.topic,
        source_files=len(parquet_files), batch_size=args.batch_size,
        max_batches=args.max_batches, pause_seconds=PAUSE_SECONDS,
    )

    try:
        for batch_number, records in enumerate(batches, start=1):
            if not args.dry_run and batch_number > 1:
                time.sleep(PAUSE_SECONDS)

            batch_started = time.perf_counter()
            publication_time = datetime.now(timezone.utc)
            event_timestamp = publication_time.isoformat(timespec="microseconds")
            for record in records:
                record["event_timestamp"] = event_timestamp

            payload = json.dumps(
                records, ensure_ascii=False, allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
            if len(payload) > MAX_PAYLOAD_BYTES:
                raise ValueError(
                    f"Payload has {len(payload):,} bytes, exceeding the "
                    f"{MAX_PAYLOAD_BYTES:,}-byte limit. Reduce --batch-size."
                )

            delivery = {}
            if producer is not None:
                send_started = time.perf_counter()
                metadata = producer.send(
                    args.topic, value=payload,
                    timestamp_ms=int(publication_time.timestamp() * 1000),
                ).get(timeout=35)
                delivery = {
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                    "delivery_seconds": round(
                        time.perf_counter() - send_started, 6
                    ),
                }

            record_count += len(records)
            batch_count += 1
            log_event(
                "dry_run_batch" if args.dry_run else "published_batch",
                batch=batch_number, records=len(records),
                event_timestamp=event_timestamp, topic=args.topic,
                payload_bytes=len(payload), cumulative_records=record_count,
                interval_seconds=(
                    None if previous_batch_started is None
                    else round(batch_started - previous_batch_started, 6)
                ),
                **delivery,
            )
            if args.dry_run and batch_number == 1:
                log_event("preview", record=records[0])
            previous_batch_started = batch_started
        if batch_count == 0:
            raise ValueError("The saved streaming dataset contains no records.")
    finally:
        source_batches.close()
        if producer is not None:
            producer.close(timeout=10)

    log_event(
        "completed", dry_run=args.dry_run,
        batches=batch_count, records_processed=record_count,
        elapsed_seconds=round(time.perf_counter() - started, 3),
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log_event("interrupted")
        sys.exit(130)
    except Exception as error:
        print(
            json.dumps({"action": "error", "message": str(error)}),
            file=sys.stderr, flush=True,
        )
        sys.exit(1)
