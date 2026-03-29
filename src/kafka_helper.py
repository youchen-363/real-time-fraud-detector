"""
kafka_consumer.py — Python consumer using confluent-kafka (librdkafka)

Why confluent-kafka over kafka-python for low latency:
  - Built on librdkafka (C library): sub-millisecond poll overhead
  - Non-blocking poll() model: no thread blocking, GIL-friendly
  - Fine-grained librdkafka config keys exposed directly
  - 3x higher producer throughput, ~50% higher consumer throughput vs kafka-python
  - Actively maintained by Confluent (the company that created Kafka)

Install:
    pip install confluent-kafka

Run:
    python kafka_consumer.py
"""

import json
import signal
import sys
import time
from confluent_kafka import Consumer, KafkaException, KafkaError

# ── Config ────────────────────────────────────────────────────────────────────

# confluent-kafka uses librdkafka config keys (dot-separated strings),
# NOT the camelCase keys used by kafka-python.
CONSUMER_CONFIG = {
    "bootstrap.servers":        "localhost:9092",
    "group.id":                 "python-pipeline-group",
    "auto.offset.reset":        "latest",        # only new messages on first start

    # ── Latency tuning ───────────────────────────────────────────────────────
    # How often librdkafka sends heartbeats to the broker (ms).
    # Lower = faster detection of consumer failure / rebalance.
    "heartbeat.interval.ms":    1000,

    # Max time between poll() calls before the broker considers this
    # consumer dead and triggers a rebalance. Must be > heartbeat.interval.ms.
    "session.timeout.ms":       6000,

    # Max time the broker will block a fetch request waiting for min.bytes.
    # Lower = lower latency at the cost of more small fetches.
    "fetch.wait.max.ms":        5,        # 5 ms (default 500) — key low-latency knob

    # Minimum bytes the broker accumulates before responding to a fetch.
    # Set low for latency, higher (65536+) for throughput.
    "fetch.min.bytes":          1,        # respond immediately with any data

    # Max bytes per partition per fetch request.
    "max.partition.fetch.bytes": 1048576, # 1 MB

    # How many messages to buffer internally (pre-fetched from broker).
    "queued.max.messages.kbytes": 32768,  # 32 MB internal queue

    # Commit offsets every N ms automatically.
    "enable.auto.commit":       True,
    "auto.commit.interval.ms":  1000,

    # ── Reliability ──────────────────────────────────────────────────────────
    # Reconnect backoff — how fast to retry a lost broker connection.
    "reconnect.backoff.ms":     50,
    "reconnect.backoff.max.ms": 1000,
}

TOPICS = ["processed-data", "metrics-data"]

# ── Consumer setup ────────────────────────────────────────────────────────────

consumer = Consumer(CONSUMER_CONFIG)
consumer.subscribe(TOPICS)

# ── Graceful shutdown ─────────────────────────────────────────────────────────

running = True

def shutdown(sig, frame):
    global running
    print("\nShutting down consumer...")
    running = False

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ── Your downstream operations ────────────────────────────────────────────────

def process_record(record: dict, topic: str):
    """
    Drop your Python logic here.
    Examples:
      - write to a database (PostgreSQL, ClickHouse, etc.)
      - call an ML model for inference
      - push to a REST API
      - stream into pandas / polars / DuckDB
    """
    if topic == "processed-data":
        record_id = record.get("id")
        timestamp = record.get("timestamp")
        source    = record.get("source")
        value     = record.get("value")
        category  = record.get("category")
        status    = record.get("status")

        # TODO: replace with your logic
        print(f"[DATA]    id={record_id:<12} source={source:<10} "
              f"category={category:<15} value={value:<8.2f} status={status}")

    elif topic == "metrics-data":
        category = record.get("category")
        count    = record.get("count")
        avg      = record.get("avg")
        print(f"[METRICS] category={category:<15} count={count:<6} avg={avg:.4f}")


# ── Stats (optional) ──────────────────────────────────────────────────────────

_msg_count   = 0
_last_report = time.monotonic()

def maybe_report_throughput():
    """Print a throughput line every 5 seconds for monitoring."""
    global _msg_count, _last_report
    now = time.monotonic()
    elapsed = now - _last_report
    if elapsed >= 5.0:
        rate = _msg_count / elapsed
        print(f"[THROUGHPUT] {rate:.0f} msg/s over last {elapsed:.1f}s")
        _msg_count   = 0
        _last_report = now


# ── Main poll loop — always ready to receive ──────────────────────────────────
#
# confluent-kafka uses a poll()-based model instead of a blocking iterator.
# This is intentional: poll() is non-blocking and integrates well with
# asyncio or other event loops if you need them later.
#
# timeout=0.001 (1 ms) means: "return immediately if no message,
# but don't spin-wait at 100% CPU" — ideal for low-latency workloads.

print(f"✅ confluent-kafka consumer ready — subscribed to: {TOPICS}")

try:
    while running:
        # poll() returns ONE message (or None on timeout / error).
        # For batch processing, call poll() in a tight loop.
        msg = consumer.poll(timeout=0.001)   # 1 ms timeout — low-latency sweet spot

        if msg is None:
            # No message in the last 1 ms — keep polling.
            maybe_report_throughput()
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition — not an error, just caught up.
                print(f"[EOF] {msg.topic()} partition {msg.partition()} "
                      f"reached offset {msg.offset()}")
            else:
                # Real error — log and continue (don't crash the pipeline).
                print(f"[KAFKA ERROR] {msg.error()}")
            continue

        # Deserialize JSON payload
        try:
            record = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"[DESERIALIZE ERROR] {e} | raw={msg.value()}")
            continue

        # Hand off to your downstream logic
        try:
            process_record(record, msg.topic())
            _msg_count += 1
        except Exception as e:
            print(f"[PROCESS ERROR] {e} | record={record}")

finally:
    # Always close cleanly — commits pending offsets and leaves the group.
    consumer.close()
    print("Consumer closed.")