"""Order producer.

Generates randomized `Order` records, serializes them with Avro against the
Confluent Schema Registry, and publishes them to the `orders` topic.

    python src/producer.py                  # 50 orders, 4/second
    python src/producer.py -n 200 -r 20     # 200 orders, 20/second
    python src/producer.py -n 0             # run forever until Ctrl-C

Producer-side reliability (the first layer of "retry logic"):
  * acks=all             -> the leader waits for all in-sync replicas
  * enable.idempotence   -> librdkafka retries internally without duplicating
  * retries + backoff    -> transient broker/network errors are retried for us
"""
import argparse
import random
import signal
import sys
import time
import uuid

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

import config

running = True


def _stop(signum, frame):  # noqa: ARG001 - signal handler signature
    global running
    running = False
    print("\n[producer] stopping, flushing outstanding messages...")


def make_order(seq: int) -> dict:
    """Build one randomized order matching schemas/order.avsc."""
    return {
        "orderId": str(1000 + seq),
        "product": random.choice(config.PRODUCTS),
        # Avro `float` is 32-bit, so expect tiny precision drift on the
        # consumer side (19.99 comes back as 19.989999771118164).
        "price": round(random.uniform(config.MIN_PRICE, config.MAX_PRICE), 2),
    }


def delivery_report(err, msg) -> None:
    """Called once per message, on success or after all retries are exhausted."""
    if err is not None:
        print(f"[producer] DELIVERY FAILED key={msg.key()}: {err}", file=sys.stderr)
        return
    key = msg.key().decode("utf-8") if msg.key() else "-"
    print(
        f"[producer] sent orderId={key:<6} "
        f"partition={msg.partition()} offset={msg.offset()}"
    )


def build_producer() -> tuple[Producer, AvroSerializer, StringSerializer]:
    schema_registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})

    # to_dict=lambda obj, ctx: obj  -> we already hand the serializer plain dicts.
    avro_serializer = AvroSerializer(
        schema_registry,
        config.load_schema(),
        lambda obj, ctx: obj,
    )

    producer = Producer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 10,
            "retry.backoff.ms": 200,
            "delivery.timeout.ms": 120000,
            "linger.ms": 20,
            "compression.type": "snappy",
            "client.id": f"order-producer-{uuid.uuid4().hex[:8]}",
        }
    )
    return producer, avro_serializer, StringSerializer("utf_8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Produce Avro order messages.")
    parser.add_argument(
        "-n", "--count", type=int, default=50,
        help="number of orders to send; 0 means run until Ctrl-C (default: 50)",
    )
    parser.add_argument(
        "-r", "--rate", type=float, default=4.0,
        help="messages per second (default: 4)",
    )
    parser.add_argument(
        "--seed", type=int, default=None, help="seed the RNG for repeatable runs",
    )
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    producer, avro_serializer, key_serializer = build_producer()
    ctx = SerializationContext(config.ORDERS_TOPIC, MessageField.VALUE)
    interval = 1.0 / args.rate if args.rate > 0 else 0.0

    print(
        f"[producer] topic={config.ORDERS_TOPIC} "
        f"broker={config.BOOTSTRAP_SERVERS} registry={config.SCHEMA_REGISTRY_URL}"
    )

    sent = 0
    try:
        while running and (args.count == 0 or sent < args.count):
            order = make_order(sent)

            producer.produce(
                topic=config.ORDERS_TOPIC,
                # Keying by orderId guarantees per-order ordering within a partition.
                key=key_serializer(order["orderId"]),
                value=avro_serializer(order, ctx),
                on_delivery=delivery_report,
            )
            sent += 1

            # Serve delivery callbacks without blocking the send loop.
            producer.poll(0)
            if interval:
                time.sleep(interval)
    except BufferError:
        print("[producer] local queue full, flushing...", file=sys.stderr)
        producer.flush()
    finally:
        remaining = producer.flush(30)
        if remaining:
            print(f"[producer] {remaining} message(s) undelivered", file=sys.stderr)
        print(f"[producer] done, {sent} order(s) produced")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
