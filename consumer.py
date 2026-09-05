import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import config
from aggregator import PriceAggregator

running = True


def _stop(signum, frame):  # noqa: ARG001 - signal handler signature
    global running
    running = False
    print("\n[consumer] shutdown requested...")


# --------------------------------------------------------------------------
# Failure taxonomy: the distinction that drives the whole retry/DLQ decision.
# --------------------------------------------------------------------------
class TransientError(Exception):
    """Something that might succeed if we simply try again (timeout, 503...)."""


class PermanentError(Exception):
    """Retrying cannot help (invalid data, business-rule violation...)."""


def process_order(order: dict) -> None:
    """Pretend to do real downstream work with the order.

    Replace the body of this function with a database write or an HTTP call to
    a payments service; the surrounding retry/DLQ machinery does not change.
    """
    # --- Real validation: bad data is permanently bad, never retry it. ------
    price = order.get("price")
    if price is None or price <= 0:
        raise PermanentError(f"invalid price {price!r}")
    if not order.get("orderId"):
        raise PermanentError("missing orderId")

    # --- Simulated downstream failures so the demo shows both paths. --------
    roll = random.random()
    if roll < config.PERMANENT_FAILURE_RATE:
        raise PermanentError("downstream rejected the order (simulated)")
    if roll < config.PERMANENT_FAILURE_RATE + config.TRANSIENT_FAILURE_RATE:
        raise TransientError("downstream timed out (simulated)")

    # Real work would happen here.


def backoff_delay(attempt: int) -> float:
    """Exponential backoff with full jitter, capped at RETRY_MAX_DELAY."""
    raw = config.RETRY_BASE_DELAY * (2 ** (attempt - 1))
    return min(raw, config.RETRY_MAX_DELAY) * (0.5 + random.random() / 2)


def send_to_dlq(dlq_producer: Producer, msg, reason: str, attempts: int) -> None:
    """Forward the original message bytes to the DLQ with diagnostic headers.

    We republish the *raw* value rather than re-serializing the decoded record.
    That way a message that failed to deserialize can still be captured, and
    nothing is lost or altered in translation.
    """
    headers = [
        ("x-error-reason", reason.encode("utf-8")),
        ("x-attempts", str(attempts).encode("utf-8")),
        ("x-original-topic", msg.topic().encode("utf-8")),
        ("x-original-partition", str(msg.partition()).encode("utf-8")),
        ("x-original-offset", str(msg.offset()).encode("utf-8")),
        ("x-failed-at", datetime.now(timezone.utc).isoformat().encode("utf-8")),
    ]

    dlq_producer.produce(
        topic=config.DLQ_TOPIC,
        key=msg.key(),
        value=msg.value(),
        headers=headers,
    )
    # Block until the DLQ write is acknowledged; only then is it safe to commit
    # the source offset, otherwise a crash here would lose the message entirely.
    dlq_producer.flush(10)
    print(f"[consumer] -> DLQ after {attempts} attempt(s): {reason}")


def handle_with_retries(order: dict, dlq_producer: Producer, msg) -> bool:
    """Run process_order with retries. Returns True if it eventually succeeded."""
    order_id = order.get("orderId", "?")

    for attempt in range(1, config.MAX_ATTEMPTS + 1):
        try:
            process_order(order)
            if attempt > 1:
                print(f"[consumer] orderId={order_id} recovered on attempt {attempt}")
            return True

        except PermanentError as exc:
            send_to_dlq(dlq_producer, msg, f"PermanentError: {exc}", attempt)
            return False

        except TransientError as exc:
            if attempt >= config.MAX_ATTEMPTS:
                send_to_dlq(
                    dlq_producer,
                    msg,
                    f"TransientError after {attempt} attempts: {exc}",
                    attempt,
                )
                return False

            delay = backoff_delay(attempt)
            print(
                f"[consumer] orderId={order_id} attempt {attempt}/"
                f"{config.MAX_ATTEMPTS} failed ({exc}); retrying in {delay:.2f}s"
            )
            time.sleep(delay)

        except Exception as exc:  # noqa: BLE001 - unknown bugs are not retryable
            send_to_dlq(dlq_producer, msg, f"{type(exc).__name__}: {exc}", attempt)
            return False

    return False


def main() -> int:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    schema_registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(
        schema_registry,
        config.load_schema(),
        lambda obj, ctx: obj,  # hand back plain dicts
    )

    consumer = Consumer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "group.id": config.CONSUMER_GROUP,
            "auto.offset.reset": config.AUTO_OFFSET_RESET,
            # Manual commits: see the module docstring for why this matters.
            "enable.auto.commit": False,
            # Retry sleeps happen between polls, so the poll interval must
            # comfortably exceed the worst-case total backoff.
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 45000,
        }
    )

    dlq_producer = Producer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
            "client.id": "dlq-producer",
        }
    )

    consumer.subscribe([config.ORDERS_TOPIC])
    aggregator = PriceAggregator()
    ctx = SerializationContext(config.ORDERS_TOPIC, MessageField.VALUE)

    print(
        f"[consumer] group={config.CONSUMER_GROUP} topic={config.ORDERS_TOPIC} "
        f"dlq={config.DLQ_TOPIC} max_attempts={config.MAX_ATTEMPTS}"
    )
    print("[consumer] waiting for messages, Ctrl-C to stop\n")

    processed = dead_lettered = 0

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            # Deserialize. A failure here is a poison pill: the bytes
            try:
                order = avro_deserializer(msg.value(), ctx)
            except Exception as exc:  # noqa: BLE001
                send_to_dlq(dlq_producer, msg, f"DeserializationError: {exc}", 1)
                dead_lettered += 1
                consumer.commit(message=msg, asynchronous=False)
                continue

            # Process with retries, or route to the DLQ. --------------
            if handle_with_retries(order, dlq_producer, msg):
                # --- 3. Aggregate only what actually succeeded. -------------
                overall = aggregator.add(order["product"], float(order["price"]))
                processed += 1
                print(
                    f"[consumer] orderId={order['orderId']:<6} "
                    f"product={order['product']:<6} "
                    f"price={order['price']:>8.2f} | "
                    f"n={overall.count:<5} running avg={overall.mean:.2f}"
                )
            else:
                dead_lettered += 1

            # Commit only after the message reached a terminal state. -
            consumer.commit(message=msg, asynchronous=False)

    except KafkaException as exc:
        print(f"[consumer] fatal Kafka error: {exc}", file=sys.stderr)
        return 1
    finally:
        print(aggregator.summary())
        print(f" processed OK : {processed}")
        print(f" sent to DLQ  : {dead_lettered}")
        dlq_producer.flush(10)
        consumer.close()
        print("[consumer] closed cleanly")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
