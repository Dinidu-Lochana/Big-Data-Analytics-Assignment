import argparse
import uuid

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import config


def decode_headers(msg) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in msg.headers() or []:
        out[key] = value.decode("utf-8", errors="replace") if value else ""
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Read the orders DLQ.")
    parser.add_argument(
        "--follow", action="store_true", help="keep tailing instead of exiting at the end",
    )
    args = parser.parse_args()

    schema_registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(
        schema_registry, config.load_schema(), lambda obj, ctx: obj
    )

    consumer = Consumer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            # A throwaway group so every run replays the DLQ from the start.
            "group.id": f"dlq-viewer-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )
    consumer.subscribe([config.DLQ_TOPIC])
    ctx = SerializationContext(config.DLQ_TOPIC, MessageField.VALUE)

    print(f"[dlq] reading {config.DLQ_TOPIC}\n")
    seen = 0
    idle = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                if not args.follow and idle > 5:
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    if not args.follow:
                        break
                    continue
                print(f"[dlq] error: {msg.error()}")
                continue

            idle = 0
            seen += 1
            headers = decode_headers(msg)

            try:
                order = deserializer(msg.value(), ctx)
                payload = (
                    f"orderId={order['orderId']} product={order['product']} "
                    f"price={order['price']:.2f}"
                )
            except Exception as exc:  # noqa: BLE001 - undecodable poison pill
                payload = f"<undecodable: {exc}>"

            print(f"--- DLQ message #{seen} (offset {msg.offset()}) ---")
            print(f"  payload : {payload}")
            print(f"  reason  : {headers.get('x-error-reason', '?')}")
            print(f"  attempts: {headers.get('x-attempts', '?')}")
            print(
                f"  origin  : {headers.get('x-original-topic', '?')}"
                f"[{headers.get('x-original-partition', '?')}]"
                f"@{headers.get('x-original-offset', '?')}"
            )
            print(f"  failedAt: {headers.get('x-failed-at', '?')}\n")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print(f"[dlq] {seen} message(s) in the dead letter queue")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
