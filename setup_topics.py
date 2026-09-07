import sys

from confluent_kafka.admin import AdminClient, NewTopic

import config


def main() -> int:
    admin = AdminClient({"bootstrap.servers": config.BOOTSTRAP_SERVERS})

    existing = set(admin.list_topics(timeout=10).topics)

    wanted = [
        NewTopic(
            config.ORDERS_TOPIC,
            num_partitions=config.ORDERS_PARTITIONS,
            replication_factor=config.REPLICATION_FACTOR,
        ),
        NewTopic(
            config.DLQ_TOPIC,
            num_partitions=config.DLQ_PARTITIONS,
            replication_factor=config.REPLICATION_FACTOR,
            # Keep failed messages around for a week so they can be inspected.
            config={"retention.ms": str(7 * 24 * 60 * 60 * 1000)},
        ),
    ]

    todo = [t for t in wanted if t.topic not in existing]
    for t in wanted:
        if t.topic in existing:
            print(f"  = {t.topic} already exists, skipping")

    if not todo:
        return 0

    for topic, future in admin.create_topics(todo).items():
        try:
            future.result()
            print(f"  + created {topic}")
        except Exception as exc:  # noqa: BLE001 - report and keep going
            print(f"  ! failed to create {topic}: {exc}", file=sys.stderr)
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
