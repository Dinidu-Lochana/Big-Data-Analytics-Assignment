# Kafka Order Pipeline — Avro, Real-Time Aggregation, Retries & DLQ

A Kafka producer/consumer system in Python. Orders are serialized with **Avro**
against the **Confluent Schema Registry**, consumed with a **running average**
of prices, retried on transient failures, and routed to a **Dead Letter Queue**
when they cannot be processed.

```
┌────────────┐   Avro    ┌───────────┐            ┌────────────┐
│ producer.py│ ────────► │  orders   │ ─────────► │ consumer.py│
└─────┬──────┘  (3 part) └───────────┘            └─────┬──────┘
      │                        ▲                        │
      │ registers schema       │ schema id in           │ running average
      ▼                        │ every message          │ + retry w/ backoff
┌──────────────────┐───────────┘                        │
│ Schema Registry  │                                    ▼
│  :8081           │                            ┌───────────────┐
└──────────────────┘                            │  orders.DLQ   │ ◄── dlq_viewer.py
                                                └───────────────┘
```

---