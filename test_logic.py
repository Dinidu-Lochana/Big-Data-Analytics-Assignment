import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import consumer as C  # noqa: E402
from aggregator import PriceAggregator  # noqa: E402


class FakeMsg:
    """Stands in for a confluent_kafka Message."""

    def topic(self):
        return "orders"

    def partition(self):
        return 0

    def offset(self):
        return 42

    def key(self):
        return b"1001"

    def value(self):
        return b"\x00rawbytes"


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key, value, headers):
        self.sent.append(dict(headers))

    def flush(self, timeout=None):
        return 0


def test_running_average_matches_batch_mean():
    """The incremental mean must equal the naive sum/count mean."""
    random.seed(7)
    agg = PriceAggregator()
    prices = [round(random.uniform(5, 500), 2) for _ in range(1000)]
    for i, p in enumerate(prices):
        agg.add(f"Item{i % 5 + 1}", p)

    assert agg.overall.count == 1000
    assert abs(agg.overall.mean - sum(prices) / len(prices)) < 1e-9
    assert sum(s.count for s in agg.per_product.values()) == 1000


def test_transient_failures_exhaust_retries_then_dlq():
    C.config.MAX_ATTEMPTS = 3
    C.config.RETRY_BASE_DELAY = 0.001
    original = C.process_order
    C.process_order = lambda order: (_ for _ in ()).throw(C.TransientError("boom"))
    try:
        producer = FakeProducer()
        assert C.handle_with_retries({"orderId": "1001"}, producer, FakeMsg()) is False
        assert len(producer.sent) == 1
        assert producer.sent[0]["x-attempts"] == b"3"
        assert producer.sent[0]["x-original-offset"] == b"42"
    finally:
        C.process_order = original


def test_permanent_failure_skips_retries():
    C.config.RETRY_BASE_DELAY = 0.001
    original = C.process_order
    C.process_order = lambda order: (_ for _ in ()).throw(C.PermanentError("bad data"))
    try:
        producer = FakeProducer()
        assert C.handle_with_retries({"orderId": "1002"}, producer, FakeMsg()) is False
        # Straight to the DLQ on the first attempt: no point retrying bad data.
        assert producer.sent[0]["x-attempts"] == b"1"
    finally:
        C.process_order = original


def test_message_recovering_on_retry_is_not_dead_lettered():
    C.config.MAX_ATTEMPTS = 3
    C.config.RETRY_BASE_DELAY = 0.001
    original = C.process_order
    calls = {"n": 0}

    def flaky(order):
        calls["n"] += 1
        if calls["n"] < 2:
            raise C.TransientError("temporary")

    C.process_order = flaky
    try:
        producer = FakeProducer()
        assert C.handle_with_retries({"orderId": "1003"}, producer, FakeMsg()) is True
        assert producer.sent == []
    finally:
        C.process_order = original


def test_backoff_grows_and_is_capped():
    C.config.RETRY_BASE_DELAY = 0.5
    C.config.RETRY_MAX_DELAY = 8.0
    # Full jitter means each delay lands in [0.5x, 1.0x] of the nominal value.
    for attempt in range(1, 8):
        nominal = min(0.5 * 2 ** (attempt - 1), 8.0)
        delay = C.backoff_delay(attempt)
        assert nominal * 0.5 <= delay <= nominal


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"  PASS  {test.__name__}")
    print(f"\n{len(tests)} test(s) passed")
