import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_PATH = PROJECT_ROOT / "order.avsc"


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


# --- Cluster ---------------------------------------------------------------
BOOTSTRAP_SERVERS = _env("BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = _env("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# --- Topics ----------------------------------------------------------------
ORDERS_TOPIC = _env("ORDERS_TOPIC", "orders")
DLQ_TOPIC = _env("DLQ_TOPIC", "orders.DLQ")
ORDERS_PARTITIONS = int(_env("ORDERS_PARTITIONS", "3"))
DLQ_PARTITIONS = int(_env("DLQ_PARTITIONS", "1"))
REPLICATION_FACTOR = int(_env("REPLICATION_FACTOR", "1"))

# --- Consumer --------------------------------------------------------------
CONSUMER_GROUP = _env("CONSUMER_GROUP", "order-processor")
AUTO_OFFSET_RESET = _env("AUTO_OFFSET_RESET", "earliest")

# --- Retry policy ----------------------------------------------------------
# Total attempts per message before it is routed to the DLQ.
MAX_ATTEMPTS = int(_env("MAX_ATTEMPTS", "3"))
# Exponential backoff: delay = RETRY_BASE_DELAY * (2 ** (attempt - 1)) + jitter
RETRY_BASE_DELAY = float(_env("RETRY_BASE_DELAY", "0.5"))
RETRY_MAX_DELAY = float(_env("RETRY_MAX_DELAY", "8.0"))

# --- Failure injection (demo only) -----------------------------------------
# Lets you show retry + DLQ behaviour live without breaking anything for real.
TRANSIENT_FAILURE_RATE = float(_env("TRANSIENT_FAILURE_RATE", "0.20"))
PERMANENT_FAILURE_RATE = float(_env("PERMANENT_FAILURE_RATE", "0.05"))

# --- Producer demo data ----------------------------------------------------
PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]
MIN_PRICE = float(_env("MIN_PRICE", "5.0"))
MAX_PRICE = float(_env("MAX_PRICE", "500.0"))


def load_schema() -> str:
    """Return the raw Avro schema text used by both serializer and deserializer."""
    return SCHEMA_PATH.read_text(encoding="utf-8")
