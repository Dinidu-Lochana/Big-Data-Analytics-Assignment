from dataclasses import dataclass, field


@dataclass
class Stats:
    count: int = 0
    total: float = 0.0
    mean: float = 0.0
    minimum: float = float("inf")
    maximum: float = float("-inf")

    def update(self, price: float) -> None:
        self.count += 1
        self.total += price
        # Incremental mean: mean += (x - mean) / n
        self.mean += (price - self.mean) / self.count
        self.minimum = min(self.minimum, price)
        self.maximum = max(self.maximum, price)


@dataclass
class PriceAggregator:
    """Overall running average plus a per-product breakdown."""

    overall: Stats = field(default_factory=Stats)
    per_product: dict[str, Stats] = field(default_factory=dict)

    def add(self, product: str, price: float) -> Stats:
        self.overall.update(price)
        stats = self.per_product.setdefault(product, Stats())
        stats.update(price)
        return self.overall

    def summary(self) -> str:
        lines = [
            "",
            "=" * 62,
            " RUNNING AGGREGATION SUMMARY",
            "=" * 62,
            f" {'PRODUCT':<12} {'COUNT':>7} {'AVG':>12} {'MIN':>12} {'MAX':>12}",
            "-" * 62,
        ]
        for product in sorted(self.per_product):
            s = self.per_product[product]
            lines.append(
                f" {product:<12} {s.count:>7} {s.mean:>12.2f} "
                f"{s.minimum:>12.2f} {s.maximum:>12.2f}"
            )
        o = self.overall
        lines.append("-" * 62)
        if o.count:
            lines.append(
                f" {'ALL':<12} {o.count:>7} {o.mean:>12.2f} "
                f"{o.minimum:>12.2f} {o.maximum:>12.2f}"
            )
        else:
            lines.append(" no messages processed")
        lines.append("=" * 62)
        return "\n".join(lines)
