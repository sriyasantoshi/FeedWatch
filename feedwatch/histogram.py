import math
from typing import List, Dict, Tuple, Optional


class LatencyHistogram:
    """
    High-precision logarithmic/bucketed histogram for latency measurements in microseconds (us).
    Supports fast recording, quantile calculation (p50, p90, p95, p99, p99.9, p99.99),
    and ASCII representation.
    """

    def __init__(self, min_val_us: float = 1.0, max_val_us: float = 10_000_000.0, precision_digits: int = 3):
        self.min_val_us = max(0.1, min_val_us)
        self.max_val_us = max_val_us
        self.precision_digits = precision_digits
        
        self._samples: List[float] = []
        self._count = 0
        self._sum = 0.0
        self._min = float("inf")
        self._max = 0.0

        self._mult = 1.0 + (10 ** (-precision_digits + 1))
        self._log_mult = math.log(self._mult)
        self._num_buckets = int(math.ceil(math.log(max_val_us / self.min_val_us) / self._log_mult)) + 2
        self._buckets = [0] * self._num_buckets

    def record(self, latency_us: float) -> None:
        if latency_us < 0:
            return

        self._count += 1
        self._sum += latency_us
        if latency_us < self._min:
            self._min = latency_us
        if latency_us > self._max:
            self._max = latency_us

        if len(self._samples) < 1_000_000:
            self._samples.append(latency_us)

        if latency_us <= self.min_val_us:
            bucket_idx = 0
        elif latency_us >= self.max_val_us:
            bucket_idx = self._num_buckets - 1
        else:
            bucket_idx = int(math.log(latency_us / self.min_val_us) / self._log_mult) + 1
            if bucket_idx >= self._num_buckets:
                bucket_idx = self._num_buckets - 1
        self._buckets[bucket_idx] += 1

    @property
    def count(self) -> int:
        return self._count

    @property
    def min(self) -> float:
        return self._min if self._count > 0 else 0.0

    @property
    def max(self) -> float:
        return self._max if self._count > 0 else 0.0

    @property
    def mean(self) -> float:
        return (self._sum / self._count) if self._count > 0 else 0.0

    def percentile(self, p: float) -> float:
        if self._count == 0:
            return 0.0

        if 0 < len(self._samples) == self._count:
            sorted_samples = sorted(self._samples)
            idx = int(math.ceil((p / 100.0) * len(sorted_samples))) - 1
            idx = max(0, min(len(sorted_samples) - 1, idx))
            return sorted_samples[idx]

        target_rank = (p / 100.0) * self._count
        accumulated = 0
        for idx, count in enumerate(self._buckets):
            accumulated += count
            if accumulated >= target_rank:
                if idx == 0:
                    return self.min_val_us
                elif idx == self._num_buckets - 1:
                    return self.max_val_us
                else:
                    return self.min_val_us * (self._mult ** (idx - 1))
        return self._max

    def summary(self) -> Dict[str, float]:
        return {
            "count": self.count,
            "min_us": self.min,
            "mean_us": self.mean,
            "p50_us": self.percentile(50.0),
            "p90_us": self.percentile(90.0),
            "p95_us": self.percentile(95.0),
            "p99_us": self.percentile(99.0),
            "p99.9_us": self.percentile(99.9),
            "p99.99_us": self.percentile(99.99),
            "max_us": self.max,
        }

    def ascii_chart(self, width: int = 40) -> str:
        if self._count == 0:
            return "No latency samples recorded."

        summary = self.summary()
        lines = [
            "=" * 60,
            "                  LATENCY HISTOGRAM REPORT                   ",
            "=" * 60,
            f" Total Samples   : {self.count:,}",
            f" Min Latency     : {self.min:10.2f} us ({self.min / 1000:8.3f} ms)",
            f" Mean Latency    : {self.mean:10.2f} us ({self.mean / 1000:8.3f} ms)",
            f" p50 (Median)    : {summary['p50_us']:10.2f} us ({summary['p50_us'] / 1000:8.3f} ms)",
            f" p90             : {summary['p90_us']:10.2f} us ({summary['p90_us'] / 1000:8.3f} ms)",
            f" p95             : {summary['p95_us']:10.2f} us ({summary['p95_us'] / 1000:8.3f} ms)",
            f" p99             : {summary['p99_us']:10.2f} us ({summary['p99_us'] / 1000:8.3f} ms)",
            f" p99.9           : {summary['p99.9_us']:10.2f} us ({summary['p99.9_us'] / 1000:8.3f} ms)",
            f" p99.99          : {summary['p99.99_us']:10.2f} us ({summary['p99.99_us'] / 1000:8.3f} ms)",
            f" Max Latency     : {self.max:10.2f} us ({self.max / 1000:8.3f} ms)",
            "-" * 60,
            " Percentile Distribution:",
        ]

        percentiles = [
            ("p50  ", 50.0),
            ("p75  ", 75.0),
            ("p90  ", 90.0),
            ("p95  ", 95.0),
            ("p99  ", 99.0),
            ("p99.9", 99.9),
            ("Max  ", 100.0),
        ]

        max_p_val = max(summary["max_us"], 1.0)
        for label, p_val in percentiles:
            val_us = self.percentile(p_val) if p_val < 100.0 else self.max
            bar_len = int((val_us / max_p_val) * width) if max_p_val > 0 else 0
            bar = "#" * max(1 if val_us > 0 else 0, bar_len)
            lines.append(f" {label} | {bar:<{width}} | {val_us:10.2f} us")

        lines.append("=" * 60)
        return "\n".join(lines)
