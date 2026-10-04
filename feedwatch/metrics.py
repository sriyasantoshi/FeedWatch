import time
from dataclasses import dataclass, field
from feedwatch.histogram import LatencyHistogram


@dataclass
class ReceiverMetrics:
    total_received: int = 0
    total_bytes: int = 0
    duplicate_count: int = 0
    retransmit_requests_sent: int = 0
    retransmitted_msgs_received: int = 0
    sequence_gaps_detected: int = 0
    out_of_order_delivered: int = 0
    backpressure_drops: int = 0
    start_time: float = field(default_factory=time.perf_counter)
    last_reported_time: float = field(default_factory=time.perf_counter)

    histogram: LatencyHistogram = field(default_factory=LatencyHistogram)

    def record_message(self, byte_size: int, latency_us: float, is_retransmit: bool = False) -> None:
        self.total_received += 1
        self.total_bytes += byte_size
        if is_retransmit:
            self.retransmitted_msgs_received += 1
        self.histogram.record(latency_us)

    def record_duplicate(self) -> None:
        self.duplicate_count += 1

    def record_gap(self, num_missing: int) -> None:
        self.sequence_gaps_detected += num_missing

    def record_retransmit_request(self) -> None:
        self.retransmit_requests_sent += 1

    def record_backpressure_drop(self) -> None:
        self.backpressure_drops += 1

    @property
    def elapsed_seconds(self) -> float:
        return max(0.000001, time.perf_counter() - self.start_time)

    @property
    def throughput_msgs_per_sec(self) -> float:
        return self.total_received / self.elapsed_seconds

    @property
    def throughput_mbps(self) -> float:
        return (self.total_bytes * 8 / 1_000_000) / self.elapsed_seconds

    def summary_str(self) -> str:
        lines = [
            "=" * 60,
            "                   FEEDWATCH RECEIVER METRICS                ",
            "=" * 60,
            f" Elapsed Time              : {self.elapsed_seconds:.2f} seconds",
            f" Total Messages Processed  : {self.total_received:,}",
            f" Total Bytes Received      : {self.total_bytes:,} bytes",
            f" Throughput (msgs/sec)     : {self.throughput_msgs_per_sec:,.2f} msg/s",
            f" Throughput (Mbps)         : {self.throughput_mbps:.2f} Mbps",
            f" Sequence Gaps Detected    : {self.sequence_gaps_detected:,}",
            f" Retransmit Requests Sent  : {self.retransmit_requests_sent:,}",
            f" Retransmitted Msgs Recv   : {self.retransmitted_msgs_received:,}",
            f" Duplicate Msgs Dropped    : {self.duplicate_count:,}",
            f" Out-of-Order Delivered    : {self.out_of_order_delivered:,}",
            f" Backpressure Drops        : {self.backpressure_drops:,}",
            "=" * 60,
        ]
        return "\n".join(lines)
