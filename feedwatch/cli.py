import argparse
import asyncio
import sys
import time
from feedwatch.simulator import FeedSimulator
from feedwatch.receiver import FeedReceiver


async def run_benchmark(args: argparse.Namespace) -> None:
    print(f"Starting FeedWatch Benchmark...", flush=True)
    print(f"  Messages        : {args.count:,}", flush=True)
    print(f"  Target Rate     : {'Unlimited' if args.rate <= 0 else f'{args.rate:,} msg/s'}", flush=True)
    print(f"  Loss Rate       : {args.loss * 100:.1f}%", flush=True)
    print(f"  Reorder Rate    : {args.reorder * 100:.1f}%", flush=True)
    print(f"  Duplicate Rate  : {args.duplicate * 100:.1f}%", flush=True)
    print(f"  UDP Port        : {args.udp_port}", flush=True)
    print(f"  TCP Port        : {args.tcp_port}", flush=True)
    print(f"  Bounded Queue   : {args.queue_size}", flush=True)
    print("-" * 60, flush=True)

    simulator = FeedSimulator(
        udp_host=args.host,
        udp_port=args.udp_port,
        tcp_host=args.host,
        tcp_port=args.tcp_port,
        loss_rate=args.loss,
        reorder_rate=args.reorder,
        duplicate_rate=args.duplicate,
    )

    receiver = FeedReceiver(
        udp_host=args.host,
        udp_port=args.udp_port,
        tcp_host=args.host,
        tcp_port=args.tcp_port,
        bounded_queue_size=args.queue_size,
    )

    await simulator.start()
    await receiver.start()

    await asyncio.sleep(0.1)

    start_bench_time = time.perf_counter()

    pub_task = asyncio.create_task(
        simulator.run_publisher(count=args.count, rate_msg_sec=args.rate)
    )

    await pub_task

    await receiver.wait_until_caught_up(simulator.current_seq, timeout=5.0)

    try:
        await asyncio.wait_for(receiver.processing_queue.join(), timeout=5.0)
    except asyncio.TimeoutError:
        pass

    await simulator.stop()
    await receiver.stop()

    bench_duration = time.perf_counter() - start_bench_time

    print("\n" + receiver.metrics.summary_str(), flush=True)
    print("\n" + receiver.metrics.histogram.ascii_chart(), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="FeedWatch: High-performance Market Data Feed Receiver with TCP Retransmission & Latency Histogram"
    )
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Network host binding")
    parser.add_argument("--udp-port", type=int, default=9999, help="UDP Market Data port")
    parser.add_argument("--tcp-port", type=int, default=9998, help="TCP Retransmission port")
    parser.add_argument("--count", type=int, default=10000, help="Total messages to publish")
    parser.add_argument("--rate", type=float, default=0.0, help="Target publish rate (0 = max burst speed)")
    parser.add_argument("--loss", type=float, default=0.0, help="UDP packet loss probability (0.0 - 1.0)")
    parser.add_argument("--reorder", type=float, default=0.0, help="UDP packet reorder probability (0.0 - 1.0)")
    parser.add_argument("--duplicate", type=float, default=0.0, help="UDP packet duplication probability (0.0 - 1.0)")
    parser.add_argument("--queue-size", type=int, default=10000, help="Receiver bounded queue size")

    args = parser.parse_args()

    try:
        asyncio.run(run_benchmark(args))
    except KeyboardInterrupt:
        print("\nBenchmark cancelled by user.", flush=True)
        sys.exit(0)


if __name__ == "__main__":
    main()
