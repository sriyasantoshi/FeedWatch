# FeedWatch 

**FeedWatch** is a high-performance market-data feed simulator and receiver implemented in Python using `asyncio` and the `struct` module. It simulates a low-latency sequenced UDP market-data feed subject to configurable network failures (packet loss, packet reordering, and packet duplication) with sequence gap detection, automatic recovery over a TCP side-channel, deduplication, reordering, explicit backpressure handling, and microsecond-level latency histogram profiling.

---

## Architecture Overview

```
                          UDP Feed (Sequenced Binary Messages)
  +------------------+ -------------------------------------------> +------------------+
  |                  | (Subject to Loss, Reorder, Duplication)      |                  |
  |  Feed Simulator  |                                              |  Feed Receiver   |
  |  (Publisher)     | <------------------------------------------- |  (Ingestion)     |
  |                  |    TCP Retransmission Request (Side Channel)  |                  |
  +------------------+ ===========================================> +------------------+
          |                                                                   |
   History Buffer                                                      Reorder Buffer &
   (Ring Buffer)                                                       Latency Histogram
```

---

## Binary Protocol Specification (`struct`)

All messages are packed using network big-endian byte order (`!`).

### 1. Market Data Message (UDP & TCP Retransmission)
Header & Payload Struct: `!2s B Q Q 8s d I` (39 Bytes)

| Field | Type | Struct Code | Size (Bytes) | Description |
|---|---|---|---|---|
| `magic` | bytes | `2s` | 2 | Protocol identifier (`b"FW"`) |
| `msg_type` | uint8 | `B` | 1 | Message Type (`1` = Market Data) |
| `seq` | uint64 | `Q` | 8 | Monotonically increasing sequence number |
| `timestamp_ns` | uint64 | `Q` | 8 | Publication timestamp (`time.perf_counter_ns()`) |
| `symbol` | bytes | `8s` | 8 | Ticker symbol padded with spaces (e.g. `b"AAPL    "`) |
| `price` | double | `d` | 8 | Asset price (e.g. `150.25`) |
| `quantity` | uint32 | `I` | 4 | Trade or quote volume |

### 2. Retransmission Request (TCP Side Channel)
Struct: `!2s B Q Q` (19 Bytes)

| Field | Type | Struct Code | Size (Bytes) | Description |
|---|---|---|---|---|
| `magic` | bytes | `2s` | 2 | Protocol identifier (`b"FW"`) |
| `msg_type` | uint8 | `B` | 1 | Message Type (`2` = Retransmit Request) |
| `start_seq` | uint64 | `Q` | 8 | First missing sequence number in gap |
| `end_seq` | uint64 | `Q` | 8 | Last missing sequence number in gap |

### 3. TCP Framing Header
Struct: `!H` (2 Bytes)
- Prefixes all messages sent over the TCP retransmission side-channel with a 16-bit payload length prefix.

---

## Gap Detection, Reordering & Recovery Workflow

1. **UDP Ingestion**: Receiver listens synchronously via `asyncio.DatagramProtocol` with an enlarged OS socket receive buffer (`SO_RCVBUF = 2MB`).
2. **Gap Detection**: When message `seq` arrives:
   - If `seq == expected_seq`: Processed immediately, `expected_seq` advances by 1.
   - If `seq < expected_seq`: Identified as a duplicate; dropped immediately and logged in duplicate metrics.
   - If `seq > expected_seq`: Gap detected (`[expected_seq, seq - 1]`). The message is held in `reorder_buffer` and a range retransmission request `RetransmitRequest(start_seq, end_seq)` is dispatched over the TCP side-channel.
3. **TCP Retransmission & Recovery**: The simulator maintains a ring buffer of published binary payloads. Upon receiving a `RetransmitRequest`, missing payloads are stream-framed and sent back over TCP.
4. **Contiguous Draining**: As retransmitted or out-of-order packets arrive, contiguous sequence numbers are popped from `reorder_buffer` and delivered sequentially to the application processing pipeline.

---

## Explicit Backpressure Handling

To guarantee stability under extreme network degradation or slow application consumers:

1. **Bounded Processing Queue**: `asyncio.Queue(maxsize=10000)` enforces application layer backpressure. If processing falls behind, `put_nowait` triggers `backpressure_drops` metric tracking rather than unbounded memory growth.
2. **Reorder Buffer Cap (`max_reorder_gap`)**: If a sequence gap is never filled (e.g., due to simulator history truncation), `reorder_buffer` drops the missing gap once buffer size exceeds `max_reorder_gap` (default 5,000), resetting `expected_seq` to prevent memory exhaustion.
3. **TCP StreamWriter Drain**: The TCP retransmission server calls `await writer.drain()`, applying socket flow control if receiver TCP buffers fill up.

---

## Installation & Running

### Requirements
- Python 3.10+
- `pytest` and `pytest-asyncio` (for running unit tests)

### Quick Run Benchmark CLI
```bash
# Run 10,000 messages with 0% failure
python -m feedwatch.cli --count 10000

# Run 10,000 messages with 5% loss, 5% reorder, 5% duplication
python -m feedwatch.cli --count 10000 --loss 0.05 --reorder 0.05 --duplicate 0.05
```

### Running Tests
```bash
python -m pytest tests -v
```

---

## Measurement Method & Benchmark Results

### Latency Measurement Method
- **Timestamping**: Each message header includes `timestamp_ns` recorded by the publisher using `time.perf_counter_ns()`.
- **Receive Time**: As datagrams hit `datagram_received()`, the receiver captures $t_{recv} = \text{time.perf_counter\_ns()}$.
- **Processing Latency**: Calculated as $(t_{recv} - t_{send}) / 1000.0$ in microseconds ($\mu s$).
- **Histogram**: Recorded into a logarithmic bucketed histogram (`LatencyHistogram`) supporting precision quantile extraction (p50, p90, p95, p99, p99.9, p99.99).

### Benchmark Results

#### Scenario A: Clean Network Feed (2,000 Messages, 0% Failures)
```
============================================================
                   FEEDWATCH RECEIVER METRICS                
============================================================
 Elapsed Time              : 0.66 seconds
 Total Messages Processed  : 2,000
 Total Bytes Received      : 78,000 bytes
 Throughput (msgs/sec)     : 3,046.01 msg/s
 Throughput (Mbps)         : 0.95 Mbps
 Sequence Gaps Detected    : 0
 Retransmit Requests Sent  : 0
 Retransmitted Msgs Recv   : 0
 Duplicate Msgs Dropped    : 0
 Out-of-Order Delivered    : 0
 Backpressure Drops        : 0
============================================================

============================================================
                  LATENCY HISTOGRAM REPORT                   
============================================================
 Total Samples   : 2,000
 Min Latency     :     757.70 us (   0.758 ms)
 Mean Latency    :   28041.70 us (  28.042 ms)
 p50 (Median)    :   29325.40 us (  29.325 ms)
 p90             :   32148.80 us (  32.149 ms)
 p95             :   32743.40 us (  32.743 ms)
 p99             :   33294.60 us (  33.295 ms)
 p99.9           :   33427.90 us (  33.428 ms)
 p99.99          :   33445.10 us (  33.445 ms)
 Max Latency     :   33445.10 us (  33.445 ms)
============================================================
```

#### Scenario B: Lossy & Reordered Feed (2,000 Messages, 5% Loss, 5% Reorder, 5% Duplicate)
```
============================================================
                   FEEDWATCH RECEIVER METRICS                
============================================================
 Elapsed Time              : 0.76 seconds
 Total Messages Processed  : 2,000
 Total Bytes Received      : 78,000 bytes
 Throughput (msgs/sec)     : 2,614.48 msg/s
 Throughput (Mbps)         : 0.82 Mbps
 Sequence Gaps Detected    : 653
 Retransmit Requests Sent  : 603
 Retransmitted Msgs Recv   : 102
 Duplicate Msgs Dropped    : 630
 Out-of-Order Delivered    : 620
 Backpressure Drops        : 0
============================================================

============================================================
                  LATENCY HISTOGRAM REPORT                   
============================================================
 Total Samples   : 2,000
 Min Latency     :     728.10 us (   0.728 ms)
 Mean Latency    :   54632.64 us (  54.633 ms)
 p50 (Median)    :   55293.90 us (  55.294 ms)
 p90             :   82176.70 us (  82.177 ms)
 p95             :   85164.10 us (  85.164 ms)
 p99             :   86859.80 us (  86.860 ms)
 p99.9           :   87711.00 us (  87.711 ms)
 p99.99          :   87735.20 us (  87.735 ms)
 Max Latency     :   87735.20 us (  87.735 ms)
============================================================
```

---

## Limits of Measuring Latency in Python

When profiling microsecond-level market-data feed latency in Pure Python, several inherent language runtime and operating system boundaries must be considered:

1. **Global Interpreter Lock (GIL) & Context Switching**:
   - Python's GIL prevents multi-threaded execution of CPU-bound byte parsing or queue processing on multiple CPU cores simultaneously. Event loop scheduling overhead occurs whenever coroutines yield control (`await asyncio.sleep(0)`).
2. **Garbage Collection (GC) Pauses**:
   - Python's stop-the-world generational garbage collector introduces un-predictable latency spikes (tail latency at p99/p99.9/p99.99). Object allocation during packet unpacking (`MarketDataMessage.unpack`) creates allocation churn that triggers GC runs.
3. **OS Network Stack Buffering**:
   - Operating system kernel UDP socket buffers (`SO_RCVBUF`) and TCP windowing introduce variable queuing delays before datagrams reach the Python process user-space boundary.
4. **Timer Precision & Clock Resolution**:
   - While `time.perf_counter_ns()` uses high-resolution hardware timers (e.g. `QueryPerformanceCounter` on Windows or `clock_gettime(CLOCK_MONOTONIC)` on Linux), calling timer functions in Python incurs ~100–300 nanoseconds of function invocation overhead per packet.
5. **Asyncio Event Loop Cooperative Scheduling Jitter**:
   - In asyncio, task execution is single-threaded and cooperative. If an I/O task or queue callback takes 200 microseconds, all subsequent network read callbacks are delayed by at least that duration, inflating measured latency compared to multi-threaded C/C++/Rust kernel-bypass architectures (e.g., Solarflare EF_VI / DPDK).
