import asyncio
import socket
import time
from typing import Dict, Optional, Set, Tuple
from feedwatch.protocol import (
    MarketDataMessage,
    RetransmitRequest,
    pack_tcp_frame,
    read_tcp_frame,
    MARKET_DATA_SIZE,
)
from feedwatch.metrics import ReceiverMetrics


class UDPDatagramProtocol(asyncio.DatagramProtocol):
    """Asyncio Datagram Protocol to receive UDP datagrams without blocking."""

    def __init__(self, receiver: "FeedReceiver"):
        self.receiver = receiver

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        recv_time_ns = time.perf_counter_ns()
        self.receiver.handle_raw_udp_message(data, recv_time_ns)

    def error_received(self, exc: Exception) -> None:
        pass


class FeedReceiver:
    """
    High-performance Market Data Feed Receiver using asyncio.
    Detects sequence gaps, requests TCP retransmissions, reorders out-of-order packets,
    de-duplicates messages, handles backpressure, and logs latency metrics.
    """

    def __init__(
        self,
        udp_host: str = "127.0.0.1",
        udp_port: int = 9999,
        tcp_host: str = "127.0.0.1",
        tcp_port: int = 9998,
        bounded_queue_size: int = 10_000,
        max_reorder_gap: int = 5_000,
        retransmit_cooldown_sec: float = 0.05,
    ):
        self.udp_host = udp_host
        self.udp_port = udp_port
        self.tcp_host = tcp_host
        self.tcp_port = tcp_port
        self.bounded_queue_size = bounded_queue_size
        self.max_reorder_gap = max_reorder_gap
        self.retransmit_cooldown_sec = retransmit_cooldown_sec

        self.metrics = ReceiverMetrics()

        self.expected_seq: int = 1
        self.reorder_buffer: Dict[int, Tuple[MarketDataMessage, int, bool]] = {}

        self.pending_retransmits: Dict[int, float] = {}

        self.is_running = False
        self._udp_transport: Optional[asyncio.DatagramTransport] = None
        self._tcp_reader: Optional[asyncio.StreamReader] = None
        self._tcp_writer: Optional[asyncio.StreamWriter] = None
        self._tcp_task: Optional[asyncio.Task] = None
        self._processor_task: Optional[asyncio.Task] = None

        self.processing_queue: asyncio.Queue[
            Tuple[MarketDataMessage, int, int, bool]
        ] = asyncio.Queue(maxsize=bounded_queue_size)

        self.on_message_callback = None

    async def _ensure_tcp_connection(self) -> bool:
        if self._tcp_writer and not self._tcp_writer.is_closing():
            return True

        try:
            reader, writer = await asyncio.open_connection(
                self.tcp_host, self.tcp_port
            )
            self._tcp_reader = reader
            self._tcp_writer = writer
            if self._tcp_task is None or self._tcp_task.done():
                self._tcp_task = asyncio.create_task(self._read_tcp_retransmissions())
            return True
        except (ConnectionRefusedError, OSError):
            return False

    async def start(self) -> None:
        if self.is_running:
            return

        self.is_running = True
        self.metrics.start_time = time.perf_counter()

        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: UDPDatagramProtocol(self),
            local_addr=(self.udp_host, self.udp_port),
        )
        self._udp_transport = transport

        sock = transport.get_extra_info("socket")
        if sock:
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2 * 1024 * 1024)
            except Exception:
                pass

        await self._ensure_tcp_connection()

        self._processor_task = asyncio.create_task(self._process_queue())

    async def stop(self) -> None:
        self.is_running = False

        if self._udp_transport:
            self._udp_transport.close()
            self._udp_transport = None

        if self._tcp_task and not self._tcp_task.done():
            self._tcp_task.cancel()
            try:
                await self._tcp_task
            except asyncio.CancelledError:
                pass

        if self._tcp_writer:
            try:
                self._tcp_writer.close()
                await asyncio.wait_for(self._tcp_writer.wait_closed(), timeout=0.5)
            except Exception:
                pass
            self._tcp_writer = None

        if self._processor_task and not self._processor_task.done():
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass

    def handle_raw_udp_message(self, data: bytes, recv_time_ns: int) -> None:
        try:
            msg = MarketDataMessage.unpack(data)
            self._process_incoming_message(msg, len(data), recv_time_ns, is_retransmit=False)
        except Exception:
            pass

    def _process_incoming_message(
        self, msg: MarketDataMessage, raw_bytes_len: int, recv_time_ns: int, is_retransmit: bool
    ) -> None:
        seq = msg.seq

        if seq < self.expected_seq:
            self.metrics.record_duplicate()
            return

        if seq in self.reorder_buffer:
            self.metrics.record_duplicate()
            return

        if seq > self.expected_seq:
            gap_start = self.expected_seq
            gap_end = seq - 1

            self.reorder_buffer[seq] = (msg, raw_bytes_len, is_retransmit)

            self._request_retransmission_range(gap_start, gap_end)

            if len(self.reorder_buffer) > self.max_reorder_gap:
                self._force_drain_reorder_buffer()
            return

        self._enqueue_for_delivery(msg, raw_bytes_len, recv_time_ns, is_retransmit)
        self.expected_seq += 1

        self.pending_retransmits.pop(seq, None)

        while self.expected_seq in self.reorder_buffer:
            buf_msg, buf_bytes_len, buf_is_retransmit = self.reorder_buffer.pop(self.expected_seq)
            self.metrics.out_of_order_delivered += 1
            self._enqueue_for_delivery(buf_msg, buf_bytes_len, recv_time_ns, buf_is_retransmit)
            self.pending_retransmits.pop(self.expected_seq, None)
            self.expected_seq += 1

    def _force_drain_reorder_buffer(self) -> None:
        if not self.reorder_buffer:
            return

        sorted_seqs = sorted(self.reorder_buffer.keys())
        oldest_available_seq = sorted_seqs[0]

        missing_count = oldest_available_seq - self.expected_seq
        self.metrics.record_backpressure_drop()

        self.expected_seq = oldest_available_seq

        recv_time_ns = time.perf_counter_ns()
        while self.expected_seq in self.reorder_buffer:
            buf_msg, buf_bytes_len, buf_is_retransmit = self.reorder_buffer.pop(self.expected_seq)
            self._enqueue_for_delivery(buf_msg, buf_bytes_len, recv_time_ns, buf_is_retransmit)
            self.expected_seq += 1

    def _request_retransmission_range(self, start_seq: int, end_seq: int) -> None:
        now = time.perf_counter()
        missing_seqs = []

        for seq in range(start_seq, end_seq + 1):
            last_req = self.pending_retransmits.get(seq, 0)
            if now - last_req > self.retransmit_cooldown_sec:
                self.pending_retransmits[seq] = now
                missing_seqs.append(seq)

        if not missing_seqs:
            return

        num_gaps = len(missing_seqs)
        self.metrics.record_gap(num_gaps)

        try:
            asyncio.create_task(
                self._async_send_retransmit_request(missing_seqs[0], missing_seqs[-1])
            )
        except RuntimeError:
            pass

    async def _async_send_retransmit_request(self, start_seq: int, end_seq: int) -> None:
        connected = await self._ensure_tcp_connection()
        if connected and self._tcp_writer and not self._tcp_writer.is_closing():
            req = RetransmitRequest(start_seq=start_seq, end_seq=end_seq)
            try:
                frame = pack_tcp_frame(req.pack())
                self._tcp_writer.write(frame)
                await self._tcp_writer.drain()
                self.metrics.record_retransmit_request()
            except Exception:
                pass

    async def _read_tcp_retransmissions(self) -> None:
        while self.is_running and self._tcp_reader:
            try:
                frame = await read_tcp_frame(self._tcp_reader)
                if frame is None:
                    break

                recv_time_ns = time.perf_counter_ns()
                try:
                    msg = MarketDataMessage.unpack(frame)
                    self._process_incoming_message(
                        msg, len(frame), recv_time_ns, is_retransmit=True
                    )
                except Exception:
                    pass
            except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
                break

    async def wait_until_caught_up(self, target_seq: int, timeout: float = 5.0) -> bool:
        deadline = time.perf_counter() + timeout
        while time.perf_counter() < deadline:
            if self.expected_seq is not None and self.expected_seq > target_seq:
                return True
            await asyncio.sleep(0.01)
        return False

    def _enqueue_for_delivery(
        self, msg: MarketDataMessage, raw_bytes_len: int, recv_time_ns: int, is_retransmit: bool
    ) -> None:
        try:
            self.processing_queue.put_nowait((msg, raw_bytes_len, recv_time_ns, is_retransmit))
        except asyncio.QueueFull:
            self.metrics.record_backpressure_drop()

    async def _process_queue(self) -> None:
        while self.is_running:
            try:
                msg, raw_bytes_len, recv_time_ns, is_retransmit = await self.processing_queue.get()
                
                latency_ns = recv_time_ns - msg.timestamp_ns
                latency_us = max(0.0, latency_ns / 1000.0)

                self.metrics.record_message(raw_bytes_len, latency_us, is_retransmit=is_retransmit)

                if self.on_message_callback:
                    try:
                        if asyncio.iscoroutinefunction(self.on_message_callback):
                            await self.on_message_callback(msg)
                        else:
                            self.on_message_callback(msg)
                    except Exception:
                        pass

                self.processing_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                pass
