import asyncio
import random
import socket
import struct
import time
from collections import deque
from typing import Dict, Optional, Tuple, Deque
from feedwatch.protocol import (
    MarketDataMessage,
    RetransmitRequest,
    pack_tcp_frame,
    read_tcp_frame,
    MARKET_DATA_SIZE,
)


class FeedSimulator:
    """
    Simulates a high-throughput UDP Market Data publisher with configurable failure modes
    (loss, reordering, duplication) and a TCP Retransmission Side-Channel server.
    """

    def __init__(
        self,
        udp_host: str = "127.0.0.1",
        udp_port: int = 9999,
        tcp_host: str = "127.0.0.1",
        tcp_port: int = 9998,
        loss_rate: float = 0.0,
        reorder_rate: float = 0.0,
        duplicate_rate: float = 0.0,
        reorder_delay_msgs: int = 3,
        history_size: int = 100_000,
    ):
        self.udp_host = udp_host
        self.udp_port = udp_port
        self.tcp_host = tcp_host
        self.tcp_port = tcp_port

        self.loss_rate = max(0.0, min(1.0, loss_rate))
        self.reorder_rate = max(0.0, min(1.0, reorder_rate))
        self.duplicate_rate = max(0.0, min(1.0, duplicate_rate))
        self.reorder_delay_msgs = max(1, reorder_delay_msgs)

        self.history_size = history_size
        self.history_buffer: Dict[int, bytes] = {}
        self.history_order: Deque[int] = deque()

        self.current_seq = 0
        self.is_running = False
        self._publish_task: Optional[asyncio.Task] = None
        self._tcp_server: Optional[asyncio.Server] = None
        self._udp_socket: Optional[socket.socket] = None

        self._reorder_queue: Deque[Tuple[int, bytes]] = deque()

        self.stats_published = 0
        self.stats_udp_sent = 0
        self.stats_dropped = 0
        self.stats_reordered = 0
        self.stats_duplicated = 0
        self.stats_retransmitted = 0

    def _record_history(self, seq: int, msg_bytes: bytes) -> None:
        self.history_buffer[seq] = msg_bytes
        self.history_order.append(seq)
        while len(self.history_order) > self.history_size:
            oldest_seq = self.history_order.popleft()
            self.history_buffer.pop(oldest_seq, None)

    async def start(self) -> None:
        if self.is_running:
            return

        self.is_running = True

        self._udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_socket.setblocking(False)

        self._tcp_server = await asyncio.start_server(
            self._handle_tcp_retransmit_client,
            host=self.tcp_host,
            port=self.tcp_port,
        )

    async def stop(self) -> None:
        self.is_running = False

        if self._publish_task and not self._publish_task.done():
            self._publish_task.cancel()
            try:
                await self._publish_task
            except asyncio.CancelledError:
                pass

        if self._tcp_server:
            self._tcp_server.close()
            try:
                await asyncio.wait_for(self._tcp_server.wait_closed(), timeout=0.5)
            except Exception:
                pass
            self._tcp_server = None

        if self._udp_socket:
            self._udp_socket.close()
            self._udp_socket = None

    async def _handle_tcp_retransmit_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            while self.is_running:
                frame = await read_tcp_frame(reader)
                if frame is None:
                    break

                try:
                    req = RetransmitRequest.unpack(frame)
                except Exception:
                    continue

                for seq in range(req.start_seq, req.end_seq + 1):
                    msg_bytes = self.history_buffer.get(seq)
                    if msg_bytes:
                        tcp_frame = pack_tcp_frame(msg_bytes)
                        writer.write(tcp_frame)
                        self.stats_retransmitted += 1

                await writer.drain()

        except (asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    def _send_udp_packet(self, msg_bytes: bytes) -> None:
        if self._udp_socket:
            try:
                self._udp_socket.sendto(msg_bytes, (self.udp_host, self.udp_port))
                self.stats_udp_sent += 1
            except BlockingIOError:
                pass

    def generate_and_send_next(
        self, symbol: str = "AAPL", price: float = 150.25, quantity: int = 100
    ) -> MarketDataMessage:
        self.current_seq += 1
        seq = self.current_seq
        timestamp_ns = time.perf_counter_ns()

        msg = MarketDataMessage(
            seq=seq,
            timestamp_ns=timestamp_ns,
            symbol=symbol,
            price=price,
            quantity=quantity,
        )
        msg_bytes = msg.pack()

        self._record_history(seq, msg_bytes)
        self.stats_published += 1

        while self._reorder_queue and self._reorder_queue[0][0] <= seq:
            _, pending_bytes = self._reorder_queue.popleft()
            self._send_udp_packet(pending_bytes)

        rnd = random.random()

        if rnd < self.loss_rate:
            self.stats_dropped += 1
            return msg

        if rnd < (self.loss_rate + self.reorder_rate):
            deliver_at = seq + self.reorder_delay_msgs
            self._reorder_queue.append((deliver_at, msg_bytes))
            self.stats_reordered += 1
            return msg

        self._send_udp_packet(msg_bytes)

        if random.random() < self.duplicate_rate:
            self._send_udp_packet(msg_bytes)
            self.stats_duplicated += 1

        return msg

    def flush_reorder_queue(self) -> None:
        while self._reorder_queue:
            _, msg_bytes = self._reorder_queue.popleft()
            self._send_udp_packet(msg_bytes)

    async def run_publisher(
        self, count: int, rate_msg_sec: float = 0.0, symbol: str = "AAPL"
    ) -> None:
        delay = (1.0 / rate_msg_sec) if rate_msg_sec > 0 else 0.0

        for _ in range(count):
            if not self.is_running:
                break

            self.generate_and_send_next(symbol=symbol)

            if delay > 0:
                await asyncio.sleep(delay)
            else:
                if self.current_seq % 10 == 0:
                    await asyncio.sleep(0)

        self.flush_reorder_queue()
