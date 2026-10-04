import struct
from dataclasses import dataclass
import asyncio
from typing import Optional, Tuple

MAGIC = b"FW"

MSG_TYPE_MARKET_DATA = 1
MSG_TYPE_RETRANSMIT_REQ = 2

# Struct formats (Big-Endian Network Byte Order)
# Market Data: Magic (2s), MsgType (B), Seq (Q), Timestamp (Q), Symbol (8s), Price (d), Quantity (I)
MARKET_DATA_FMT = "!2s B Q Q 8s d I"
MARKET_DATA_SIZE = struct.calcsize(MARKET_DATA_FMT)

# Retransmit Request: Magic (2s), MsgType (B), StartSeq (Q), EndSeq (Q)
RETRANSMIT_REQ_FMT = "!2s B Q Q"
RETRANSMIT_REQ_SIZE = struct.calcsize(RETRANSMIT_REQ_FMT)

# TCP Frame Header: Payload Length (H - uint16)
TCP_FRAME_HEADER_FMT = "!H"
TCP_FRAME_HEADER_SIZE = struct.calcsize(TCP_FRAME_HEADER_FMT)


@dataclass(slots=True, frozen=True)
class MarketDataMessage:
    seq: int
    timestamp_ns: int
    symbol: str
    price: float
    quantity: int

    def pack(self) -> bytes:
        symbol_bytes = self.symbol.ljust(8)[:8].encode("ascii")
        return struct.pack(
            MARKET_DATA_FMT,
            MAGIC,
            MSG_TYPE_MARKET_DATA,
            self.seq,
            self.timestamp_ns,
            symbol_bytes,
            self.price,
            self.quantity,
        )

    @classmethod
    def unpack(cls, data: bytes) -> "MarketDataMessage":
        if len(data) < MARKET_DATA_SIZE:
            raise ValueError(
                f"Data buffer too short for MarketDataMessage: expected {MARKET_DATA_SIZE} bytes, got {len(data)}"
            )

        magic, msg_type, seq, timestamp_ns, symbol_bytes, price, quantity = (
            struct.unpack(MARKET_DATA_FMT, data[:MARKET_DATA_SIZE])
        )

        if magic != MAGIC:
            raise ValueError(f"Invalid magic bytes: {magic!r}")
        if msg_type != MSG_TYPE_MARKET_DATA:
            raise ValueError(f"Unexpected message type: {msg_type}")

        symbol = symbol_bytes.decode("ascii").rstrip()
        return cls(
            seq=seq,
            timestamp_ns=timestamp_ns,
            symbol=symbol,
            price=price,
            quantity=quantity,
        )


@dataclass(slots=True, frozen=True)
class RetransmitRequest:
    start_seq: int
    end_seq: int

    def pack(self) -> bytes:
        return struct.pack(
            RETRANSMIT_REQ_FMT,
            MAGIC,
            MSG_TYPE_RETRANSMIT_REQ,
            self.start_seq,
            self.end_seq,
        )

    @classmethod
    def unpack(cls, data: bytes) -> "RetransmitRequest":
        if len(data) < RETRANSMIT_REQ_SIZE:
            raise ValueError(
                f"Data buffer too short for RetransmitRequest: expected {RETRANSMIT_REQ_SIZE} bytes, got {len(data)}"
            )

        magic, msg_type, start_seq, end_seq = struct.unpack(
            RETRANSMIT_REQ_FMT, data[:RETRANSMIT_REQ_SIZE]
        )

        if magic != MAGIC:
            raise ValueError(f"Invalid magic bytes: {magic!r}")
        if msg_type != MSG_TYPE_RETRANSMIT_REQ:
            raise ValueError(f"Unexpected message type: {msg_type}")

        return cls(start_seq=start_seq, end_seq=end_seq)


def pack_tcp_frame(payload: bytes) -> bytes:
    """Prefix payload with 2-byte uint16 length."""
    if len(payload) > 65535:
        raise ValueError("Payload size exceeds maximum TCP frame limit (65535 bytes)")
    return struct.pack(TCP_FRAME_HEADER_FMT, len(payload)) + payload


async def read_tcp_frame(reader: asyncio.StreamReader) -> Optional[bytes]:
    """Reads a length-prefixed TCP frame from the StreamReader. Returns None on EOF."""
    try:
        header = await reader.readexactly(TCP_FRAME_HEADER_SIZE)
    except asyncio.IncompleteReadError:
        return None

    (length,) = struct.unpack(TCP_FRAME_HEADER_FMT, header)
    if length == 0:
        return b""

    try:
        payload = await reader.readexactly(length)
        return payload
    except asyncio.IncompleteReadError:
        return None
