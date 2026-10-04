import pytest
import struct
from feedwatch.protocol import (
    MarketDataMessage,
    RetransmitRequest,
    pack_tcp_frame,
    read_tcp_frame,
    MAGIC,
    MSG_TYPE_MARKET_DATA,
    MSG_TYPE_RETRANSMIT_REQ,
    MARKET_DATA_SIZE,
    RETRANSMIT_REQ_SIZE,
)
import asyncio


def test_market_data_message_pack_unpack():
    msg = MarketDataMessage(
        seq=1050,
        timestamp_ns=1234567890,
        symbol="AAPL",
        price=175.50,
        quantity=500,
    )
    packed = msg.pack()

    assert len(packed) == MARKET_DATA_SIZE
    assert packed[:2] == MAGIC
    assert packed[2] == MSG_TYPE_MARKET_DATA

    unpacked = MarketDataMessage.unpack(packed)
    assert unpacked.seq == 1050
    assert unpacked.timestamp_ns == 1234567890
    assert unpacked.symbol == "AAPL"
    assert abs(unpacked.price - 175.50) < 1e-6
    assert unpacked.quantity == 500


def test_market_data_symbol_padding():
    msg1 = MarketDataMessage(1, 100, "GOOGL_EXTRA", 100.0, 10)
    unpacked1 = MarketDataMessage.unpack(msg1.pack())
    assert unpacked1.symbol == "GOOGL_EX"

    msg2 = MarketDataMessage(2, 100, "MSFT", 200.0, 20)
    unpacked2 = MarketDataMessage.unpack(msg2.pack())
    assert unpacked2.symbol == "MSFT"


def test_market_data_invalid_unpack():
    with pytest.raises(ValueError, match="too short"):
        MarketDataMessage.unpack(b"FW\x01")

    bad_magic = struct.pack("!2s B Q Q 8s d I", b"XX", 1, 1, 100, b"AAPL    ", 10.0, 1)
    with pytest.raises(ValueError, match="Invalid magic"):
        MarketDataMessage.unpack(bad_magic)

    bad_type = struct.pack("!2s B Q Q 8s d I", b"FW", 99, 1, 100, b"AAPL    ", 10.0, 1)
    with pytest.raises(ValueError, match="Unexpected message type"):
        MarketDataMessage.unpack(bad_type)


def test_retransmit_request_pack_unpack():
    req = RetransmitRequest(start_seq=10, end_seq=25)
    packed = req.pack()

    assert len(packed) == RETRANSMIT_REQ_SIZE
    assert packed[:2] == MAGIC
    assert packed[2] == MSG_TYPE_RETRANSMIT_REQ

    unpacked = RetransmitRequest.unpack(packed)
    assert unpacked.start_seq == 10
    assert unpacked.end_seq == 25


@pytest.mark.asyncio
async def test_tcp_framing():
    payload = b"Hello FeedWatch Binary Frame"
    framed = pack_tcp_frame(payload)

    assert len(framed) == 2 + len(payload)

    reader = asyncio.StreamReader()
    reader.feed_data(framed)
    reader.feed_eof()

    read_payload = await read_tcp_frame(reader)
    assert read_payload == payload
