import pytest
import asyncio
from feedwatch.simulator import FeedSimulator
from feedwatch.protocol import RetransmitRequest, MarketDataMessage, pack_tcp_frame, read_tcp_frame


@pytest.mark.asyncio
async def test_simulator_history_buffer():
    simulator = FeedSimulator(history_size=5)
    await simulator.start()

    try:
        for _ in range(10):
            simulator.generate_and_send_next()

        assert simulator.current_seq == 10
        assert len(simulator.history_buffer) == 5
        assert set(simulator.history_buffer.keys()) == {6, 7, 8, 9, 10}
    finally:
        await simulator.stop()


@pytest.mark.asyncio
async def test_simulator_tcp_retransmission_server():
    simulator = FeedSimulator(udp_port=10001, tcp_port=10002)
    await simulator.start()

    try:
        for _ in range(50):
            simulator.generate_and_send_next()

        reader, writer = await asyncio.open_connection("127.0.0.1", 10002)

        req = RetransmitRequest(start_seq=10, end_seq=15)
        writer.write(pack_tcp_frame(req.pack()))
        await writer.drain()

        received_messages = []
        for _ in range(6):
            frame = await read_tcp_frame(reader)
            assert frame is not None
            msg = MarketDataMessage.unpack(frame)
            received_messages.append(msg.seq)

        assert received_messages == [10, 11, 12, 13, 14, 15]

        writer.close()
        await writer.wait_closed()
    finally:
        await simulator.stop()
