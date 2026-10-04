import pytest
import asyncio
from feedwatch.simulator import FeedSimulator
from feedwatch.receiver import FeedReceiver
from feedwatch.protocol import MarketDataMessage


@pytest.mark.asyncio
async def test_queue_backpressure_drop():
    udp_port = 10501
    tcp_port = 10502

    queue_size = 5
    receiver = FeedReceiver(
        udp_host="127.0.0.1",
        udp_port=udp_port,
        tcp_port=tcp_port,
        bounded_queue_size=queue_size,
    )

    msg = MarketDataMessage(1, 100, "AAPL", 150.0, 100)

    for i in range(queue_size):
        receiver._enqueue_for_delivery(msg, 39, 1000, False)

    assert receiver.processing_queue.full()
    assert receiver.metrics.backpressure_drops == 0

    receiver._enqueue_for_delivery(msg, 39, 1000, False)

    assert receiver.metrics.backpressure_drops == 1


@pytest.mark.asyncio
async def test_reorder_buffer_max_gap_drain():
    udp_port = 10601
    tcp_port = 10602

    max_gap = 5
    receiver = FeedReceiver(
        udp_host="127.0.0.1",
        udp_port=udp_port,
        tcp_port=tcp_port,
        max_reorder_gap=max_gap,
    )

    receiver.expected_seq = 1

    for seq in range(10, 17):
        msg = MarketDataMessage(seq, 100, "AAPL", 150.0, 100)
        receiver._process_incoming_message(msg, 39, 1000, is_retransmit=False)

    assert receiver.metrics.backpressure_drops > 0
    assert receiver.expected_seq > 10
