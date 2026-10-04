import pytest
import asyncio
from feedwatch.simulator import FeedSimulator
from feedwatch.receiver import FeedReceiver


@pytest.mark.asyncio
async def test_packet_loss_retransmission():
    """Test Failure Mode 1: Packet Loss & TCP Retransmission Recovery."""
    udp_port = 10101
    tcp_port = 10102
    total_msgs = 200

    simulator = FeedSimulator(
        udp_port=udp_port,
        tcp_port=tcp_port,
        loss_rate=0.15,
        reorder_rate=0.0,
        duplicate_rate=0.0,
    )

    receiver = FeedReceiver(udp_port=udp_port, tcp_port=tcp_port)

    received_seqs = []

    def on_msg(msg):
        received_seqs.append(msg.seq)

    receiver.on_message_callback = on_msg

    await simulator.start()
    await receiver.start()

    try:
        await asyncio.sleep(0.05)
        await simulator.run_publisher(count=total_msgs, rate_msg_sec=0)

        caught_up = await receiver.wait_until_caught_up(total_msgs, timeout=5.0)
        assert caught_up is True

        await asyncio.wait_for(receiver.processing_queue.join(), timeout=2.0)

        assert len(received_seqs) == total_msgs
        assert received_seqs == list(range(1, total_msgs + 1))

        assert receiver.metrics.sequence_gaps_detected > 0
        assert receiver.metrics.retransmit_requests_sent > 0
        assert receiver.metrics.retransmitted_msgs_received > 0
    finally:
        await simulator.stop()
        await receiver.stop()


@pytest.mark.asyncio
async def test_packet_reordering():
    """Test Failure Mode 2: Packet Reordering & Sequential Buffer Delivery."""
    udp_port = 10201
    tcp_port = 10202
    total_msgs = 200

    simulator = FeedSimulator(
        udp_port=udp_port,
        tcp_port=tcp_port,
        loss_rate=0.0,
        reorder_rate=0.20,
        duplicate_rate=0.0,
    )

    receiver = FeedReceiver(udp_port=udp_port, tcp_port=tcp_port)

    received_seqs = []

    def on_msg(msg):
        received_seqs.append(msg.seq)

    receiver.on_message_callback = on_msg

    await simulator.start()
    await receiver.start()

    try:
        await asyncio.sleep(0.05)
        await simulator.run_publisher(count=total_msgs, rate_msg_sec=0)

        caught_up = await receiver.wait_until_caught_up(total_msgs, timeout=5.0)
        assert caught_up is True

        await asyncio.wait_for(receiver.processing_queue.join(), timeout=2.0)

        assert len(received_seqs) == total_msgs
        assert received_seqs == list(range(1, total_msgs + 1))

        assert receiver.metrics.out_of_order_delivered > 0
    finally:
        await simulator.stop()
        await receiver.stop()


@pytest.mark.asyncio
async def test_packet_duplication():
    """Test Failure Mode 3: Packet Duplication & De-duplication."""
    udp_port = 10301
    tcp_port = 10302
    total_msgs = 200

    simulator = FeedSimulator(
        udp_port=udp_port,
        tcp_port=tcp_port,
        loss_rate=0.0,
        reorder_rate=0.0,
        duplicate_rate=0.25,
    )

    receiver = FeedReceiver(udp_port=udp_port, tcp_port=tcp_port)

    received_seqs = []

    def on_msg(msg):
        received_seqs.append(msg.seq)

    receiver.on_message_callback = on_msg

    await simulator.start()
    await receiver.start()

    try:
        await asyncio.sleep(0.05)
        await simulator.run_publisher(count=total_msgs, rate_msg_sec=0)

        caught_up = await receiver.wait_until_caught_up(total_msgs, timeout=5.0)
        assert caught_up is True

        await asyncio.wait_for(receiver.processing_queue.join(), timeout=2.0)

        assert len(received_seqs) == total_msgs
        assert received_seqs == list(range(1, total_msgs + 1))

        assert receiver.metrics.duplicate_count > 0
    finally:
        await simulator.stop()
        await receiver.stop()


@pytest.mark.asyncio
async def test_combined_failure_modes():
    """Test Combined Failure Modes: Loss, Reordering, and Duplication concurrently."""
    udp_port = 10401
    tcp_port = 10402
    total_msgs = 300

    simulator = FeedSimulator(
        udp_port=udp_port,
        tcp_port=tcp_port,
        loss_rate=0.10,
        reorder_rate=0.10,
        duplicate_rate=0.10,
    )

    receiver = FeedReceiver(udp_port=udp_port, tcp_port=tcp_port)

    received_seqs = []

    def on_msg(msg):
        received_seqs.append(msg.seq)

    receiver.on_message_callback = on_msg

    await simulator.start()
    await receiver.start()

    try:
        await asyncio.sleep(0.05)
        await simulator.run_publisher(count=total_msgs, rate_msg_sec=0)

        caught_up = await receiver.wait_until_caught_up(total_msgs, timeout=5.0)
        assert caught_up is True

        await asyncio.wait_for(receiver.processing_queue.join(), timeout=2.0)

        assert len(received_seqs) == total_msgs
        assert received_seqs == list(range(1, total_msgs + 1))

        assert receiver.metrics.sequence_gaps_detected > 0
        assert receiver.metrics.duplicate_count > 0
        assert receiver.metrics.out_of_order_delivered > 0
    finally:
        await simulator.stop()
        await receiver.stop()
