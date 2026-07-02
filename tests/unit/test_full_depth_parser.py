import asyncio
import struct

from dhan_engine.infrastructure.dhan.full_depth import FullDepth


def test_parses_200_depth_row_count_from_header():
    rows = b"".join(struct.pack("<dII", 100.0 + i, i + 1, i + 2) for i in range(200))
    packet_len = 12 + len(rows)
    packet = struct.pack("<HBBiI", packet_len, 41, 2, 123, 200) + rows
    result = FullDepth._parse_packet(packet, expected_levels=200)
    assert result["level_count"] == 200
    assert result["levels"][-1]["price"] == 299.0


def test_rejects_invalid_200_depth_row_count():
    rows = struct.pack("<dII", 100.0, 1, 1)
    packet = struct.pack("<HBBiI", 28, 41, 2, 123, 200) + rows
    assert FullDepth._parse_packet(packet, expected_levels=200) is None


def test_depth_queue_coalesces_updates_by_security_id():
    feed = FullDepth("client", "token")
    feed._queue = asyncio.Queue(maxsize=4)

    feed._enqueue_latest({"security_id": 101, "levels": [1]})
    feed._enqueue_latest({"security_id": 101, "levels": [2]})
    feed._enqueue_latest({"security_id": 202, "levels": [3]})

    assert feed._queue.qsize() == 2
    assert feed._latest_payload_by_key[("security", 101)]["levels"] == [2]
    assert feed._dropped_payload_count == 1
