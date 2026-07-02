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
