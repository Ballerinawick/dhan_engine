import struct

HEADER_SIZE = 12
ROW_SIZE = 16
BID_CODE = 41
ASK_CODE = 51

class DepthDecoder:
    def __init__(self):
        self.buffer = b""
        self.bids = []
        self.asks = []

    def feed(self, data: bytes):
        self.buffer += data
        self._process()

    def _process(self):
        while len(self.buffer) >= HEADER_SIZE:
            msg_len = struct.unpack_from("<H", self.buffer, 0)[0]

            if len(self.buffer) < msg_len:
                return  # wait for full frame

            frame = self.buffer[:msg_len]
            self.buffer = self.buffer[msg_len:]

            self._decode_frame(frame)

    def _decode_frame(self, frame: bytes):
        packet_type = frame[HEADER_SIZE]
        offset = HEADER_SIZE + 1
        rows = []

        while offset + ROW_SIZE <= len(frame):
            price, qty, orders = struct.unpack_from("<dII", frame, offset)

            if price > 0 and qty > 0:
                rows.append((price, qty, orders))

            offset += ROW_SIZE

        if packet_type == BID_CODE:
            self.bids = rows
        elif packet_type == ASK_CODE:
            self.asks = rows

    def ready(self):
        return bool(self.bids and self.asks)
