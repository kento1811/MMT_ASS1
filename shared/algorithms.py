def checksum(data: bytes) -> int:
    """Tính 1's complement checksum 16-bit cho Header + Payload."""
    if len(data) % 2:
        data += b"\x00"

    total = sum(
        int.from_bytes(data[i:i + 2], "big")
        for i in range(0, len(data), 2)
    )

    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)

    return (~total) & 0xFFFF


# --- HỖ TRỢ XỬ LÝ WRAP-AROUND 32-BIT (SEQUENCE NUMBER ARITHMETIC) ---
MAX_SEQ = 1 << 32
HALF_SEQ = 1 << 31

def seq_diff(a: int, b: int) -> int:
    """Trả về khoảng cách có dấu (a - b) theo số học Modulo 2^32."""
    diff = (a - b) % MAX_SEQ
    return diff - MAX_SEQ if diff >= HALF_SEQ else diff

def seq_lt(a: int, b: int) -> bool:
    return seq_diff(a, b) < 0

def seq_lte(a: int, b: int) -> bool:
    return seq_diff(a, b) <= 0

def seq_gt(a: int, b: int) -> bool:
    return seq_diff(a, b) > 0

def seq_gte(a: int, b: int) -> bool:
    return seq_diff(a, b) >= 0


class SlidingWindow:
    def __init__(
        self,
        send_window_size=65535,
        receive_window_size=256 * 1024,
        initial_seq_num=0
    ):
        self.send_window_size = send_window_size
        self.send_base = initial_seq_num % MAX_SEQ
        self.next_seq_num = initial_seq_num % MAX_SEQ
        self.unacked_packets = {}

        self.receive_window_size = receive_window_size
        self.receive_next = initial_seq_num % MAX_SEQ
        self.receive_buffer = {}

    def can_send(self, payload_length: int) -> bool:
        if payload_length <= 0:
            return False
        return seq_lte(self.next_seq_num + payload_length, self.send_base + self.send_window_size)

    def add_packet(self, seq_num: int, packet, payload_length: int) -> bool:
        seq_num %= MAX_SEQ
        if seq_num != self.next_seq_num:
            return False

        if not self.can_send(payload_length):
            return False

        self.unacked_packets[seq_num] = (packet, payload_length)
        self.next_seq_num = (self.next_seq_num + payload_length) % MAX_SEQ
        return True

    def process_ack(self, ack_num: int) -> bool:
        ack_num %= MAX_SEQ
        if seq_lte(ack_num, self.send_base):
            return False

        if seq_gt(ack_num, self.next_seq_num):
            return False

        self.send_base = ack_num

        acknowledged = [
            seq
            for seq, (_, length) in self.unacked_packets.items()
            if seq_lte((seq + length) % MAX_SEQ, ack_num)
        ]

        for seq in acknowledged:
            self.unacked_packets.pop(seq, None)

        return True

    def get_unacked_packets(self):
        return list(self.unacked_packets.items())

    def receive_packet(self, seq_num: int, payload: bytes):
        if not payload:
            return b"", self.receive_next

        seq_num %= MAX_SEQ
        payload_length = len(payload)
        packet_end = (seq_num + payload_length) % MAX_SEQ
        window_end = (self.receive_next + self.receive_window_size) % MAX_SEQ

        if seq_lt(seq_num, self.receive_next) or seq_gt(packet_end, window_end):
            return b"", self.receive_next

        self.receive_buffer[seq_num] = payload
        received_data = bytearray()

        while self.receive_next in self.receive_buffer:
            chunk = self.receive_buffer.pop(self.receive_next)
            received_data.extend(chunk)
            self.receive_next = (self.receive_next + len(chunk)) % MAX_SEQ

        return bytes(received_data), self.receive_next

    def buffered_bytes(self) -> int:
        return sum(len(payload) for payload in self.receive_buffer.values())