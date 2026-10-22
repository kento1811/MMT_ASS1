
def checksum(data: bytes) -> int:
    if len(data) % 2:
        data += b"\x00"

    total = sum(
        int.from_bytes(data[i:i + 2], "big")
        for i in range(0, len(data), 2)
    )

    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)

    return (~total) & 0xFFFF


class SlidingWindow:
    def __init__(
        self,
        send_window_size=1024,
        receive_window_size=1024,
        initial_seq_num=0
    ):
        self.send_window_size = send_window_size
        self.send_base = initial_seq_num
        self.next_seq_num = initial_seq_num
        self.unacked_packets = {}

        self.receive_window_size = receive_window_size
        self.receive_next = initial_seq_num
        self.receive_buffer = {}

    def can_send(self, payload_length):
        return (
            payload_length > 0
            and self.next_seq_num + payload_length
            <= self.send_base + self.send_window_size
        )

    def add_packet(self, seq_num, packet, payload_length):
        if seq_num != self.next_seq_num:
            return False

        if not self.can_send(payload_length):
            return False

        self.unacked_packets[seq_num] = (packet, payload_length)
        self.next_seq_num += payload_length
        return True

    def process_ack(self, ack_num):
        if ack_num <= self.send_base:
            return False

        if ack_num > self.next_seq_num:
            return False

        self.send_base = ack_num

        acknowledged = [
            seq
            for seq, (_, length) in self.unacked_packets.items()
            if seq + length <= ack_num
        ]

        for seq in acknowledged:
            del self.unacked_packets[seq]

        return True

    def get_unacked_packets(self):
        return list(self.unacked_packets.items())

    def receive_packet(self, seq_num, payload):
        if not payload:
            return b"", self.receive_next

        payload_length = len(payload)
        packet_end = seq_num + payload_length
        window_end = self.receive_next + self.receive_window_size

        if seq_num < self.receive_next or packet_end > window_end:
            return b"", self.receive_next

        for buffered_seq, buffered_payload in self.receive_buffer.items():
            buffered_end = buffered_seq + len(buffered_payload)

            if seq_num < buffered_end and buffered_seq < packet_end:
                return b"", self.receive_next

        self.receive_buffer[seq_num] = payload
        received_data = bytearray()

        while self.receive_next in self.receive_buffer:
            chunk = self.receive_buffer.pop(self.receive_next)
            received_data.extend(chunk)
            self.receive_next += len(chunk)

        return bytes(received_data), self.receive_next
    def buffered_bytes(self):
        """Tổng byte payload đang buffer out-of-order (chưa assemble)."""
        return sum(len(payload) for payload in self.receive_buffer.values())