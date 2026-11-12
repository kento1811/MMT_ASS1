def checksum(data: bytes) -> int:
    """Tính 1's complement checksum 16-bit cho Header + Payload."""
    # Thêm byte 0 nếu dữ liệu có độ dài lẻ.
    if len(data) % 2:
        data += b"\x00"

    # Chia dữ liệu thành các word 16-bit và tính tổng.
    total = sum(
        int.from_bytes(data[i:i + 2], "big")
        for i in range(0, len(data), 2)
    )

    # Cộng phần carry về 16-bit thấp.
    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)

    # Đảo bit để tạo checksum.
    return (~total) & 0xFFFF


# --- HỖ TRỢ XỬ LÝ WRAP-AROUND 32-BIT ---
MAX_SEQ = 1 << 32
HALF_SEQ = 1 << 31


def seq_diff(a: int, b: int) -> int:
    """Trả về khoảng cách có dấu (a - b) theo Modulo 2^32."""
    # Xử lý hiệu theo không gian sequence number 32-bit.
    diff = (a - b) % MAX_SEQ
    return diff - MAX_SEQ if diff >= HALF_SEQ else diff


# Các hàm so sánh sequence number có xét wrap-around.
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
        # Khởi tạo trạng thái cửa sổ gửi.
        self.send_window_size = send_window_size
        self.send_base = initial_seq_num % MAX_SEQ
        self.next_seq_num = initial_seq_num % MAX_SEQ
        self.unacked_packets = {}

        # Khởi tạo trạng thái cửa sổ nhận.
        self.receive_window_size = receive_window_size
        self.receive_next = initial_seq_num % MAX_SEQ
        self.receive_buffer = {}

    def can_send(self, payload_length: int) -> bool:
        # Kiểm tra dữ liệu có nằm trong giới hạn cửa sổ gửi.
        if payload_length <= 0:
            return False
        return seq_lte(
            self.next_seq_num + payload_length,
            self.send_base + self.send_window_size
        )

    def add_packet(self, seq_num: int, packet, payload_length: int) -> bool:
        # Chỉ chấp nhận gói có sequence number đúng thứ tự gửi.
        seq_num %= MAX_SEQ
        if seq_num != self.next_seq_num:
            return False

        if not self.can_send(payload_length):
            return False

        # Lưu gói để truyền lại nếu chưa được xác nhận.
        self.unacked_packets[seq_num] = (packet, payload_length)
        self.next_seq_num = (self.next_seq_num + payload_length) % MAX_SEQ
        return True

    def process_ack(self, ack_num: int) -> bool:
        # Kiểm tra ACK có hợp lệ và xác nhận dữ liệu mới hay không.
        ack_num %= MAX_SEQ
        if seq_lte(ack_num, self.send_base):
            return False

        if seq_gt(ack_num, self.next_seq_num):
            return False

        self.send_base = ack_num

        # Xác định các gói đã được xác nhận hoàn toàn.
        acknowledged = [
            seq
            for seq, (_, length) in self.unacked_packets.items()
            if seq_lte((seq + length) % MAX_SEQ, ack_num)
        ]

        # Xóa các gói đã được xác nhận khỏi bộ đệm.
        for seq in acknowledged:
            self.unacked_packets.pop(seq, None)

        return True

    def get_unacked_packets(self):
        # Lấy danh sách các gói chưa được xác nhận.
        return list(self.unacked_packets.items())

    def receive_packet(self, seq_num: int, payload: bytes):
        # Gói rỗng không làm thay đổi trạng thái nhận.
        if not payload:
            return b"", self.receive_next

        seq_num %= MAX_SEQ
        payload_length = len(payload)
        packet_end = (seq_num + payload_length) % MAX_SEQ
        window_end = (self.receive_next + self.receive_window_size) % MAX_SEQ

        # Loại bỏ gói nằm ngoài cửa sổ nhận.
        if seq_lt(seq_num, self.receive_next) or seq_gt(packet_end, window_end):
            return b"", self.receive_next

        # Lưu gói để ghép lại khi đủ dữ liệu đúng thứ tự.
        self.receive_buffer[seq_num] = payload
        received_data = bytearray()

        # Lấy liên tiếp các gói bắt đầu từ sequence number mong đợi.
        while self.receive_next in self.receive_buffer:
            chunk = self.receive_buffer.pop(self.receive_next)
            received_data.extend(chunk)
            self.receive_next = (self.receive_next + len(chunk)) % MAX_SEQ

        # Trả về dữ liệu liên tục và ACK tiếp theo cần nhận.
        return bytes(received_data), self.receive_next

    def buffered_bytes(self) -> int:
        # Tính tổng số byte đang chờ trong bộ đệm nhận.
        return sum(len(payload) for payload in self.receive_buffer.values())