
import struct
from shared.constants import TCPFlags
from shared.algorithms import checksum


class Packet:
    # Định dạng header của packet:
    # src_port, dst_port: 16 bit
    # seq_num, ack_num: 32 bit
    # flags, window_size, checksum: 16 bit
    HEADER_FORMAT = "!HHIIHHH"

    # Kích thước header tính theo byte
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(
        self,
        src_port: int,
        dst_port: int,
        seq_num: int = 0,
        ack_num: int = 0,
        flags: TCPFlags = TCPFlags(0),
        window_size: int = 65535,
        payload: bytes = b"",
    ):
        # Cổng nguồn và cổng đích
        self.src_port = src_port
        self.dst_port = dst_port

        # Sequence number và Acknowledgment number
        self.seq_num = seq_num
        self.ack_num = ack_num

        # Các cờ điều khiển TCP như SYN, ACK, FIN, RST
        self.flags = TCPFlags(flags)

        # Kích thước cửa sổ nhận mà bên gửi quảng bá
        self.window_size = window_size

        # Dữ liệu được truyền trong packet
        self.payload = payload

    def pack(self) -> bytes:
        # Bước 1: Tạo header với trường checksum tạm thời bằng 0
        header = struct.pack(
            self.HEADER_FORMAT,
            self.src_port,
            self.dst_port,
            self.seq_num,
            self.ack_num,
            int(self.flags),
            self.window_size,
            0,
        )

        # Ghép header và payload để tạo segment hoàn chỉnh
        segment = header + self.payload

        # Bước 2: Tính checksum trên toàn bộ segment
        checksum_value = checksum(segment)

        # Bước 3: Tạo lại header với checksum vừa tính được
        header = struct.pack(
            self.HEADER_FORMAT,
            self.src_port,
            self.dst_port,
            self.seq_num,
            self.ack_num,
            int(self.flags),
            self.window_size,
            checksum_value,
        )

        # Trả về packet hoàn chỉnh gồm header và payload
        return header + self.payload

    @classmethod
    def unpack(cls, raw_bytes: bytes):
        # Kiểm tra packet có đủ kích thước header hay không
        if len(raw_bytes) < cls.HEADER_SIZE:
            raise ValueError("Packet không đủ header!")

        # Kiểm tra tính toàn vẹn dữ liệu bằng checksum
        # Nếu kết quả khác 0 thì packet đã bị lỗi hoặc thay đổi dữ liệu
        if checksum(raw_bytes) != 0:
            raise ValueError("Checksum không hợp lệ!")

        # Giải mã các trường trong header
        (
            src_port,
            dst_port,
            seq_num,
            ack_num,
            flags,
            window_size,
            checksum_value,
        ) = struct.unpack(
            cls.HEADER_FORMAT,
            raw_bytes[:cls.HEADER_SIZE],
        )

        # Tạo đối tượng Packet từ header và payload đã giải mã
        packet = cls(
            src_port=src_port,
            dst_port=dst_port,
            seq_num=seq_num,
            ack_num=ack_num,
            flags=TCPFlags(flags),
            window_size=window_size,
            payload=raw_bytes[cls.HEADER_SIZE:],
        )

        # Trả về packet và checksum nhận được từ header
        return packet, checksum_value
