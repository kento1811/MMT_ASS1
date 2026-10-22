
import struct
from constants import TCPFlags
from algorithms import checksum


class Packet:
    HEADER_FORMAT = "!HHIIHHH"
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
        self.src_port = src_port
        self.dst_port = dst_port
        self.seq_num = seq_num
        self.ack_num = ack_num
        self.flags = TCPFlags(flags)
        self.window_size = window_size
        self.payload = payload

    def pack(self) -> bytes:
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

        segment = header + self.payload
        checksum_value = checksum(segment)

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

        return header + self.payload

    @classmethod
    def unpack(cls, raw_bytes: bytes):
        if len(raw_bytes) < cls.HEADER_SIZE:
            raise ValueError("Packet không đủ header!")

        if checksum(raw_bytes) != 0:
            raise ValueError("Checksum không hợp lệ!")

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

        packet = cls(
            src_port=src_port,
            dst_port=dst_port,
            seq_num=seq_num,
            ack_num=ack_num,
            flags=TCPFlags(flags),
            window_size=window_size,
            payload=raw_bytes[cls.HEADER_SIZE:],
        )

        return packet, checksum_value
