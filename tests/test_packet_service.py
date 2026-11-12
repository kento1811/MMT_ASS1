import pytest
import socket
import time
from shared.packet_service import PacketService
from shared.algorithms import seq_gt, seq_lt, seq_diff, MAX_SEQ

def test_sequence_number_wrap_around():
    """Kiểm tra tính toán số thứ tự khi chạm mốc 2^32."""
    seq1 = MAX_SEQ - 100
    seq2 = 50
    # seq2 nằm sau seq1 theo số học vòng Modulo 2^32
    assert seq_gt(seq2, seq1) is True
    assert seq_lt(seq1, seq2) is True
    assert seq_diff(seq2, seq1) == 150

def test_packet_service_local_loopback():
    """Kiểm tra khởi tạo và bắt tay local giả lập giữa 2 socket."""
    sock1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock1.bind(("127.0.0.1", 0))
    port1 = sock1.getsockname()[1]

    sock2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock2.bind(("127.0.0.1", 0))
    port2 = sock2.getsockname()[1]

    svc1 = PacketService(sock1, port1, port2, shared_socket=False)
    svc2 = PacketService(sock2, port2, port1, shared_socket=False)

    assert svc1.state == "CLOSED"
    assert svc2.state == "CLOSED"

    sock1.close()
    sock2.close()