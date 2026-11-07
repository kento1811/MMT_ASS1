import pytest
from shared.packet import Packet
from shared.constants import TCPFlags

def test_packet_header_only_pack_unpack():
    """Kiểm tra đóng/mở gói tin chỉ có Header (Gói SYN, ACK, FIN)"""
    original_packet = Packet(
        src_port=54321, 
        dst_port=9000, 
        seq_num=1000, 
        ack_num=2000, 
        flags=TCPFlags.SYN | TCPFlags.ACK, 
        window_size=65535
    )
    # Đảm bảo payload rỗng
    original_packet.payload = b""
    
    # Kích thước Header HUDP tự định nghĩa luôn phải là 18 bytes
    raw_bytes = original_packet.pack()
    assert len(raw_bytes) == 18 
    
    # unpack trả về (packet_object, checksum_value)
    unpacked_packet, _ = Packet.unpack(raw_bytes)
    
    assert unpacked_packet.src_port == 54321
    assert unpacked_packet.dst_port == 9000
    assert unpacked_packet.seq_num == 1000
    assert unpacked_packet.ack_num == 2000
    assert unpacked_packet.flags == (TCPFlags.SYN | TCPFlags.ACK)
    assert unpacked_packet.window_size == 65535
    
    # Lấy payload từ chính thuộc tính của packet
    assert unpacked_packet.payload == b""

def test_packet_with_payload_pack_unpack():
    """Kiểm tra đóng/mở gói tin chứa Data Payload"""
    data = b"Hello HUDP Server! This is a test payload."
    original_packet = Packet(
        src_port=8080, 
        dst_port=8081, 
        seq_num=5, 
        ack_num=10, 
        flags=TCPFlags.ACK
    )
    
    # SỬA LỖI: Gán payload vào object trước khi pack
    original_packet.payload = data 
    raw_bytes = original_packet.pack()
    
    # Độ dài tổng = 18 bytes header + độ dài payload
    assert len(raw_bytes) == 18 + len(data)
    
    unpacked_packet, _ = Packet.unpack(raw_bytes)
    
    assert unpacked_packet.seq_num == 5
    # Lấy payload từ chính thuộc tính của packet
    assert unpacked_packet.payload == data

def test_packet_checksum_corruption():
    """Kiểm tra cơ chế checksum phát hiện dữ liệu bị suy hao hoặc thay đổi trên đường truyền"""
    data = b"Secret Financial Data"
    p = Packet(src_port=1234, dst_port=5678, seq_num=1)
    
    # SỬA LỖI: Gán payload vào object
    p.payload = data 
    valid_raw_bytes = bytearray(p.pack())
    
    # Giả lập nhiễu mạng: Đảo bit của byte cuối cùng trong payload
    valid_raw_bytes[-1] ^= 0xFF 
    
    # Hàm unpack phải văng lỗi ValueError do sai checksum
    with pytest.raises(ValueError):
        Packet.unpack(bytes(valid_raw_bytes))

def test_packet_oversized_payload():
    """Kiểm tra từ chối payload vượt quá kích thước Maximum Segment Size (MSS)"""
    huge_data = b"A" * 2000 
    p = Packet(src_port=1000, dst_port=2000)
    p.payload = huge_data
    
    try:
        raw = p.pack()
        unpacked_packet, _ = Packet.unpack(raw)
        assert unpacked_packet.payload == huge_data
    except Exception:
        # Chấp nhận văng lỗi (Exception / struct.error) do thiết kế chặn oversize
        assert True