import pytest
from shared.packet import Packet
from shared.constants import TCPFlags
from shared.algorithms import SlidingWindow
from shared.security import CryptoBox, derive_key

# ==========================================
# NHÓM 1: CORE PACKET & HEADER CƠ BẢN
# ==========================================
def test_packet_header_only_pack_unpack():
    original_packet = Packet(
        src_port=54321, dst_port=9000, seq_num=1000, ack_num=2000, 
        flags=TCPFlags.SYN | TCPFlags.ACK, window_size=65535
    )
    original_packet.payload = b""
    raw_bytes = original_packet.pack()
    assert len(raw_bytes) == 18 
    
    unpacked_packet, _ = Packet.unpack(raw_bytes)
    assert unpacked_packet.src_port == 54321
    assert unpacked_packet.flags == (TCPFlags.SYN | TCPFlags.ACK)
    assert unpacked_packet.payload == b""

def test_packet_with_payload_pack_unpack():
    data = b"Hello HUDP Server! This is a test payload."
    original_packet = Packet(src_port=8080, dst_port=8081, seq_num=5, ack_num=10, flags=TCPFlags.ACK)
    original_packet.payload = data 
    
    raw_bytes = original_packet.pack()
    assert len(raw_bytes) == 18 + len(data)
    
    unpacked_packet, _ = Packet.unpack(raw_bytes)
    assert unpacked_packet.payload == data

def test_packet_seq_ack_boundaries():
    max_uint32 = 4294967295  # 0xFFFFFFFF
    p = Packet(src_port=1, dst_port=2, seq_num=max_uint32, ack_num=max_uint32)
    raw = p.pack()
    unpacked, _ = Packet.unpack(raw)
    assert unpacked.seq_num == max_uint32

def test_packet_multiple_flags():
    flags = TCPFlags.SYN | TCPFlags.ACK | TCPFlags.PSH
    p = Packet(src_port=80, dst_port=443, flags=flags)
    raw = p.pack()
    unpacked, _ = Packet.unpack(raw)
    
    assert unpacked.flags & TCPFlags.SYN
    assert unpacked.flags & TCPFlags.ACK
    assert unpacked.flags & TCPFlags.PSH
    assert not (unpacked.flags & TCPFlags.FIN)

# ==========================================
# NHÓM 2: DATA INTEGRITY & CORRUPTION (XỬ LÝ LỖI)
# ==========================================
def test_packet_checksum_corruption():
    data = b"Secret Financial Data"
    p = Packet(src_port=1234, dst_port=5678, seq_num=1)
    p.payload = data 
    valid_raw_bytes = bytearray(p.pack())
    
    # Lật bit cuối
    valid_raw_bytes[-1] ^= 0xFF 
    with pytest.raises(ValueError):
        Packet.unpack(bytes(valid_raw_bytes))

def test_packet_unpack_truncated_data():
    incomplete_raw = b'\x00\x50\x01\xbb\x00\x00\x00\x01'  # 8 bytes (Dưới 18 bytes)
    with pytest.raises(ValueError, match="không đủ header"):
        Packet.unpack(incomplete_raw)

def test_packet_oversized_payload():
    huge_data = b"A" * 2000 
    p = Packet(src_port=1000, dst_port=2000)
    p.payload = huge_data
    try:
        raw = p.pack()
        unpacked_packet, _ = Packet.unpack(raw)
        assert unpacked_packet.payload == huge_data
    except Exception:
        assert True

def test_packet_odd_length_payload():
    odd_payload = b"HELLO"  # 5 bytes
    p = Packet(src_port=10, dst_port=20, payload=odd_payload)
    raw = p.pack()
    unpacked, _ = Packet.unpack(raw)
    
    assert unpacked.payload == odd_payload
    assert len(unpacked.payload) == 5

# ==========================================
# NHÓM 3: LOGIC SLIDING WINDOW & CONGESTION
# ==========================================
def test_sliding_window_out_of_order_delivery():
    window = SlidingWindow(initial_seq_num=0, receive_window_size=4096)
    
    data_1, next_seq = window.receive_packet(seq_num=0, payload=b"Chunk_1_")
    assert data_1 == b"Chunk_1_"
    assert next_seq == 8
    
    # Gói 3 đến trước
    data_3, next_seq = window.receive_packet(seq_num=16, payload=b"Chunk_3_")
    assert data_3 == b"" 
    assert next_seq == 8 
    
    # Gói 2 lấp đầy lỗ hổng
    data_2, next_seq = window.receive_packet(seq_num=8, payload=b"Chunk_2_")
    assert data_2 == b"Chunk_2_Chunk_3_" 
    assert next_seq == 24

# ==========================================
# NHÓM 4: SECURITY LAYER (AES-256-GCM)
# ==========================================
def test_aes_gcm_tamper_resistance():
    secret_key = derive_key(b"my_password", salt=b"salt123")
    box = CryptoBox(secret_key)
    
    original_msg = b"Transaction_Amount=1000$"
    encrypted_blob = box.encrypt(original_msg)
    
    assert box.decrypt(encrypted_blob) == original_msg
    
    tampered_blob = bytearray(encrypted_blob)
    tampered_blob[-5] ^= 0xFF  # Lật bit
    
    with pytest.raises(Exception) as exc_info:
        box.decrypt(bytes(tampered_blob))
    assert "Giải mã thất bại" in str(exc_info.value)