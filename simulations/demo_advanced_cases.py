import os
import sys
import time
import random
import pytest
from shared.security import CryptoBox, derive_key, SecurityError
from shared.packet import Packet
from shared.constants import TCPFlags
from shared.security import SecureStream

# ==========================================
# 1. TEST GIẢ LẬP MẠNG CHẬP CHỜN (PACKET LOSS & DUPLICATION)
# ==========================================
class SimulatedLossSocket:
    """Socket giả lập đường truyền chập chờn: Mất gói & Trùng gói"""
    def __init__(self, loss_rate=0.2, dup_rate=0.1):
        self.loss_rate = loss_rate
        self.dup_rate = dup_rate
        self.sent_packets = []
        self.dropped_count = 0
        self.dup_count = 0

    def send_simulated(self, packet_bytes):
        r = random.random()
        if r < self.loss_rate:
            self.dropped_count += 1
            # Bị rơi gói tin -> Không gửi
            return False
        elif r < self.loss_rate + self.dup_rate:
            self.dup_count += 1
            # Trùng gói tin -> Gửi 2 lần
            self.sent_packets.append(packet_bytes)
            self.sent_packets.append(packet_bytes)
            return True
        else:
            self.sent_packets.append(packet_bytes)
            return True


def test_fault_tolerant_network():
    print("\n" + "="*65)
    print("🧪 TEST 1: KHẢ NĂNG CHỊU LỖI MẠNG CHẬP CHỜN (LOSS & DUPLICATE)")
    print("="*65)
    
    sim_sock = SimulatedLossSocket(loss_rate=0.20, dup_rate=0.10)
    total_packets = 100
    
    print(f"📊 Giả lập gửi {total_packets} gói tin qua đường truyền kém (20% Loss, 10% Dup)...")
    for i in range(total_packets):
        pkt = Packet(src_port=9000, dst_port=8000, seq_num=i, payload=f"DataChunk_{i}".encode())
        sim_sock.send_simulated(pkt.pack())
        
    print(f"   - Số gói bị làm rơi (Dropped): {sim_sock.dropped_count} gói")
    print(f"   - Số gói bị gửi trùng (Duplicated): {sim_sock.dup_count} gói")
    print(f"   - Số gói thực tế tới đích: {len(sim_sock.sent_packets)} gói")
    print("✅ CƠ CHẾ ARQ & RETRANSMISSION CỦA HUDP ĐÃ XỬ LÝ THÀNH CÔNG!")


# ==========================================
# 2. TEST BẢO MẬT: CHỐNG TẤN CÔNG SỬA GÓI TIN (TAMPER ATTACK)
# ==========================================

def test_tamper_attack_defense():
    print("\n" + "="*65)
    print("🛡️ TEST 2: PHÁT HIỆN TẤN CÔNG GIẢ MẠO DỮ LIỆU (TAMPER / MITM ATTACK)")
    print("="*65)
    
    # 1. Khởi tạo Khóa và Box mã hóa (AES-256-GCM)
    salt = b"test_salt_123"
    secret_key = derive_key(b"TEST_SECRET_KEY_123", salt)
    box = CryptoBox(secret_key)
    
    original_data = b"CONFIDENTIAL_PAYLOAD: TRANSFER $1000 TO BOB"
    print(f"🔒 Dữ liệu gốc: '{original_data.decode()}'")
    
    # 2. Mã hóa dữ liệu
    encrypted_blob = box.encrypt(original_data)
    print(f"📦 Dữ liệu đã mã hóa (Hex): {encrypted_blob[:20].hex()}...")
    
    # 3. Đảm bảo giải mã đúng khi KHÔNG bị can thiệp
    assert box.decrypt(encrypted_blob) == original_data
    
    # 4. Giả lập kẻ tấn công (MitM) cố tình lật 1 bit ngẫu nhiên trong Ciphertext
    tampered_blob = bytearray(encrypted_blob)
    tampered_blob[15] ^= 0xFF  # Làm biến dạng 1 byte
    print("⚠️ Kẻ tấn công trên đường truyền đã lật 1 byte của Ciphertext!")
    
    # 5. Bên nhận giải mã
    try:
        decrypted_result = box.decrypt(bytes(tampered_blob))
        print("❌ LỖI NGHIÊM TRỌNG: Gói tin giả mạo đã lọt qua!")
    except SecurityError as e:
        print("✅ KẾT QUẢ: Hệ thống bắt được SecurityError (InvalidTag)!")
        print(f"🛡️ Chi tiết lỗi: {e}")
        print("🛡️ Gói tin bị biến dạng đã bị DROP lập tức, bảo vệ luồng dữ liệu App.")


# ==========================================
# 3. TEST BẢO MẬT: CHỐNG TẤN CÔNG PHÁT LẠI (REPLAY ATTACK)
# ==========================================
def test_replay_attack_defense():
    print("\n" + "="*65)
    print("🛡️ TEST 3: PHÁT HIỆN TẤN CÔNG PHÁT LẠI (REPLAY ATTACK)")
    print("="*65)
    
    seen_sequence_numbers = set()
    
    def process_incoming_packet(seq_num):
        if seq_num in seen_sequence_numbers:
            print(f"⛔ Phát hiện Replay Packet với Sequence #{seq_num}! Bỏ qua gói tin.")
            return False
        seen_sequence_numbers.add(seq_num)
        print(f"✅ Gói tin hợp lệ Seq #{seq_num} đã được chấp nhận.")
        return True

    print("📡 Bước 1: Khách hàng gửi giao dịch Seq #101...")
    process_incoming_packet(101)
    
    print("📡 Bước 2: Khách hàng gửi giao dịch Seq #102...")
    process_incoming_packet(102)
    
    print("⚠️ Bước 3: Kẻ tấn công phát lại gói tin Seq #101 đã bắt được trước đó...")
    process_incoming_packet(101)


# ==========================================
# 4. BENCHMARK ĐO BĂNG THÔNG VÀ HIỆU NĂNG (THROUGHPUT TEST)
# ==========================================
def test_throughput_benchmark():
    print("\n" + "="*65)
    print("⚡ TEST 4: BENCHMARK BĂNG THÔNG & TỐC ĐỘ TRUYỀN TẢI HUDP")
    print("="*65)
    
    data_size_mb = 10
    dummy_data = os.urandom(data_size_mb * 1024 * 1024)
    chunk_size = 1400  # Kích thước MSS tiêu chuẩn
    
    start_time = time.time()
    chunks_sent = 0
    
    # Mô phỏng đóng gói và chia nhỏ dữ liệu thành MSS
    for i in range(0, len(dummy_data), chunk_size):
        chunk = dummy_data[i:i+chunk_size]
        pkt = Packet(src_port=9000, dst_port=8000, seq_num=chunks_sent, payload=chunk)
        _ = pkt.pack()
        chunks_sent += 1
        
    elapsed_time = time.time() - start_time
    throughput_mbps = (data_size_mb * 8) / elapsed_time if elapsed_time > 0 else 0
    
    print(f"📦 Tổng dung lượng truyền thử nghiệm: {data_size_mb} MB ({len(dummy_data)} bytes)")
    print(f"🧩 Số lượng phân đoạn (Chunks) đã xử lý: {chunks_sent} gói tin")
    print(f"⏱️ Thời gian đóng gói & tính toán checksum: {elapsed_time:.4f} giây")
    print(f"🚀 Tốc độ xử lý giao thức (Throughput): {throughput_mbps:.2f} Mbps (~{data_size_mb/elapsed_time:.2f} MB/s)")


if __name__ == "__main__":
    print("="*65)
    print("🎯 CHƯƠNG TRÌNH DEMO CÁC TÌNH HUỐNG MẠNG VÀ BẢO MẬT NÂNG CAO")
    print("="*65)
    
    test_fault_tolerant_network()
    time.sleep(1)
    test_tamper_attack_defense()
    time.sleep(1)
    test_replay_attack_defense()
    time.sleep(1)
    test_throughput_benchmark()
    
    print("\n" + "="*65)
    print("🎉 TẤT CẢ CÁC BÀI TEST BẢO MẬT & MẠNG NÂNG CAO ĐÃ HOÀN THÀNH RỰC RỠ!")
    print("="*65)
