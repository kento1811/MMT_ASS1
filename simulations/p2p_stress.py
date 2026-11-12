import os
import time
import threading
from client.stream_manager import ClientStreamManager
from client.file_handler import FileHandler
from shared.schemas import LoginRequest, FileSendRequest, FileAcceptResponse
from server.main import start_server
from simulations.utils import clear_screen, cleanup_artifacts, setup_logger

logger = setup_logger("P2P_Stress")

class HeadlessClient:
    def __init__(self, username):
        self.username = username
        self.download_dir = f"downloads_{username}"
        self.fh = FileHandler(download_dir=self.download_dir)
        self.sm = ClientStreamManager(message_callback=self.on_message)
        self.pending_uploads = {}
        
    def connect(self):
        if self.sm.connect():
            self.sm.send_json(LoginRequest(username=self.username))
            time.sleep(0.5) 
            return True
        return False
        
    def on_message(self, msg):
        action = msg.get("action")
        
        if action == "INCOMING_FILE_REQ":
            sender, filename = msg.get("from_user"), msg.get("filename")
            logger.info(f"[{self.username}] 📥 Tự động accept file '{filename}' từ {sender}.")
            
            # ĐÃ SỬA: Đón nhận cả listen_ip và listen_port từ file_handler
            listen_ip, listen_port = self.fh.start_receiving(filename)
            # Tạo session_id chuẩn xác với 3 thành phần: IP:Port:Filename
            session_id = f"{listen_ip}:{listen_port}:{filename}"
            self.sm.send_json(FileAcceptResponse(to_user=sender, session_id=session_id))
            
        elif action == "SIGNAL_FILE_READY":
            # ĐÃ SỬA: Bóc tách 3 thành phần từ session_id
            target_ip, target_port, filename = msg.get("session_id").split(":", 2)
            filepath = self.pending_uploads.get(filename)
            
            if filepath:
                logger.info(f"[{self.username}] 📤 Mở luồng P2P đẩy '{filename}' -> {target_ip}:{target_port}.")
                threading.Thread(
                    target=self.fh.send_file,
                    # Truyền IP thực tế lấy được vào hàm send_file
                    args=(target_ip, int(target_port), filepath),
                    daemon=True
                ).start()

    def send_file(self, target_user, filepath):
        filename = os.path.basename(filepath)
        self.pending_uploads[filename] = filepath
        req = FileSendRequest(
            to_user=target_user,
            filename=filename,
            filesize=os.path.getsize(filepath)
        )
        self.sm.send_json(req)
        
    def close(self):
        try:
            if self.sm.svc:
                self.sm.svc.close(timeout=0.5)
            self.sm.close()
        except Exception:
            pass

def create_dummy_file(filename, size_kb=256):
    with open(filename, "wb") as f:
        f.write(os.urandom(size_kb * 1024))

def wait_for_files(expected_files, timeout=30):
    """Cơ chế chờ động: Liên tục kiểm tra xem file trên đĩa đã ghi đủ dung lượng chưa"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        all_done = True
        for filepath, expected_size in expected_files.items():
            if not os.path.exists(filepath) or os.path.getsize(filepath) < expected_size:
                all_done = False
                break
        if all_done:
            return True
        time.sleep(1)
    return False

def test_many_to_one():
    logger.info("\n" + "="*60 + "\n🚀 KỊCH BẢN 1: 4 SENDER ĐẨY FILE CHO 1 RECEIVER\n" + "="*60)
    
    file_size_kb = 256
    expected_size = file_size_kb * 1024
    expected_files = {}

    for i in range(1, 5):
        fname = f"test_payload_{i}.bin"
        create_dummy_file(fname, file_size_kb)
        expected_files[f"downloads_TargetBob/{fname}"] = expected_size
        
    receiver = HeadlessClient("TargetBob")
    receiver.connect()
    
    senders = [HeadlessClient(f"SenderAlice_{i}") for i in range(1, 5)]
    for s in senders:
        s.connect()
        
    time.sleep(1)
    for i, s in enumerate(senders):
        s.send_file("TargetBob", f"test_payload_{i+1}.bin")
        
    logger.info(f"\n⏳ Truyền 4 file P2P song song ({file_size_kb}KB/file)... Đang chờ tải hoàn tất.")
    
    if wait_for_files(expected_files, timeout=30):
        logger.info("✅ Cả 4 file đã được tải về thành công và đầy đủ dung lượng!")
    else:
        logger.error("❌ Hết thời gian chờ 30s, một số file tải không thành công.")
    
    receiver.close()
    for s in senders: s.close()
        
def test_mutual_transfer():
    logger.info("\n" + "="*60 + "\n🚀 KỊCH BẢN 2: 2 CLIENT TRUYỀN P2P FULL-DUPLEX\n" + "="*60)
    
    file_size_kb = 256
    expected_size = file_size_kb * 1024
    
    create_dummy_file("alice_to_bob.bin", file_size_kb)
    create_dummy_file("bob_to_alice.bin", file_size_kb)
    
    expected_files = {
        "downloads_Bob/alice_to_bob.bin": expected_size,
        "downloads_Alice/bob_to_alice.bin": expected_size
    }

    alice, bob = HeadlessClient("Alice"), HeadlessClient("Bob")
    alice.connect()
    bob.connect()
    time.sleep(1)
    
    alice.send_file("Bob", "alice_to_bob.bin")
    bob.send_file("Alice", "bob_to_alice.bin")
    
    logger.info("\n⏳ Truyền P2P 2 chiều... Đang chờ tải hoàn tất.")
    
    if wait_for_files(expected_files, timeout=25):
        logger.info("✅ Cả 2 chiều (Alice <-> Bob) đã tải xong đầy đủ dung lượng!")
    else:
        logger.error("❌ Hết thời gian chờ, một số file tải không thành công.")
    
    alice.close()
    bob.close()

if __name__ == "__main__":
    clear_screen()
    logger.info("BẮT ĐẦU P2P STRESS TEST (PHIÊN BẢN ĐÃ FIX TREO & MẠNG LAN)...")

    logger.info("[Hệ thống] Đang tự động bật Server tại 0.0.0.0:9000...")
    threading.Thread(target=start_server, daemon=True).start()
    time.sleep(1)
    
    cleanup_artifacts()
    logger.info("🧹 Đã dọn dẹp workspace trước khi test.")
    
    try:
        test_many_to_one()
        time.sleep(2)
        test_mutual_transfer()
        logger.info("\n✅ KẾT QUẢ: TOÀN BỘ KỊCH BẢN P2P ĐÃ HOÀN TẤT VÀ ĐƯỢC GHI LOG!")
    except Exception as e:
        logger.error(f"\n❌ LỖI: {e}")
    finally:
        cleanup_artifacts()
        logger.info("🧹 Đã dọn dẹp artifacts sau khi test xong.")