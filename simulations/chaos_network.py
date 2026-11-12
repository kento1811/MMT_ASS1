import socket
import threading
import time
import random
from server.main import start_server
from client.stream_manager import ClientStreamManager
from shared.schemas import LoginRequest, BroadcastRequest
from simulations.utils import clear_screen, setup_logger

logger = setup_logger("Chaos_Net")
LOSS_RATE = 0.3  # Tỉ lệ làm rơi gói tin: 30%

class ChaosSocket(socket.socket):
    """Ghi đè (Override) phương thức sendto của OS để giả lập rớt gói"""
    def sendto(self, data, address):
        if random.random() < LOSS_RATE:
            logger.warning(f"[Chaos] 💥 ĐÃ NUỐT 1 GÓI TIN bay tới {address[1]}! (Size: {len(data)} bytes)")
            return len(data) 
        return super().sendto(data, address)

def main():
    clear_screen()
    logger.info("="*70)
    logger.info("🚀 BẮT ĐẦU BÀI TEST CHAOS ENGINEERING (TRÊN SOCKET THỰC TẾ)")
    logger.info("="*70)

    # 1. Bật Server thật
    logger.info("[Hệ thống] Đang bật Server tại 0.0.0.0:9000...")
    threading.Thread(target=start_server, daemon=True).start()
    time.sleep(1)

    # 2. Bật Client và tiêm mã Chaos
    logger.info("\n[Hệ thống] Khởi tạo Client và tiêm mã Chaos vào socket...")
    sm = ClientStreamManager(message_callback=lambda msg: logger.info(f"[Client nhận] {msg}"))
    
    sm.sock = ChaosSocket(socket.AF_INET, socket.SOCK_DGRAM)
    sm.sock.bind(("0.0.0.0", 0))
    
    logger.info("[Hệ thống] Bắt đầu Connect (Cẩn thận, gói SYN có thể bị nuốt)...")
    if sm.connect():
        logger.info("✅ [Client] Bắt tay 3 bước & ECDH thành công (Vượt qua bão Packet Loss)!\n")
        sm.send_json(LoginRequest(username="ChaosBot"))
        time.sleep(1)

        logger.info("-" * 70)
        logger.info("🔥 BẮT ĐẦU SPAM DỮ LIỆU ĐỂ ÉP FAST RETRANSMIT VÀ RTO")
        logger.info("-" * 70)
        
        for i in range(1, 4):
            msg = f"Đây là thông điệp số {i} - " + "Dữ liệu rác " * 20
            sm.send_json(BroadcastRequest(msg=msg))
            time.sleep(0.5)

        logger.info("\n⏳ Đang chờ hệ thống tự động gửi lại (Retransmit) các gói bị nuốt...")
        time.sleep(5) 

        logger.info("\n" + "-" * 70)
        logger.info("💀 TEST RÚT CÁP ĐỘT NGỘT (DIRTY DISCONNECT)")
        logger.info("-" * 70)
        logger.info("[Hệ thống] Bóp chết socket của Client đột ngột mà không gửi cờ FIN...")
        
        # Kill nóng socket
        sm.sock.close() 
        sm.is_connected = False
        
        logger.info("⏳ Chờ Server nhận ra client bốc hơi (thông qua Keepalive/Idle Timeout)...")
        time.sleep(5)
        
        logger.info("\n✅ HOÀN TẤT KỊCH BẢN THỰC CHIẾN!")
    else:
        logger.error("❌ [Client] Kết nối thất bại do xui xẻo rớt sạch gói SYN/SYN-ACK.")

if __name__ == "__main__":
    main()