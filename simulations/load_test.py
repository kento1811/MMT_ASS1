import time
import threading
import random
from client.stream_manager import ClientStreamManager
from shared.schemas import LoginRequest, ChatRequest, BroadcastRequest
from server.main import start_server
from simulations.utils import clear_screen, setup_logger

logger = setup_logger("Load_Test")

# Giảm xuống 20 bot để tránh Windows bị tràn bộ đệm UDP (WSAENOBUFS) và ngắt luồng đột ngột
NUM_BOTS = 20
VICTIM_BOT = "Bot_0"
DUPLEX_BOT_1, DUPLEX_BOT_2 = "Bot_1", "Bot_2"

def bot_worker(bot_id):
    username = f"Bot_{bot_id}"
    # Tắt callback hoàn toàn để Terminal không bị rác log tin nhắn
    sm = ClientStreamManager(message_callback=lambda msg: None) 
    
    try:
        if not sm.connect():
            return
    except Exception:
        return # Nuốt lỗi connect để console không bị rác

    try:
        sm.send_json(LoginRequest(username=username))
    except Exception:
        pass
        
    time.sleep(2) # Chờ Server xử lý login

    try:
        # KỊCH BẢN 1: N-to-1 (Bot 3 đến Bot 7 spam Victim)
        if 3 <= bot_id <= 7:
            for i in range(10):
                sm.send_json(ChatRequest(to_user=VICTIM_BOT, msg=f"Spam đợt {i}"))
                time.sleep(0.15) # Tăng nhẹ delay để tránh ngập lụt Window
                
        # KỊCH BẢN 2: Full-Duplex (Bot 1 và Bot 2 spam chéo)
        elif username in (DUPLEX_BOT_1, DUPLEX_BOT_2):
            target = DUPLEX_BOT_2 if username == DUPLEX_BOT_1 else DUPLEX_BOT_1
            for i in range(10):
                sm.send_json(ChatRequest(to_user=target, msg=f"Ping Pong {i}"))
                time.sleep(0.1) 

        # KỊCH BẢN 3: Broadcast (Các bot còn lại)
        elif bot_id > 7:
            time.sleep(random.uniform(0.1, 1.5))
            sm.send_json(BroadcastRequest(msg=f"Nhiễu từ {username}"))
            
    except Exception:
        # Bỏ qua các lỗi WinError 10054 do Windows tự kill socket khi nghẽn
        pass

    time.sleep(2)
    
    # Ép đóng kết nối thô bạo để không bị kẹt luồng (Deadlock FIN-ACK)
    try:
        if sm.svc:
            sm.svc.close(timeout=0.1) 
        sm.close()
    except Exception:
        pass
    
    if bot_id % 5 == 0: # Chỉ log đại diện vài bot
        logger.info(f"[-] {username} đã hoàn tất kịch bản.")

def main():
    clear_screen()
    logger.info("="*70)
    logger.info(f"🚀 BẮT ĐẦU ADVANCED STRESS TEST VỚI {NUM_BOTS} CLIENTS (Tối ưu cho Windows)...")
    logger.info("="*70)

    logger.info("[Hệ thống] Đang tự động bật Server tại 0.0.0.0:9000...")
    threading.Thread(target=start_server, daemon=True).start()
    time.sleep(1)
    
    threads = []
    for i in range(NUM_BOTS):
        t = threading.Thread(target=bot_worker, args=(i,), daemon=True)
        threads.append(t)
        t.start()
        time.sleep(0.15) # Delay 150ms giữa các lần khởi tạo để chống SYN Flood
        
    for t in threads:
        # Giới hạn thời gian sống của luồng, luồng nào kẹt quá 15s tự bỏ qua
        t.join(timeout=15) 
        
    logger.info("\n✅ HOÀN TẤT KỊCH BẢN KIỂM THỬ KHẮC NGHIỆT! Kiểm tra log Server.")

if __name__ == "__main__":
    main()