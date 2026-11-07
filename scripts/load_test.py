import time
import threading
from client.stream_manager import ClientStreamManager
from shared.schemas import LoginRequest, BroadcastRequest

NUM_BOTS = 20 # Số lượng client đồng thời (10-20 theo yêu cầu)

def bot_worker(bot_id):
    def on_msg(msg):
        pass # Bỏ qua việc in tin nhắn để tránh rác terminal
    
    # Khởi tạo lõi mạng độc lập cho mỗi Bot
    sm = ClientStreamManager(message_callback=on_msg)
    
    if sm.connect():
        print(f"[+] Bot_{bot_id} đã Handshake & thiết lập Bảo mật thành công!")
        # 1. Đăng nhập
        sm.send_json(LoginRequest(username=f"Bot_{bot_id}"))
        time.sleep(1)
        
        # 2. Spam tin nhắn nhóm để test định tuyến và chịu tải của Server
        for i in range(3):
            sm.send_json(BroadcastRequest(msg=f"Hello mọi người, tôi là Bot {bot_id} - Tin số {i}"))
            time.sleep(0.5)
            
        time.sleep(2)
        sm.close()
        print(f"[-] Bot_{bot_id} đã ngắt kết nối an toàn.")
    else:
        print(f"[!] Bot_{bot_id} kết nối thất bại!")

def main():
    print(f"🚀 BẮT ĐẦU BÀI TEST CHỊU TẢI VỚI {NUM_BOTS} CLIENTS...\n")
    threads = []
    
    for i in range(NUM_BOTS):
        t = threading.Thread(target=bot_worker, args=(i,))
        threads.append(t)
        t.start()
        # Tránh bão SYN (SYN Flood) cùng 1 mili-giây gây nghẽn phần cứng cục bộ
        time.sleep(0.2) 
        
    for t in threads:
        t.join()
        
    print("\n✅ LOAD TEST HOÀN TẤT! Hãy kiểm tra log của Server.")

if __name__ == "__main__":
    main()