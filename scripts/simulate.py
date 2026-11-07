import subprocess
import time
import sys
import os

def main():
    print("🚀 Đang khởi động HUDP Network Simulation...")
    
    # Cờ mở cửa sổ Terminal mới (chỉ dùng cho Windows)
    creationflags = subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0

    # 1. Bật Server
    print("Bật Server tại 0.0.0.0:9000...")
    server = subprocess.Popen([sys.executable, "-m", "server.main"], creationflags=creationflags)
    time.sleep(2) # Chờ Server khởi động xong

    # 2. Bật 3 Client với 3 Port Web khác nhau
    clients = []
    ui_ports = [8001, 8002, 8003]
    
    for port in ui_ports:
        print(f"Bật Client UI tại http://127.0.0.1:{port}...")
        client = subprocess.Popen(
            [sys.executable, "-m", "client.main", str(port)], 
            creationflags=creationflags
        )
        clients.append(client)
        time.sleep(1) # Tránh tranh chấp ghi log đồng thời

    print("\n✅ Mô phỏng đang chạy! Có 4 cửa sổ Terminal mới đã được mở.")
    print("Bấm Ctrl+C ở đây để ĐÓNG TẤT CẢ khi test xong.")

    try:
        server.wait()
    except KeyboardInterrupt:
        print("\nĐang dọn dẹp hệ thống...")
        server.terminate()
        for c in clients:
            c.terminate()
        print("Đã đóng toàn bộ Server và Client.")

if __name__ == "__main__":
    main()