import subprocess
import time
import sys
import os
from simulations.utils import clear_screen, setup_logger

logger = setup_logger("Simulate_UI")

def main():
    clear_screen()
    logger.info("="*70)
    logger.info("🚀 KHỞI ĐỘNG HUDP NETWORK SIMULATION (KÈM GIAO DIỆN WEB)")
    logger.info("="*70)
    
    creationflags = subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0

    # 1. Bật Server
    logger.info("🌐 Bật Máy Chủ Trung Tâm (Server) tại 0.0.0.0:9000...")
    server = subprocess.Popen([sys.executable, "-m", "server.main"], creationflags=creationflags)
    time.sleep(2.0) # Chờ OS cấp phát port 9000 xong

    # 2. Bật 3 Client UI
    clients = []
    ui_ports = [8001, 8002, 8003]
    
    for port in ui_ports:
        logger.info(f"💻 Bật Client UI tại http://127.0.0.1:{port}...")
        client = subprocess.Popen(
            [sys.executable, "-m", "client.main", str(port)], 
            creationflags=creationflags
        )
        clients.append(client)
        time.sleep(1) # Tránh tranh chấp ghi log đồng thời ở Terminal

    logger.info("\n✅ Mô phỏng đang chạy! Có 4 cửa sổ Terminal phụ đã được mở.")
    logger.info("🛑 Bấm Ctrl+C ở đây để TẮT TOÀN BỘ Server và Client khi test xong.")

    try:
        server.wait()
    except KeyboardInterrupt:
        logger.info("\n🧹 Đang dọn dẹp hệ thống...")
        server.terminate()
        for c in clients:
            c.terminate()
        logger.info("✅ Đã đóng an toàn toàn bộ Máy Chủ và Máy Khách.")

if __name__ == "__main__":
    main()