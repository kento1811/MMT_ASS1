import os
import sys
import time

from client.stream_manager import ClientStreamManager
from client.file_handler import FileHandler
from client.ui_bridge import UIBridge

# Khởi tạo các biến toàn cục (nhưng chưa khởi tạo UIBridge vội)
file_handler = FileHandler()
pending_uploads = {}
ui_bridge = None


def handle_incoming_message(msg_obj: dict):
    action = msg_obj.get("action")

    if action == "SIGNAL_FILE_READY":
        session_id = msg_obj.get("session_id")
        target_port, filename = session_id.split(":", 1)

        filepath = pending_uploads.get(filename)
        if filepath:
            ui_bridge.push_message_to_ui(
                {"action": "SYSTEM", "msg": f"Bắt đầu đẩy luồng P2P File: {filename}"}
            )
            file_handler.send_file("127.0.0.1", int(target_port), filepath)
        else:
            ui_bridge.push_message_to_ui(
                {
                    "action": "SYSTEM",
                    "msg": f"Lỗi: Không tìm thấy cache file: {filename}",
                }
            )

    # Đẩy tín hiệu lên Web UI
    if ui_bridge:
        ui_bridge.push_message_to_ui(msg_obj)


def main():
    global ui_bridge

    # 1. Đọc Port từ dòng lệnh (mặc định là 8080 nếu không truyền gì)
    http_port = 36
    if len(sys.argv) > 1:
        http_port = int(sys.argv[1])

    # 2. KHỞI TẠO UI BRIDGE VỚI ĐÚNG PORT
    ui_bridge = UIBridge(host="0.0.0.0", port=http_port)

    stream_manager = ClientStreamManager(message_callback=handle_incoming_message)
    ui_bridge.setup_context(stream_manager, file_handler, pending_uploads)

    try:
        print("Đang khởi động Client...")
        if stream_manager.connect():
            ui_bridge.start()
            print("🚀 Client đã sẵn sàng. Bấm Ctrl+C để thoát.")
            while True:
                time.sleep(1)

    except KeyboardInterrupt:
        print("\nĐang tắt máy khách...")
    except Exception as e:
        print(f"Lỗi hệ thống: {e}")
    finally:
        stream_manager.close()


if __name__ == "__main__":
    main()
