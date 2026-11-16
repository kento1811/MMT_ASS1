import socket
from server.config import HOST, PORT, MAX_DATAGRAM_SIZE
from server.state_manager import StateManager
from server.stream_manager import StreamManager
from server.chat_handler import ChatHandler
from server.file_handler import FileHandler
from server.router import Router


def start_server():
    # Start socket use SOCK_DRAM
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, PORT))
    print(f"Server khởi chạy thành công tại {HOST}:{PORT}")

    # Connect service dependency
    state_manager = StateManager()
    stream_manager = StreamManager(state_manager, sock)

    chat_handler = ChatHandler(state_manager, stream_manager)
    file_handler = FileHandler(state_manager, stream_manager)

    router = Router(stream_manager, chat_handler, file_handler)
    stream_manager.set_router(router)

    # Host to recive
    try:
        while True:
            try:
                # Nhận nguyên cục raw_data (tối đa 65535 bytes) từ OS
                raw_data, addr = sock.recvfrom(MAX_DATAGRAM_SIZE)

                # Quăng ngay vào cho StreamManager xử lý
                stream_manager.handle_incoming_datagram(raw_data, addr)

            except ConnectionResetError:
                # Fix WINERROR 10054:
                pass
            except Exception as e:
                print(f"[Server Error] Lỗi ngoại lệ vòng lặp UDP: {e}")

    except KeyboardInterrupt:
        print("\nĐang tắt Server...")
    finally:
        sock.close()


if __name__ == "__main__":
    start_server()
