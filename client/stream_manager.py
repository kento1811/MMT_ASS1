import socket
import threading
import json
from shared.packet_service import PacketService
from shared.exceptions import ConnectionTimeoutError
from shared.security import  SecureStream
from client.config import SERVER_IP, SERVER_PORT

class ClientStreamManager:
    def __init__(self, message_callback):
        self.svc = None
        self.sock = None
        self.message_callback = message_callback
        self.is_connected = False
        self.chan = None

    # Method kết nối với Backend qua 3-way handshake
    def connect(self) -> bool:
        if not self.sock:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.bind(("0.0.0.0", 0))
        local_port = self.sock.getsockname()[1]
        
        print(f"Đang chạy Client tại port {local_port}...")
        # Tạo agent xử lý packet đại diện client
        self.svc = PacketService(
            sock=self.sock,
            shared_socket=False, 
            src_port=local_port, 
            dst_port=SERVER_PORT
        )
        
        print("Đang bắt tay (Handshake) với Server...")
        if self.svc.connect((SERVER_IP, SERVER_PORT)):
            self.is_connected = True
            print("Đang thiết lập khóa bảo mật mã hóa đầu cuối...")
            self.chan = SecureStream(self.svc, is_server=False, psk=b"HUDP_SECRET_KEY")
            try:
                self.chan.handshake(timeout=5)
                print("Bảo mật thành công!")
            except Exception as e:
                print(f"Lỗi bảo mật: {e}")
                return False
            # Bật luồng đọc ngầm ngay khi kết nối thành công
            threading.Thread(target=self._receive_worker, daemon=True).start()
            return True
        return False

    def send_json(self, pydantic_obj):
        """Mã hóa Pydantic model thành chuỗi JSON bytes và gửi đi"""
        if self.is_connected:
            self.chan.send_message(pydantic_obj.model_dump_json().encode('utf-8'))

    def _receive_worker(self):
        """Luồng ngầm liên tục lắng nghe và parse JSON"""
        while self.is_connected:
            try:
                data = self.chan.recv_message(timeout=0.5)
                if data is None:
                    self.message_callback({"action": "SYSTEM", "msg": "Server đã đóng kết nối."})
                    self.is_connected = False
                    break
                    
                if data:
                    try:
                        msg_obj = json.loads(data.decode('utf-8'))
                        self.message_callback(msg_obj) # Đẩy data lên tầng giao diện
                    except json.JSONDecodeError:
                        self.message_callback({"action": "SYSTEM", "msg": "Lỗi parse JSON từ Server"})
                        
            except ConnectionTimeoutError:
                pass # Bỏ qua timeout, tiếp tục lặp
            except Exception as e:
                self.message_callback({"action": "SYSTEM", "msg": f"Lỗi luồng đọc: {e}"})
                self.is_connected = False
                break

    def close(self):
        self.is_connected = False
        if self.chan:
            self.chan.close()
        if self.svc:
            self.svc.close(timeout=2)
        if self.sock:
            self.sock.close()