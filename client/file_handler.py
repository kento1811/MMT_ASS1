import os
import socket
import threading
import random
from shared.packet import Packet
from shared.packet_service import PacketService
from shared.constants import TCPFlags
from shared.exceptions import ConnectionTimeoutError
from shared.security import SecureStream
from client.ui_bridge import MESSAGE_QUEUE

class FileHandler:
    def __init__(self, download_dir="downloads"):
        """Tự động tạo thư mục lưu file tải về nếu chưa có"""
        self.download_dir = download_dir
        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

    # ==========================================
    # LOGIC NGƯỜI NHẬN (MỞ PORT & CHỜ KẾT NỐI)
    # ==========================================
    def start_receiving(self, filename: str) -> int:
        """
        Mở một UDP port ngẫu nhiên và chạy luồng lắng nghe.
        Trả về port để Client gửi thông báo cho Server (thông qua session_id).
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", 0))
        local_port = sock.getsockname()[1]
        
        threading.Thread(
            target=self._receive_file_worker, 
            args=(sock, local_port, filename),
            daemon=True
        ).start()
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('8.8.8.8', 1))
            local_ip = s.getsockname()[0]
        except Exception:
            local_ip = '127.0.0.1'
        finally:
            s.close()

        return local_ip, local_port

    def _receive_file_worker(self, sock: socket.socket, local_port: int, filename: str):
        print(f"\n[File] Đang chờ người gửi kết nối P2P tại port {local_port}...\n> ", end="")
        sock.settimeout(30.0) # Nếu sau 30s người gửi không connect thì hủy
        
        try:
            # 1. Bắt gói SYN từ người gửi
            raw_data, addr = sock.recvfrom(65535)
            packet, _ = Packet.unpack(raw_data)
            
            if not (packet.flags & TCPFlags.SYN):
                return
                
            # 2. Tạo gói SYN-ACK để đồng ý kết nối P2P
            local_seq = random.randint(0, 65535)
            syn_ack = Packet(
                src_port=local_port, dst_port=addr[1],
                seq_num=local_seq, ack_num=packet.seq_num + 1,
                flags=TCPFlags.SYN | TCPFlags.ACK
            )
            sock.sendto(syn_ack.pack(), addr)
            
            # 3. Kích hoạt cỗ máy TCP-over-UDP (PacketService)
            svc = PacketService(
                sock=sock,
                shared_socket=False, # Độc quyền socket này
                src_port=local_port,
                dst_port=addr[1]
            )
            svc.establish_server_side(
                peer_address=addr,
                peer_port=addr[1],
                local_next_seq=local_seq + 1,
                peer_next_seq=packet.seq_num + 1,
                peer_window=packet.window_size
            )
            chan = SecureStream(svc, is_server=True, psk=b"P2P_FILE_SECRET")
            chan.handshake(timeout=10)
            # 4. Bắt đầu hứng file
            filepath = os.path.join(self.download_dir, filename)
            received_bytes = 0
            
            with open(filepath, "wb") as f:
                while True:
                    try:
                        # Cứ chờ tối đa 5s cho mỗi chunk
                        data = chan.recv_message(timeout=5.0) 
                        if data is None: # Nhận được cờ FIN -> EOF
                            print(f"\n[File] ✅ Đã tải xong '{filename}'! Tổng: {received_bytes} bytes.\n> ", end="")
                            break
                            
                        f.write(data)
                        received_bytes += len(data)
                        
                    except ConnectionTimeoutError:
                        print(f"\n[File] ❌ Lỗi: Timeout khi đang tải '{filename}'.\n> ", end="")
                        break
                    except ConnectionResetError:
                        print(f"\n[File] ❌ Lỗi: Người gửi ngắt kết nối.\n> ", end="")
                        break
                        
            # 5. Dọn dẹp
            chan.close()
            sock.close()
            
        except Exception as e:
            print(f"\n[File] Lỗi luồng nhận file: {e}\n> ", end="")
            sock.close()


    # ==========================================
    # LOGIC NGƯỜI GỬI (CONNECT VÀ BĂM FILE ĐẨY ĐI)
    # ==========================================
    def send_file(self, target_ip: str, target_port: int, filepath: str):
        """Kích hoạt luồng đẩy data file tới địa chỉ của người nhận"""
        threading.Thread(
            target=self._send_file_worker,
            args=(target_ip, target_port, filepath),
            daemon=True
        ).start()

    def _send_file_worker(self, target_ip: str, target_port: int, filepath: str):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.bind(("0.0.0.0", 0))
            
            svc = PacketService(
                sock=sock,
                shared_socket=False,
                src_port=sock.getsockname()[1],
                dst_port=target_port
            )
            
            print(f"\n[File] Đang kết nối P2P tới người nhận...\n> ", end="")
            if not svc.connect((target_ip, target_port), timeout=2, max_retries=5):
                print("\n[File] Người nhận không phản hồi.\n> ", end="")
                return
                
            print(f"\n[File] Kết nối P2P thành công! Đang đẩy file...\n> ", end="")
            
            # Đọc file nhị phân và gửi theo từng Chunk 1024 bytes
            chunk_size = 1024
            sent_bytes = 0
            filename = os.path.basename(filepath)

            chan = SecureStream(svc, is_server=False, psk=b"P2P_FILE_SECRET")
            chan.handshake(timeout=10)

            total_size = os.path.getsize(filepath)
            with open(filepath, "rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    # Hàm send() sẽ tự block đợi ACK rồi mới gửi chunk tiếp
                    chan.send_message(chunk)
                    sent_bytes += len(chunk)

                    percent = int((sent_bytes / total_size) * 100)
                    MESSAGE_QUEUE.put({
                        "action": "FILE_PROGRESS", 
                        "filename": filename, 
                        "percent": percent,
                        "type": "upload"
                    })
                    
            print(f"\n[File] ✅ Đã gửi xong '{filename}'! Tổng: {sent_bytes} bytes.\n> ", end="")
            svc.close(timeout=2)
            chan.close()
            sock.close()
            
        except Exception as e:
            print(f"\n[File] ❌ Lỗi gửi file: {e}\n> ", end="")