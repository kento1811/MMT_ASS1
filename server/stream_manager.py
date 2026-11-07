import threading

# Giả sử bạn có các class này từ thư mục dự án
from shared.constants import TCPFlags
from shared.packet import Packet
from shared.packet_service import PacketService
from shared.exceptions import ConnectionTimeoutError
from shared.security import SecureStream


class StreamManager:
    def __init__(self, state_manager, udp_socket):
        """
        - state_manager: Để dọn dẹp (logout) khi client rớt mạng
        - udp_socket: Raw socket (đã bind ở main.py) dùng chung cho mọi client
        """
        self.state_manager = state_manager
        self.sock = udp_socket
        self.router = None  # Sẽ được set thông qua dependency injection

        # Bảng định tuyến nội bộ: map từ (IP, Port) -> instance của PacketService
        self.connections = {}
        self.lock = threading.Lock()
        self.channels = {} 

    def set_router(self, router):
        """Gắn Router vào sau khi khởi tạo để tránh Circular Import"""
        self.router = router

    # ==========================================
    # 1. XỬ LÝ GÓI TIN ĐẾN (Được gọi liên tục bởi main.py)
    # ==========================================
    def handle_incoming_datagram(self, raw_data: bytes, addr: tuple):
        with self.lock:
            # Lấy thẳng svc đã lưu lúc accept
            svc = self.connections.get(addr)

        if svc:
            # Ném datagram cho lớp TCP (PacketService) bên dưới xử lý
            svc.feed_datagram(raw_data, addr)
        else:
            # Phân tích SYN nếu là khách lạ
            try:
                packet, _ = Packet.unpack(raw_data)
                if packet.flags & TCPFlags.SYN:
                    self._accept_new_connection(addr, packet)
                else:
                    print(f"[StreamManager] Bỏ qua gói tin không phải SYN từ khách lạ {addr}")
            except Exception:
                pass

    def _accept_new_connection(self, addr: tuple, syn_packet: Packet):
        print(f"[StreamManager] Đang thiết lập kết nối với {addr}...")
        from server.config import PORT

        # 1. Khởi tạo PacketService cho Server PHẢI CÓ đầy đủ Port
        svc = PacketService(
            sock=self.sock, shared_socket=True, src_port=PORT, dst_port=addr[1]
        )
        svc.peer_address = addr

        import random

        local_seq = random.randint(0, 65535)

        # 2. QUAN TRỌNG: Gắn đầy đủ Port vào gói SYN-ACK
        try:
            syn_ack_packet = Packet(
                src_port=PORT,
                dst_port=addr[1],
                seq_num=local_seq,
                ack_num=syn_packet.seq_num + 1,
                window_size=65535,
            )

            # Sử dụng toán tử | (Bitwise OR) để gộp cờ SYN và ACK
            syn_ack_packet.flags = TCPFlags.SYN | TCPFlags.ACK

            self.sock.sendto(syn_ack_packet.pack(), addr)

        except Exception as e:
            print(f"[StreamManager] Lỗi tạo gói SYN-ACK: {e}")
            return

        # 3. Kích hoạt kết nối phía Server
        try:
            # Hãy truyền các tham số đúng với hàm establish_server_side mà bạn đã code
            # (Bạn có thể cần thêm tham số peer_window=syn_packet.window_size nếu hàm yêu cầu)
            svc.establish_server_side(
                peer_address=addr,
                peer_port=addr[1],
                local_next_seq=local_seq + 1,
                peer_next_seq=syn_packet.seq_num + 1,
                peer_window=syn_packet.window_size,
            )
        except Exception as e:
            print(f"[StreamManager] Lỗi establish: {e}")

        # 4. Lưu lại session và bật luồng lắng nghe
        chan = SecureStream(svc, is_server=True, psk=b"HUDP_SECRET_KEY")
        with self.lock:
            self.connections[addr] = svc
            self.channels[addr] = chan

        import threading

        threading.Thread(
            target=self._client_read_worker,
            args=(addr, chan),
            daemon=True,
            name=f"Reader-{addr[1]}",
        ).start()

    # ==========================================
    # 2. LUỒNG LẮNG NGHE DỮ LIỆU TỪNG CLIENT
    # ==========================================
    def _client_read_worker(self, addr: tuple, chan: SecureStream):
        """
        Luồng chạy ngầm riêng cho mỗi client.
        Chờ lấy data hoàn chỉnh từ PacketService và ném cho Router.
        """
        try:
            print(f"[StreamManager] Đang bắt tay bảo mật với {addr}...")
            chan.handshake(timeout=10) 
            print(f"[StreamManager] Bắt tay bảo mật thành công với {addr}!")
            while True:
                # Hàm này sẽ block chờ tới khi có 1 chunk dữ liệu IN-ORDER
                data = chan.recv_message()

                if data is None:
                    # Nhận được cờ FIN từ Client -> Đóng kết nối an toàn (EOF)
                    print(f"[StreamManager] Client {addr} đóng kết nối (EOF).")
                    break

                if data and self.router:
                    # Chuyển data (chuỗi JSON bytes) sang cho Router xử lý
                    # Vì đang ở luồng riêng, Router xử lý lâu cũng không sao!
                    self.router.process_message(sender_address=addr, payload=data)

        except ConnectionTimeoutError:
            print(f"[StreamManager] Mất kết nối mạng hoặc Timeout với {addr}")
        except ConnectionResetError:
            print(f"[StreamManager] Client {addr} ngắt kết nối đột ngột (RST)")
        except Exception as e:
            print(f"[StreamManager] Lỗi luồng đọc {addr}: {e}")
        finally:
            # Bất kể đóng an toàn hay rớt mạng, luôn dọn dẹp rác
            self._cleanup_connection(addr, chan)

    # ==========================================
    # 3. GỬI DỮ LIỆU (Được gọi bởi Router/Handlers)
    # ==========================================
    def send_to(self, addr: tuple, data: bytes):
        """
        Router nhờ gửi bytes tới một address.
        """
        with self.lock:
            chan = self.channels.get(addr)

        if chan:
            try:
                # Hàm send() của PacketService sẽ chia nhỏ data thành MSS,
                # gửi và chờ ACK. (Blocking call)
                chan.send_message(data)
            except Exception as e:
                err_msg = str(e)
                # CHỈ IN RA nếu đó là một lỗi lạ. 
                if "chưa được thiết lập" not in err_msg and "CLOSED" not in err_msg:
                    print(f"[StreamManager] Lỗi gửi data tới {addr}: {e}")
        else:
            print(f"[StreamManager] Không tìm thấy connection của {addr} để gửi.")

    # ==========================================
    # 4. DỌN DẸP
    # ==========================================
    def _cleanup_connection(self, addr: tuple, svc: PacketService):
        """Dọn dẹp hệ thống khi Client rời đi"""
        # 1. Báo cho PacketService đóng luồng nội bộ (nếu chưa đóng)
        try:
            svc.close(timeout=0.1)
        except:
            pass

        # 2. Xóa khỏi StreamManager
        with self.lock:
            self.connections.pop(addr, None)
            chan = self.channels.pop(addr, None)

        if chan:
            try:
                chan.close()
            except:
                pass

        # 3. Báo cho StateManager "Đăng xuất" user này để user khác biết
        logged_out_user = self.state_manager.logout_by_address(addr)
        if logged_out_user:
            print(f"[StreamManager] Đã dọn dẹp xong session của user '{logged_out_user}'.")
            
            # --- THÊM MỚI: BÁO CHO CÁC CLIENT KHÁC CẬP NHẬT LẠI DANH SÁCH ---
            all_users = self.state_manager.get_all_users_online()
            import json
            update_bytes = json.dumps({"action": "USER_LIST", "users": all_users}).encode('utf-8')
            
            # Lặp qua các channel đang còn sống để gửi danh sách mới
            with self.lock:
                for target_addr, target_chan in list(self.channels.items()):
                    try:
                        target_chan.send_message(update_bytes)
                    except:
                        pass
