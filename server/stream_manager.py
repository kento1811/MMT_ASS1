import threading
import time

# Giả sử bạn có các class này từ thư mục dự án
from shared.constants import TCPFlags
from shared.packet import Packet
from shared.packet_service import PacketService
from shared.exceptions import ConnectionTimeoutError
from shared.security import SecureStream
from shared.algorithms import MAX_SEQ


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
        self.lock = threading.RLock()
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
            # Phân tích gói lạ
            try:
                packet, _ = Packet.unpack(raw_data)
                
                # CHỐNG LỖI MUX/DEMUX:
                # 1. Nếu không phải gói SYN thuần túy -> Bỏ qua lặng lẽ (chống quét port / DDoS rác)
                if not (packet.flags & TCPFlags.SYN) or (packet.flags & TCPFlags.ACK):
                    return
                    
                # 2. Xử lý SYN (Bắt tay mới)
                self._accept_new_connection(addr, packet)
                
            except Exception:
                pass # Lặng lẽ nuốt lỗi giải nén (sai checksum)

    def _accept_new_connection(self, addr: tuple, syn_packet: Packet):
        from server.config import PORT, MAX_CLIENTS
        
        # CHỐNG LỖI MUX/DEMUX: Bảo vệ Max Connections
        with self.lock:
            if len(self.connections) >= MAX_CLIENTS:
                # Trả RST báo bận
                rst = Packet(
                    src_port=PORT, dst_port=syn_packet.src_port,
                    seq_num=syn_packet.ack_num, ack_num=0,
                    flags=TCPFlags.RST, window_size=0
                )
                try:
                    self.sock.sendto(rst.pack(), addr)
                except OSError:
                    pass
                return

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
                ack_num=(syn_packet.seq_num + 1) % MAX_SEQ, # Chống lỗi wrap-around 32-bit
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
            # Truyền các tham số đúng với hàm establish_server_side
            svc.establish_server_side(
                peer_address=addr,
                peer_port=addr[1],
                local_next_seq=(local_seq + 1) % MAX_SEQ,
                peer_next_seq=(syn_packet.seq_num + 1) % MAX_SEQ,
                peer_window=syn_packet.window_size,
            )
        except Exception as e:
            print(f"[StreamManager] Lỗi establish: {e}")
            return

        # 4. Lưu lại session và bật luồng lắng nghe
        chan = SecureStream(svc, is_server=True, psk=b"HUDP_SECRET_KEY")
        with self.lock:
            # CHỐNG LỖI MUX/DEMUX: Xóa luồng cũ nếu Client crash rồi khởi động lại ngay lập tức trùng IP/Port
            if addr in self.connections:
                old_svc = self.connections.pop(addr)
                try:
                    old_svc.close(timeout=0)
                except:
                    pass
                    
            self.connections[addr] = svc
            self.channels[addr] = chan

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
            chan.handshake(timeout=10) 
            while True:
                # Hàm này sẽ block chờ tới khi có 1 chunk dữ liệu IN-ORDER
                data = chan.recv_message()

                if data is None:
                    # Nhận được cờ FIN từ Client -> Đóng kết nối an toàn (EOF)
                    break

                if data and self.router:
                    # Chuyển data (chuỗi JSON bytes) sang cho Router xử lý
                    # Vì đang ở luồng riêng, Router xử lý lâu cũng không sao!
                    self.router.process_message(sender_address=addr, payload=data)

        except ConnectionTimeoutError:
            pass # Lặng lẽ đóng
        except ConnectionResetError:
            pass # Lặng lẽ đóng
        except Exception:
            pass # Lặng lẽ đóng
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
            threading.Thread(
                target=self._async_send_worker, 
                args=(chan, addr, data), 
                daemon=True
            ).start()
        else:
            pass

    # ==========================================
    # 4. DỌN DẸP
    # ==========================================
    def _cleanup_connection(self, addr: tuple, chan: SecureStream):
        """Dọn dẹp hệ thống khi Client rời đi"""
        # 1. Xóa khỏi StreamManager để không ai gửi tin tới nữa
        with self.lock:
            svc = self.connections.pop(addr, None)
            chan_in_dict = self.channels.pop(addr, None)

        # 2. Báo cho PacketService đóng luồng nội bộ (nếu chưa đóng)
        if svc:
            try:
                svc.close(timeout=0.1)
            except:
                pass

        if chan:
            try:
                chan.close()
            except:
                pass

        # 3. Báo cho StateManager "Đăng xuất" user này để user khác biết
        logged_out_user = self.state_manager.logout_by_address(addr)
        if logged_out_user:
            # --- BÁO CHO CÁC CLIENT KHÁC CẬP NHẬT LẠI DANH SÁCH ---
            all_users = self.state_manager.get_all_users_online()
            import json
            update_bytes = json.dumps({"action": "USER_LIST", "users": all_users}).encode('utf-8')
            
            with self.lock:
                active_channels = list(self.channels.keys()) 
            
            for target_addr in active_channels:
                self.send_to(target_addr, update_bytes)

    def _async_send_worker(self, chan, addr, data):
        try:
            # Hàm send() của PacketService sẽ chia nhỏ data thành MSS,
            # gửi và chờ ACK. (Chỉ block luồng ngầm này, không block Server)
            chan.send_message(data)
        except Exception as e:
            err_msg = str(e).lower()
            # Ẩn hoàn toàn các lỗi liên quan đến việc peer ngắt kết nối khi đang gửi dở
            if "chưa được thiết lập" not in err_msg and "closed" not in err_msg and "đóng" not in err_msg and "failed" not in err_msg:
                print(f"[StreamManager] Lỗi gửi data tới {addr}: {e}")