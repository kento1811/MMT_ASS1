
import socket
import threading
import time
from queue import Queue, Empty

from shared.packet import Packet
from shared.constants import TCPFlags
from shared.packet_service import PacketService
from shared.algorithms import MAX_SEQ


class ServerMux:
    # Thời gian chờ trước khi giải phóng một kết nối bị lỗi
    FAILED_GRACE = 10.0

    def __init__(self, sock, max_connections=64, reap_interval=1.0):
        self.sock = sock
        self.local_port = sock.getsockname()[1]
        self.max_connections = max_connections
        self.reap_interval = reap_interval

        # Quản lý các kết nối theo địa chỉ client
        # Mỗi entry chứa PacketService, ISN của client và gói SYN-ACK đã tạo
        self._conns = {}
        self._lock = threading.RLock()

        # Hàng đợi các kết nối đã được chấp nhận để ứng dụng gọi accept()
        self._accept_q = Queue()

        self._running = False
        self._rx = None
        self._reaper = None

    def start(self):
        # Tránh khởi động nhiều lần
        if self._running:
            return

        self._running = True

        # Đặt timeout để luồng nhận có thể kiểm tra trạng thái dừng định kỳ
        self.sock.settimeout(0.2)

        # Luồng nhận và phân phối datagram đến từng kết nối
        self._rx = threading.Thread(
            target=self._demux_loop,
            daemon=True,
            name="mux-recv"
        )

        # Luồng định kỳ dọn dẹp các kết nối đã đóng hoặc bị lỗi
        self._reaper = threading.Thread(
            target=self._reap_loop,
            daemon=True,
            name="mux-reaper"
        )

        self._rx.start()
        self._reaper.start()

    def stop(self, close_connections=True):
        # Yêu cầu các luồng nền dừng hoạt động
        self._running = False

        # Đóng tất cả kết nối đang được quản lý nếu được yêu cầu
        if close_connections:
            for addr in list(self._conns):
                self.remove(addr, close=True)

        # Chờ các luồng nền kết thúc
        for t in (self._rx, self._reaper):
            if t is not None and t is not threading.current_thread():
                t.join(timeout=1)

    def accept(self, timeout=None):
        # Lấy một kết nối mới từ hàng đợi
        # Trả về None nếu hết thời gian chờ
        try:
            return self._accept_q.get(timeout=timeout)
        except Empty:
            return None

    def get(self, addr):
        # Lấy PacketService tương ứng với địa chỉ client
        with self._lock:
            entry = self._conns.get(addr)
            return entry["svc"] if entry else None

    def connections(self):
        # Trả về danh sách (địa chỉ, PacketService) của các kết nối
        with self._lock:
            return [(a, e["svc"]) for a, e in self._conns.items()]

    def remove(self, addr, close=False):
        # Xóa kết nối khỏi danh sách quản lý
        with self._lock:
            entry = self._conns.pop(addr, None)

        # Nếu yêu cầu đóng, giải phóng PacketService tương ứng
        if entry is not None and close:
            self._kill(entry["svc"])

    @staticmethod
    def _kill(svc):
        # Chuyển kết nối về trạng thái CLOSED
        svc.running = False
        svc.state = "CLOSED"

        # Đánh thức các luồng đang chờ dữ liệu hoặc ACK
        with svc.condition:
            svc.condition.notify_all()

    def _sendto(self, packet, addr):
        # Đóng gói Packet và gửi qua UDP socket dùng chung
        try:
            self.sock.sendto(packet.pack(), addr)
        except OSError:
            pass

    def _demux_loop(self):
        # Luồng nhận UDP duy nhất của ServerMux
        while self._running:
            try:
                raw, addr = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except ConnectionResetError:
                continue
            except OSError:
                break

            # Phân phối datagram đến kết nối tương ứng
            try:
                self._dispatch(raw, addr)
            except Exception:
                continue

    def _dispatch(self, raw, addr):
        # Giải mã datagram thành Packet
        try:
            packet, _ = Packet.unpack(raw)
        except Exception:
            return

        # Bỏ qua packet không gửi đến cổng của server
        if packet.dst_port != self.local_port:
            return

        # Packet SYN không có ACK biểu thị yêu cầu thiết lập kết nối mới
        if packet.flags & TCPFlags.SYN and not packet.flags & TCPFlags.ACK:
            self._handle_syn(packet, addr)
            return

        # Tìm kết nối tương ứng với địa chỉ nguồn
        with self._lock:
            entry = self._conns.get(addr)

        # Nếu tìm thấy kết nối hợp lệ, chuyển datagram cho PacketService xử lý
        if entry is not None and entry["svc"].src_port == packet.dst_port:
            entry["svc"].feed_datagram(raw, addr)
            return

        # Không tìm thấy kết nối thì gửi RST để từ chối
        if not packet.flags & TCPFlags.RST:
            self._send_rst(packet, addr)

    def _send_rst(self, packet, addr):
        # Gửi RST thông báo kết nối không tồn tại hoặc bị từ chối
        self._sendto(Packet(
            src_port=self.local_port,
            dst_port=packet.src_port,
            seq_num=packet.ack_num,
            ack_num=0,
            flags=TCPFlags.RST,
            window_size=0
        ), addr)

    def _handle_syn(self, syn, addr):
        # Kiểm tra xem client đã có kết nối được quản lý hay chưa
        with self._lock:
            entry = self._conns.get(addr)

            if entry is not None:
                # Client gửi lại SYN cũ khi SYN-ACK có thể đã bị mất
                if (
                    entry["client_isn"] == syn.seq_num
                    and entry["svc"].state in ("ESTABLISHED", "SYN_RCVD")
                ):
                    # Gửi lại SYN-ACK đã lưu thay vì tạo kết nối mới
                    self._sendto(entry["synack"], addr)
                    return

                # SYN mới không khớp kết nối cũ: giải phóng kết nối cũ
                self._conns.pop(addr, None)
                self._kill(entry["svc"])

            # Đếm số kết nối còn hoạt động
            live = sum(
                1 for e in self._conns.values()
                if e["svc"].state not in ("CLOSED", "FAILED")
            )

            # Từ chối kết nối nếu server đã đạt giới hạn
            if live >= self.max_connections:
                self._send_rst(syn, addr)
                return

            # Tạo PacketService mới cho client
            # shared_socket=True vì tất cả kết nối dùng chung UDP socket
            svc = PacketService(
                self.sock,
                self.local_port,
                syn.src_port,
                shared_socket=True
            )

            # Lưu sequence number ban đầu của server
            server_isn = svc.seq_num

            # Tạo gói SYN-ACK để phản hồi SYN của client
            synack = Packet(
                src_port=self.local_port,
                dst_port=syn.src_port,
                seq_num=server_isn,
                ack_num=(syn.seq_num + 1) % MAX_SEQ,
                flags=TCPFlags.SYN | TCPFlags.ACK,
                window_size=65535
            )

            # Lưu thông tin kết nối để xử lý các packet tiếp theo
            self._conns[addr] = {
                "svc": svc,
                "client_isn": syn.seq_num,
                "synack": synack
            }

            # Khởi tạo trạng thái kết nối phía server
            svc.establish_server_side(
                peer_address=addr,
                peer_port=syn.src_port,
                local_next_seq=(server_isn + 1) % MAX_SEQ,
                peer_next_seq=(syn.seq_num + 1) % MAX_SEQ,
                peer_window=syn.window_size
            )

            # Gửi SYN-ACK đến client
            self._sendto(synack, addr)

            # Đưa kết nối vào hàng đợi để ứng dụng nhận bằng accept()
            self._accept_q.put((svc, addr))

    def _reap_loop(self):
        # Luồng dọn dẹp kết nối chạy định kỳ
        while self._running:
            time.sleep(self.reap_interval)

            # Tạo bản sao danh sách kết nối để tránh giữ lock quá lâu
            with self._lock:
                items = list(self._conns.items())

            for addr, entry in items:
                svc = entry["svc"]

                # Nếu kết nối bị lỗi, bắt đầu tính thời gian chờ giải phóng
                if svc.state == "FAILED":
                    failed_at = entry.setdefault(
                        "failed_at",
                        time.monotonic()
                    )

                    # Đóng kết nối nếu đã vượt quá thời gian chờ
                    if time.monotonic() - failed_at > self.FAILED_GRACE:
                        self._kill(svc)

                # Xóa kết nối đã đóng khỏi danh sách quản lý
                if svc.state == "CLOSED":
                    with self._lock:
                        # Kiểm tra entry vẫn là kết nối hiện tại trước khi xóa
                        if self._conns.get(addr) is entry:
                            del self._conns[addr]
