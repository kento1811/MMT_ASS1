"""Mux/Demux: nhiều kết nối PacketService dùng chung MỘT UDP socket của server.

    client A ─┐                      ┌─> PacketService(A)  ─┐
    client B ─┼─> UDP socket :port ──┤   PacketService(B)  ─┼─> accept()
    client C ─┘   (demux theo addr)  └─> PacketService(C)  ─┘
                  <───── mux: các service tự sendto() trên cùng socket ─────

- Demux: 1 thread đọc socket, tra bảng (ip, port) -> PacketService rồi
  feed_datagram(). Datagram của peer lạ bị trả RST (trừ khi chính nó là RST).
- Mux: các PacketService (shared_socket=True) gọi sock.sendto() trực tiếp
  (sendto thread-safe) nên không cần hàng đợi gửi riêng.
- Passive open: nhận SYN -> gửi SYN-ACK -> tạo PacketService ->
  establish_server_side() -> đưa vào hàng đợi accept().
  SYN trùng (SYN-ACK bị mất) -> gửi lại đúng SYN-ACK đã cache.
"""

import random
import socket
import threading
import time
from queue import Queue, Empty

from packet import Packet
from constants import TCPFlags
from packet_service import PacketService


class ServerMux:
    FAILED_GRACE = 10.0

    def __init__(self, sock, max_connections=64, reap_interval=1.0):
        self.sock = sock
        self.local_port = sock.getsockname()[1]
        self.max_connections = max_connections
        self.reap_interval = reap_interval

        self._conns = {}          # addr -> {"svc", "client_isn", "synack"}
        self._lock = threading.RLock()
        self._accept_q = Queue()
        self._running = False
        self._rx = None
        self._reaper = None

    # ------------------------------------------------------------ lifecycle --
    def start(self):
        if self._running:
            return
        self._running = True
        self.sock.settimeout(0.2)
        self._rx = threading.Thread(target=self._demux_loop, daemon=True,
                                    name="mux-recv")
        self._reaper = threading.Thread(target=self._reap_loop, daemon=True,
                                        name="mux-reaper")
        self._rx.start()
        self._reaper.start()

    def stop(self, close_connections=True):
        """Dừng demux. Không đóng socket (caller sở hữu socket)."""
        self._running = False
        if close_connections:
            for addr in list(self._conns):
                self.remove(addr, close=True)
        for t in (self._rx, self._reaper):
            if t is not None and t is not threading.current_thread():
                t.join(timeout=1)

    # ------------------------------------------------------------- public ----
    def accept(self, timeout=None):
        """Trả (PacketService, addr) của kết nối mới, hoặc None nếu hết timeout."""
        try:
            return self._accept_q.get(timeout=timeout)
        except Empty:
            return None

    def get(self, addr):
        with self._lock:
            entry = self._conns.get(addr)
            return entry["svc"] if entry else None

    def connections(self):
        """[(addr, PacketService)] — để server liệt kê client/IP/port đang nối."""
        with self._lock:
            return [(a, e["svc"]) for a, e in self._conns.items()]

    def remove(self, addr, close=False):
        with self._lock:
            entry = self._conns.pop(addr, None)
        if entry is not None and close:
            self._kill(entry["svc"])

    # ------------------------------------------------------------ internals --
    @staticmethod
    def _kill(svc):
        """Dừng service ngay (không handshake FIN) — dùng cho dọn dẹp."""
        svc.running = False
        svc.state = "CLOSED"
        with svc.condition:
            svc.condition.notify_all()

    def _sendto(self, packet, addr):
        try:
            self.sock.sendto(packet.pack(), addr)
        except OSError:
            pass

    def _demux_loop(self):
        while self._running:
            try:
                raw, addr = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except ConnectionResetError:     # Windows: ICMP unreachable
                continue
            except OSError:
                break
            try:
                self._dispatch(raw, addr)
            except Exception:                # 1 datagram lỗi không giết mux
                continue

    def _dispatch(self, raw, addr):
        try:
            packet, _ = Packet.unpack(raw)   # checksum sai/thiếu header -> drop
        except Exception:
            return
        if packet.dst_port != self.local_port:
            return

        if packet.flags & TCPFlags.SYN and not packet.flags & TCPFlags.ACK:
            self._handle_syn(packet, addr)
            return

        with self._lock:
            entry = self._conns.get(addr)
        if entry is not None and entry["svc"].src_port == packet.dst_port:
            entry["svc"].feed_datagram(raw, addr)
            return

        # Peer không có kết nối: RST (không trả RST cho RST để tránh vòng lặp).
        if not packet.flags & TCPFlags.RST:
            self._send_rst(packet, addr)

    def _send_rst(self, packet, addr):
        self._sendto(Packet(
            src_port=self.local_port, dst_port=packet.src_port,
            seq_num=packet.ack_num, ack_num=0,
            flags=TCPFlags.RST, window_size=0), addr)

    def _handle_syn(self, syn, addr):
        with self._lock:
            entry = self._conns.get(addr)
            if entry is not None:
                if entry["client_isn"] == syn.seq_num and \
                        entry["svc"].state in ("ESTABLISHED", "SYN_RCVD"):
                    # SYN-ACK bị mất -> gửi lại đúng gói cũ.
                    self._sendto(entry["synack"], addr)
                    return
                # Cùng addr nhưng ISN khác: client khởi động lại -> bỏ kết nối cũ.
                self._conns.pop(addr, None)
                self._kill(entry["svc"])

            live = sum(1 for e in self._conns.values()
                       if e["svc"].state not in ("CLOSED", "FAILED"))
            if live >= self.max_connections:
                self._send_rst(syn, addr)    # server đầy
                return

            svc = PacketService(self.sock, self.local_port, syn.src_port,
                                shared_socket=True)
            server_isn = svc.seq_num
            synack = Packet(
                src_port=self.local_port, dst_port=syn.src_port,
                seq_num=server_isn, ack_num=syn.seq_num + 1,
                flags=TCPFlags.SYN | TCPFlags.ACK, window_size=65535)

            self._conns[addr] = {"svc": svc, "client_isn": syn.seq_num,
                                 "synack": synack}
            # Đăng ký vào bảng TRƯỚC khi gửi SYN-ACK: ACK/data đầu của client
            # có thể tới ngay sau đó.
            svc.establish_server_side(
                peer_address=addr, peer_port=syn.src_port,
                local_next_seq=server_isn + 1, peer_next_seq=syn.seq_num + 1,
                peer_window=syn.window_size)
            self._sendto(synack, addr)
        self._accept_q.put((svc, addr))

    def _reap_loop(self):
        """Gỡ khỏi bảng các kết nối đã chết để addr/port được dùng lại."""
        while self._running:
            time.sleep(self.reap_interval)
            with self._lock:
                items = list(self._conns.items())
            for addr, entry in items:
                svc = entry["svc"]
                if svc.state == "FAILED":
                    # Cho app 10s để thấy lỗi FAILED (ConnectionTimeoutError)
                    # trước khi dọn, nếu không nó sẽ nhận lỗi "reset" sai loại.
                    failed_at = entry.setdefault("failed_at", time.monotonic())
                    if time.monotonic() - failed_at > self.FAILED_GRACE:
                        self._kill(svc)
                if svc.state == "CLOSED":
                    with self._lock:
                        if self._conns.get(addr) is entry:
                            del self._conns[addr]