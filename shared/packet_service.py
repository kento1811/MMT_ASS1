"""PacketService — "TCP tự cài đặt" chạy trên UDP.

UDP chỉ ném datagram đi: không đảm bảo đến nơi, đúng thứ tự hay không trùng lặp.
Lớp này bổ sung cho MỘT kết nối các tính chất mà ứng dụng cần từ TCP:

    Tính chất                    Hiện thực chính
    ---------------------------  ------------------------------------------------
    Kết nối                      connect / establish_server_side (3-way), close (FIN)
    Tin cậy + đúng thứ tự        seq/ack theo byte, SlidingWindow, retransmit
    Phát hiện mất gói            RTO (một timer) + fast retransmit (3 dup ACK)
    Kiểm soát luồng              window quảng cáo (advertised window)
    Kiểm soát tắc nghẽn          CUBIC + NewReno fast recovery
    Phát hiện peer chết          keepalive + idle timeout

Mô hình luồng (thread) — mỗi kết nối có 2 thread nền:
    * _receive_loop : nhận datagram, kiểm tra hợp lệ rồi gọi _handle_packet.
    * _timer_loop   : mỗi RETRANSMIT_TICK kiểm tra RTO, keepalive, idle, TIME_WAIT.
Ứng dụng gọi send()/receive()/close() từ thread của mình.

Đồng bộ hóa:
    * self.condition (RLock) bảo vệ MỌI trạng thái giao thức (window, cwnd, RTO...).
      Các hàm có hậu tố `_locked` chỉ được gọi khi đang giữ khóa này.
    * self._send_lock làm send() atomic: nhiều thread gọi send() cùng lúc sẽ được
      xếp hàng, byte của các lần gửi không bị xen kẽ.

Số thứ tự là 32-bit và CÓ wrap-around: mọi phép so sánh seq phải dùng các hàm
modular seq_lt / seq_lte / seq_gt / seq_gte / seq_diff (shared/algorithms.py),
không được dùng toán tử < > trực tiếp trên seq/ack.
"""

import random
import socket
import struct
import threading
import time
from queue import Queue, Empty, Full

from shared.packet import Packet
from shared.constants import TCPFlags
from shared.exceptions import ConnectionTimeoutError, ProtocolError, PeerResetError
from shared.algorithms import SlidingWindow, seq_gt, seq_gte, seq_lt, seq_lte, seq_diff, MAX_SEQ


class PacketService:
    """Một kết nối đáng tin cậy trên UDP (hai đầu đối xứng sau khi bắt tay).

    Cách dùng:
        Client: svc = PacketService(sock, src_port, dst_port); svc.connect(addr)
        Server: socket dùng chung -> ServerMux tạo PacketService(shared_socket=True),
                gửi SYN-ACK rồi gọi establish_server_side().
        Sau đó: send(bytes) / receive(timeout) / close().

    Quy ước lỗi của receive(): b"" là EOF bình thường; ConnectionTimeoutError là
    mất mạng/hết giờ (dữ liệu có thể bị cắt); ConnectionResetError / PeerResetError
    là bị đóng bất thường. Với truyền file, caller PHẢI phân biệt ba trường hợp này.
    """

    # ---- Tham số mặc định -------------------------------------------------
    # Có thể ghi đè bằng class attribute trước khi tạo kết nối
    # (ví dụ PacketService.MSS = 1400).

    # MSS: payload tối đa mỗi packet. 1024 + 18 (header) + 28 (IP/UDP) < MTU 1500
    # nên datagram không bị IP phân mảnh.
    MSS = 1024
    # Bộ đệm nhận tối đa = dữ liệu in-order chưa được app đọc + dữ liệu out-of-order.
    # Phần còn trống chính là window mà ta quảng cáo cho peer (flow control).
    RECEIVE_BUFFER_BYTES = 256 * 1024
    # Phát hiện peer chết: gửi ACK rỗng mỗi KEEPALIVE_INTERVAL; không nhận được gì
    # trong IDLE_TIMEOUT thì chuyển sang FAILED. RETRANSMIT_TICK là chu kỳ của timer
    # thread. TIME_WAIT_SECONDS: thời gian ở lại TIME_WAIT sau close() để còn re-ACK
    # FIN nếu ACK cuối của ta bị mất.
    KEEPALIVE_INTERVAL = 5.0
    IDLE_TIMEOUT = 30.0
    RETRANSMIT_TICK = 0.05
    TIME_WAIT_SECONDS = 2.0

    # RTO (retransmission timeout), đơn vị giây: giá trị khởi đầu và biên trên/dưới
    # (theo RFC 6298). Sau khi có mẫu RTT, RTO = SRTT + 4*RTTVAR.
    INITIAL_RTO = 1.0
    MIN_RTO = 0.2
    MAX_RTO = 60.0

    # Congestion window (byte): khởi đầu 4 MSS, tối thiểu 1 MSS, tối đa 64 MSS.
    # MAX_CWND = 64 KB cũng là trần của window quảng cáo (trường window chỉ có 16 bit).
    # DUP_ACK_THRESHOLD: số dup ACK liên tiếp để kích hoạt fast retransmit.
    INITIAL_CWND = 4 * MSS
    MIN_CWND = MSS
    MAX_CWND = 64 * MSS
    DUP_ACK_THRESHOLD = 3

    def __init__(self, sock, src_port, dst_port, shared_socket=False):
        """Tạo kết nối ở trạng thái CLOSED (chưa có window).

        sock          : socket UDP. shared_socket=False -> service tự recvfrom;
                        True -> socket do ServerMux đọc và đưa vào feed_datagram().
        src_port      : cổng của ta (ghi vào header, peer dùng để lọc).
        dst_port      : cổng của peer (server sẽ ghi đè khi establish_server_side).
        """
        self.sock = sock
        self.src_port = src_port
        self.dst_port = dst_port
        self.shared_socket = shared_socket

        # ---- Trạng thái kết nối --------------------------------------------
        # CLOSED -> SYN_SENT -> ESTABLISHED -> TIME_WAIT -> CLOSED
        # FAILED: kết nối chết do mạng (idle timeout / quá số lần gửi lại).
        self.state = "CLOSED"
        # ISN (initial sequence number) 32-bit ngẫu nhiên: giảm khả năng gói cũ của
        # kết nối trước bị nhận nhầm vào kết nối mới. Số thứ tự đếm theo BYTE.
        self.seq_num = random.randint(0, 0xFFFFFFFF)
        self.peer_address = None

        # Hai SlidingWindow chỉ được tạo sau handshake (_init_windows):
        # send_window giữ gói đã gửi chưa ACK; receive_window ráp lại byte theo thứ tự.
        self.send_window = None
        self.receive_window = None
        self.max_retries = 5

        # ---- Đo RTT và RTO (Jacobson/Karels + quy tắc Karn) --------------------
        # sent_at / retry_counts / retransmitted / seq_end_map / start_of_end là sổ
        # sách theo từng segment đang bay; được dọn khi segment được ACK.
        #   sent_at[seq]       : thời điểm gửi lần gần nhất
        #   retransmitted      : segment đã gửi lại (không lấy mẫu RTT từ chúng)
        #   start_of_end[end]  : seq đầu của segment kết thúc ở `end` (tra theo ack_num)
        self.rto = self.INITIAL_RTO
        self.srtt = None
        self.rttvar = None
        self.sent_at = {}
        self.retry_counts = {}
        self.retransmitted = set()
        self.seq_end_map = {}
        self.start_of_end = {}

        # ---- Kiểm soát luồng (flow control) -----------------------------------
        # peer_window        : window peer quảng cáo (byte ta được phép gửi tiếp)
        # last_ack_num       : ack_num mới nhất đã chấp nhận (dùng nhận diện dup ACK)
        # send_window_right  : cạnh phải của cửa sổ gửi do peer cho phép
        # _last_advertised_window: window ta đã báo cho peer lần gần nhất; so sánh với
        #                          giá trị mới để biết khi nào cần gửi "window update".
        self.peer_window = 65535
        self.last_ack_num = None
        self.send_window_right = None
        self._last_advertised_window = 65535

        # ---- Kiểm soát tắc nghẽn (congestion control) -------------------------
        # cwnd: số byte tối đa được phép đang bay. ssthresh: ngưỡng chuyển từ
        # slow start (cwnd tăng theo cấp số nhân) sang congestion avoidance.
        self.cwnd = float(self.INITIAL_CWND)
        self.ssthresh = float(self.MAX_CWND)
        self.dup_ack_count = 0
        self.in_fast_recovery = False

        # (CUBIC thay cho tăng tuyến tính của Reno trong giai đoạn congestion avoidance.
        # Sau mỗi lần mất gói: w_max = cwnd lúc đó, epoch_start = mốc bắt đầu đường cong.)
        # CUBIC Congestion Control
        self.cubic_c = 0.4
        self.cubic_beta = 0.7
        self.w_max = float(self.INITIAL_CWND)
        self.epoch_start = None

        # ---- Phía nhận ---------------------------------------------------------
        # received_data : hàng đợi khối byte đã đủ thứ tự, chờ app gọi receive().
        # _received_bytes: tổng byte trong hàng đợi (để tính window quảng cáo).
        # _eof / _pending_fin_seq / _fin_accepted_seq: theo dõi FIN của peer; FIN đến
        #   sớm hơn dữ liệu thì được nhớ lại (pending) đến khi dữ liệu lấp đủ chỗ trống.
        # incoming_packets: hàng đợi datagram khi dùng shared socket (có giới hạn
        #   để quá tải thì bỏ gói chứ không phình RAM; UDP vốn cho phép mất gói).
        self.received_data = Queue()
        self._received_bytes = 0
        self._eof = False
        self._pending_fin_seq = None
        self._fin_accepted_seq = None
        self.incoming_packets = Queue(maxsize=2048)

        # ---- Đồng bộ hóa -------------------------------------------------------
        # condition (trên RLock) bảo vệ toàn bộ trạng thái; send() chờ trên nó và được
        # đánh thức bởi ACK/timer. _send_lock làm send() atomic giữa nhiều thread.
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        self.fin_ack_event = threading.Event()
        self._send_lock = threading.Lock()
        self._rto_start = None
        self._reset_by_peer = False
        self._recover = 0

        # ---- Đóng kết nối và thread nền ----------------------------------------
        # fin_seq_num: seq của FIN ta gửi (FIN chiếm 1 số thứ tự).
        # _reset_by_peer: phân biệt RST của peer với đóng cục bộ để chọn đúng exception.
        self.fin_seq_num = None
        self.peer_fin_received = False
        self.running = False
        self.receiver_thread = None
        self.timer_thread = None
        self._last_error = None

        self.last_recv_time = time.monotonic()
        self.last_keepalive_sent = time.monotonic()
        self._time_wait_deadline = None

    def _cubic_update(self):
        """Cwnd mục tiêu (byte) theo đường cong CUBIC (RFC 8312) tại thời điểm hiện tại.

            W(t) = C * (t - K)^3 + W_max      (đơn vị MSS)
            K    = cbrt(W_max * (1 - beta) / C)

        t là thời gian từ lúc mất gói gần nhất (epoch_start). Đường cong lõm lên đến
        W_max (cwnd trước khi mất gói) rồi lồi lên để dò băng thông mới.
        """
        if self.epoch_start is None:
            self.epoch_start = time.monotonic()

        t = time.monotonic() - self.epoch_start
        w_max_mss = self.w_max / self.MSS
        target_k = ((w_max_mss * (1.0 - self.cubic_beta)) / self.cubic_c) ** (1.0 / 3.0)
        target_mss = self.cubic_c * ((t - target_k) ** 3) + w_max_mss
        target_cwnd = target_mss * self.MSS

        return max(float(self.MIN_CWND), min(float(self.MAX_CWND), target_cwnd))

    def _make_packet(self, flags, seq_num=None, ack_num=None, payload=b""):
        """Dựng Packet gửi đi.

        Mặc định seq = next_seq_num (byte tiếp theo ta gửi), ack = receive_next (byte
        kế tiếp ta đang chờ). Mọi packet đều quảng cáo window hiện tại, nên ta ghi nhớ
        giá trị đó để biết khi nào cần gửi window update. seq/ack được lấy modulo 2^32.
        """
        if seq_num is None:
            seq_num = (self.send_window.next_seq_num
                       if self.send_window is not None else self.seq_num)
        if ack_num is None:
            ack_num = (self.receive_window.receive_next
                       if self.receive_window is not None else 0)
        window = self._advertised_window()
        self._last_advertised_window = window
        return Packet(
            src_port=self.src_port, dst_port=self.dst_port,
            seq_num=seq_num % MAX_SEQ, ack_num=ack_num % MAX_SEQ,
            flags=flags, window_size=window, payload=payload,
        )

    def _advertised_window(self):
        """Window quảng cáo = chỗ trống của bộ đệm nhận, chặn trên bởi 65535.

        Bộ đệm bị chiếm bởi cả dữ liệu in-order chưa đọc lẫn dữ liệu out-of-order.
        """
        if self.receive_window is None:
            return 65535
        buffered = self._received_bytes + self._ooo_buffered_bytes()
        free = self.RECEIVE_BUFFER_BYTES - buffered
        cap = getattr(self.receive_window, "receive_window_size", self.RECEIVE_BUFFER_BYTES)
        return max(0, min(free, cap, 65535))

    def _ooo_buffered_bytes(self):
        """Số byte out-of-order đang giữ. Thiếu buffered_bytes() thì báo lỗi chứ không
        trả 0: nếu không window sẽ bị tính thừa và bộ nhớ không được kiểm soát."""
        try:
            return self.receive_window.buffered_bytes()
        except AttributeError as e:
            raise ProtocolError("SlidingWindow phải có buffered_bytes()!") from e

    def _send_packet(self, packet):
        """Gửi một packet tới peer qua UDP (không đảm bảo đến nơi)."""
        if self.peer_address is None:
            raise ProtocolError("Chưa xác định địa chỉ peer!")
        self.sock.sendto(packet.pack(), self.peer_address)

    def _decode_packet(self, raw_data, address):
        """Datagram thô -> Packet, hoặc None nếu phải bỏ.

        Bỏ khi: sai địa chỉ nguồn, sai checksum/thiếu header, hoặc sai cặp cổng.
        Bỏ im lặng là đúng: bên gửi sẽ gửi lại khi hết RTO.
        """
        if address != self.peer_address:
            return None
        try:
            packet, _ = Packet.unpack(raw_data)
        except (ValueError, struct.error):
            return None
        if packet.src_port != self.dst_port or packet.dst_port != self.src_port:
            return None
        return packet

    def _init_windows(self, local_next_seq, peer_next_seq):
        """Tạo hai cửa sổ trượt sau handshake.

        local_next_seq: seq byte đầu tiên ta sẽ gửi (ISN của ta + 1).
        peer_next_seq : seq byte đầu tiên ta mong nhận (ISN của peer + 1).
        send_window_size = MAX_CWND để cửa sổ gửi không chặn cwnd.
        """
        self.send_window = SlidingWindow(
            initial_seq_num=local_next_seq,
            send_window_size=self.MAX_CWND,
            receive_window_size=self.RECEIVE_BUFFER_BYTES,
        )
        self.receive_window = SlidingWindow(
            initial_seq_num=peer_next_seq,
            send_window_size=self.MAX_CWND,
            receive_window_size=self.RECEIVE_BUFFER_BYTES,
        )
        self.seq_num = local_next_seq % MAX_SEQ
        self.last_ack_num = local_next_seq % MAX_SEQ
        self.send_window_right = (local_next_seq + self.peer_window) % MAX_SEQ

    def _rtt_sample(self, rtt):
        """Cập nhật SRTT / RTTVAR / RTO từ một mẫu RTT (Jacobson/Karels, RFC 6298).

            lần đầu : SRTT = rtt, RTTVAR = rtt / 2
            các lần sau: RTTVAR = 0.75*RTTVAR + 0.25*|SRTT - rtt|
                         SRTT   = 0.875*SRTT + 0.125*rtt
            RTO = SRTT + 4*RTTVAR, kẹp trong [MIN_RTO, MAX_RTO]
        """
        if self.srtt is None:
            self.srtt = rtt
            self.rttvar = rtt / 2
        else:
            self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - rtt)
            self.srtt = 0.875 * self.srtt + 0.125 * rtt
        self.rto = max(self.MIN_RTO, min(self.MAX_RTO, self.srtt + 4 * self.rttvar))

    def _rto_backoff(self):
        """Exponential backoff: mỗi lần hết giờ thì gấp đôi RTO (trần MAX_RTO)."""
        self.rto = min(self.MAX_RTO, self.rto * 2)

    def connect(self, server_address, timeout=1, max_retries=5):
        """Client chủ động mở kết nối (3-way handshake): SYN -> SYN-ACK -> ACK.

        timeout     : thời gian chờ SYN-ACK lần đầu, nhân đôi sau mỗi lần thất bại.
        max_retries : số lần gửi SYN tối đa (cũng là số lần gửi lại tối đa về sau).
        Raise ConnectionTimeoutError nếu hết lần thử; ProtocolError nếu bị RST hoặc
        gọi sai trạng thái. Không dùng được với shared socket.
        """
        if self.shared_socket:
            raise ProtocolError("connect() không dùng với shared socket!")
        if self.state != "CLOSED":
            raise ProtocolError("Connection không ở trạng thái CLOSED!")

        self.peer_address = server_address
        self.max_retries = max_retries
        self.state = "SYN_SENT"

        initial_seq = self.seq_num
        # SYN chiếm 1 số thứ tự: SYN-ACK hợp lệ phải có ack_num = ISN + 1.
        syn = self._make_packet(flags=TCPFlags.SYN, seq_num=initial_seq, ack_num=0)

        response = None
        current_timeout = timeout

        # Gửi SYN rồi chờ SYN-ACK; mỗi lần thất bại gửi lại với thời gian chờ gấp đôi
        # (exponential backoff: 1s, 2s, 4s...).
        for _ in range(max_retries):
            self.sock.settimeout(current_timeout)
            self._send_packet(syn)
            try:
                raw_data, address = self.sock.recvfrom(65535)
            except socket.timeout:
                current_timeout *= 2
                continue
            # Windows báo ICMP 'port unreachable' ngay lập tức dưới dạng ConnectionResetError.
            # Phải chờ rồi mới thử lại, nếu không các lần thử bị đốt hết trong vài mili giây.
            except ConnectionResetError:
                time.sleep(current_timeout)
                current_timeout *= 2
                continue

            packet = self._decode_packet(raw_data, address)
            if packet is None:
                continue
            # RST: peer từ chối kết nối (không có dịch vụ hoặc server đã đầy).
            if packet.flags & TCPFlags.RST:
                self.state = "CLOSED"
                raise ProtocolError("Peer từ chối kết nối (RST).")
            # Chỉ nhận SYN-ACK trả lời đúng SYN của ta (ack_num = ISN + 1); gói khác bị bỏ.
            if (packet.flags & TCPFlags.SYN and packet.flags & TCPFlags.ACK
                    and packet.ack_num == (initial_seq + 1) % MAX_SEQ):
                response = packet
                break

        if response is None:
            self.state = "CLOSED"
            raise ConnectionTimeoutError("Không nhận được SYN-ACK hợp lệ!")

        if response.window_size is not None:
            self.peer_window = response.window_size

        # SYN của mỗi bên chiếm 1 số thứ tự nên dữ liệu bắt đầu từ ISN + 1.
        # Gửi ACK cuối của bắt tay 3 bước rồi mới bật thread nền.
        self._init_windows((initial_seq + 1) % MAX_SEQ, (response.seq_num + 1) % MAX_SEQ)
        self._send_packet(self._make_packet(flags=TCPFlags.ACK))
        self._start_receiver()
        return True

    def establish_server_side(self, peer_address, peer_port, local_next_seq, peer_next_seq, peer_window=65535):
        """Phía server: dựng kết nối sau khi dispatcher (ServerMux) đã nhận SYN.

        Hàm này KHÔNG gửi gì: việc gửi SYN-ACK (và gửi lại khi nhận SYN trùng) do
        dispatcher làm, vì khi đó service chưa tồn tại / chưa chạy thread.
        """
        if self.state != "CLOSED":
            raise ProtocolError("Connection không ở trạng thái CLOSED!")
        self.peer_address = peer_address
        self.dst_port = peer_port
        self.peer_window = peer_window
        self._init_windows(local_next_seq, peer_next_seq)
        self.state = "ESTABLISHED"
        self._start_receiver()

    def feed_datagram(self, raw_data, address):
        """Dispatcher đưa datagram thuộc kết nối này vào hàng đợi (chỉ khi shared_socket).

        Không chặn: hàng đợi đầy thì bỏ gói, giống mạng thật bị nghẽn."""
        if not self.shared_socket:
            raise ProtocolError("feed_datagram chỉ dùng với shared socket!")
        if not self.running:
            return
        try:
            self.incoming_packets.put_nowait((raw_data, address))
        except Full:
            pass

    def _start_receiver(self):
        """Bật hai thread nền sau khi kết nối thiết lập xong (đều là daemon thread)."""
        self.state = "ESTABLISHED"
        self.running = True
        self.last_recv_time = time.monotonic()
        if not self.shared_socket:
            self.sock.settimeout(0.2)

        self.receiver_thread = threading.Thread(target=self._receive_loop, daemon=True, name="pkt-recv")
        self.receiver_thread.start()
        self.timer_thread = threading.Thread(target=self._timer_loop, daemon=True, name="pkt-timer")
        self.timer_thread.start()

    def _receive_loop(self):
        """Vòng nhận packet: lấy datagram -> kiểm tra hợp lệ -> _handle_packet.

        Một packet gây lỗi chỉ bị ghi vào _last_error chứ không làm chết thread;
        thread chỉ dừng khi socket đóng (OSError) hoặc kết nối đã CLOSED/FAILED.
        """
        while self.running:
            try:
                # shared socket: ServerMux đọc socket và đưa datagram vào qua feed_datagram().
                # Ngược lại tự recvfrom (timeout 0,2 s để còn kiểm tra cờ self.running).
                if self.shared_socket:
                    try:
                        raw_data, address = self.incoming_packets.get(timeout=0.2)
                    except Empty:
                        continue
                else:
                    try:
                        raw_data, address = self.sock.recvfrom(65535)
                    except socket.timeout:
                        continue
                    # Windows: ICMP unreachable từ lần gửi trước; bỏ qua, cơ chế retransmit sẽ lo.
                    except ConnectionResetError:
                        continue
            except OSError:
                break

            packet = self._decode_packet(raw_data, address)
            if packet is None:
                continue

            try:
                self._handle_packet(packet)
            except Exception as e:
                self._last_error = e
                with self.condition:
                    if self.state in ("CLOSED", "FAILED"):
                        break

    def _timer_loop(self):
        """Vòng timer, mỗi RETRANSMIT_TICK kiểm tra theo thứ tự:
          1. TIME_WAIT hết hạn -> CLOSED.
          2. Idle quá IDLE_TIMEOUT (không nhận gì từ peer) -> FAILED.
          3. Đến hạn keepalive -> gửi ACK rỗng (cũng mang window mới nhất).
          4. Hết RTO -> gửi lại segment cũ nhất chưa ACK.
        """
        while self.running:
            time.sleep(self.RETRANSMIT_TICK)
            try:
                with self.condition:
                    if not self.running:
                        break
                    now = time.monotonic()

                    # TIME_WAIT: chỉ chờ hết hạn, không retransmit hay keepalive nữa.
                    if self._time_wait_deadline is not None:
                        if now >= self._time_wait_deadline:
                            self.state = "CLOSED"
                            self.running = False
                            self.condition.notify_all()
                            break
                        continue

                    # Peer im lặng quá lâu (keepalive của peer cũng không tới) -> coi như đã chết.
                    if self.state == "ESTABLISHED" and now - self.last_recv_time > self.IDLE_TIMEOUT:
                        self.state = "FAILED"
                        self.condition.notify_all()
                        break

                    if self.state == "ESTABLISHED" and now - self.last_keepalive_sent >= self.KEEPALIVE_INTERVAL:
                        try:
                            self._send_packet(self._make_packet(flags=TCPFlags.ACK))
                        except (OSError, ProtocolError):
                            pass
                        self.last_keepalive_sent = now

                    # Kiểm tra RTO; hàm này raise ConnectionTimeoutError khi một segment quá max_retries.
                    if self.send_window is not None and self.state == "ESTABLISHED":
                        self._retransmit_due_locked()
            # _retransmit_due_locked đã đặt state = FAILED; dừng timer thread.
            except ConnectionTimeoutError as e:
                self._last_error = e
                break
            except Exception as e:
                self._last_error = e

    def _handle_packet(self, packet):
        """Xử lý một packet hợp lệ, theo thứ tự: RST -> ACK -> payload -> FIN."""
        with self.condition:
            self.last_recv_time = time.monotonic()

            # RST: hủy kết nối ngay, không cần bắt tay đóng. Ghi nhận để receive()/close()
            # báo đúng loại lỗi (PeerResetError) thay vì lỗi chung.
            if packet.flags & TCPFlags.RST:
                self._reset_by_peer = True
                self.state = "CLOSED"
                self.running = False
                self.condition.notify_all()
                return

            # Packet nào mang cờ ACK (kể cả packet dữ liệu) đều xác nhận dữ liệu ta đã gửi.
            if packet.flags & TCPFlags.ACK:
                self._handle_ack_locked(packet)

            # Có dữ liệu: kiểm tra còn chỗ trong bộ đệm (flow control) rồi mới nhận.
            if packet.payload and self.receive_window is not None:
                buffered = self._received_bytes + self._ooo_buffered_bytes()
                free = self.RECEIVE_BUFFER_BYTES - buffered
                # Hết chỗ: từ chối (không advance) và gửi lại ACK cũ kèm window nhỏ để bên gửi chậm lại.
                if len(packet.payload) > free:
                    ack = self._make_packet(flags=TCPFlags.ACK, ack_num=self.receive_window.receive_next)
                    self._send_packet(ack)
                else:
                    # receive_packet ráp theo thứ tự: gói đúng thứ tự -> trả dữ liệu (kèm các gói đã buffer
                    # nay đã liền mạch); gói đến sớm -> giữ lại và ACK số cũ (sinh ra dup ACK);
                    # gói trùng -> bỏ.
                    data, ack_num = self.receive_window.receive_packet(packet.seq_num, packet.payload)
                    if ack_num is not None:
                        ack = self._make_packet(flags=TCPFlags.ACK, ack_num=ack_num)
                        self._send_packet(ack)
                    if data:
                        self.received_data.put(data)
                        self._received_bytes += len(data)
                    self._try_resolve_pending_fin_locked()

            # FIN xử lý SAU payload vì FIN nằm ngay sau dữ liệu của cùng packet.
            if packet.flags & TCPFlags.FIN:
                self._handle_fin_locked(packet)

    def _handle_ack_locked(self, packet):
        """Xử lý trường ACK của packet (đang giữ khóa). Các bước:

          1. Nếu ACK xác nhận FIN của ta -> đánh thức close().
          2. Lấy mẫu RTT (bỏ qua nếu ACK đi qua segment đã gửi lại - quy tắc Karn).
          3. Nhận diện dup ACK; đủ ngưỡng -> fast retransmit + vào fast recovery.
          4. ACK tiến lên: thoát fast recovery, hoặc xử lý partial ACK (NewReno).
          5. Trượt cửa sổ gửi, tăng cwnd (slow start / CUBIC), dọn sổ sách.
          6. Cập nhật window của peer; đánh thức send() đang chờ.
        """
        # (1) ACK cho FIN của ta: ack_num = fin_seq + 1 (FIN chiếm 1 số thứ tự).
        if (self.fin_seq_num is not None
                and self.state in ("ESTABLISHED", "TIME_WAIT")
                and packet.ack_num == (self.fin_seq_num + 1) % MAX_SEQ
                and self.send_window is not None
                and seq_lte(packet.ack_num, (self.send_window.next_seq_num + 1) % MAX_SEQ)):
            self.fin_ack_event.set()

        if self.send_window is None:
            return

        ack = packet.ack_num
        # ACK hợp lệ chỉ khi nằm trong [send_base, next_seq_num]; ACK cũ hoặc vượt quá bị bỏ qua.
        # So sánh theo modulo 2^32 vì seq có thể wrap-around.
        in_window = seq_gte(ack, self.send_window.send_base) and seq_lte(ack, self.send_window.next_seq_num)

        if in_window:
            # (2) Mẫu RTT: chỉ lấy khi ACK kết thúc đúng tại biên một segment gửi MỘT lần.
            # Bỏ mẫu nếu ACK đi qua segment đã gửi lại (quy tắc Karn): không biết ACK trả lời
            # cho lần gửi nào, và với ACK tích lũy thời gian đo còn gồm cả thời gian chờ lấp lỗ
            # nên sẽ làm RTO phình to.
            start = self.start_of_end.get(ack)
            crossed_retx = any(seq_gte(r, self.send_window.send_base) and seq_lt(r, ack)
                               for r in self.retransmitted)
            if (start is not None
                    and start in self.sent_at
                    and start not in self.retransmitted
                    and not crossed_retx):
                self._rtt_sample(time.monotonic() - self.sent_at[start])

        # (3) Dup ACK: chỉ ACK THUẦN (không payload/SYN/FIN) mới được tính. Packet dữ liệu của
        # peer cũng mang cờ ACK với cùng ack_num; nếu đếm chúng thì khi hai bên cùng gửi dữ liệu
        # sẽ có fast retransmit giả làm cwnd bị cắt oan.
        is_pure_ack = (not packet.payload and not (packet.flags & (TCPFlags.SYN | TCPFlags.FIN)))
        if (is_pure_ack
                and ack == self.send_window.send_base
                and self.last_ack_num is not None
                and ack == self.last_ack_num
                and packet.window_size == self.peer_window
                and seq_lt(self.send_window.send_base, self.send_window.next_seq_num)):
            self.dup_ack_count += 1
            # Đủ 3 dup ACK: gói ở send_base gần như chắc chắn đã mất -> gửi lại ngay (fast
            # retransmit) thay vì chờ RTO. _recover = điểm phải phục hồi tới (NewReno).
            if not self.in_fast_recovery and self.dup_ack_count >= self.DUP_ACK_THRESHOLD:
                self.in_fast_recovery = True
                self._recover = self.send_window.next_seq_num
                self._retransmit_seq_locked(self.send_window.send_base)

                # CUBIC: ghi nhớ W_max, mở epoch mới, ssthresh = beta * cwnd (beta = 0,7).
                # Vào fast recovery với cwnd = ssthresh + 3 MSS (3 dup ACK = 3 gói đã rời mạng).
                self.w_max = self.cwnd
                self.epoch_start = time.monotonic()
                self.ssthresh = max(float(self.MIN_CWND), self.cwnd * self.cubic_beta)
                self.cwnd = self.ssthresh + self.DUP_ACK_THRESHOLD * self.MSS
            # Mỗi dup ACK thêm trong recovery nghĩa là thêm 1 gói rời mạng: nới cwnd 1 MSS
            # để tiếp tục gửi dữ liệu mới (window inflation).
            elif self.in_fast_recovery:
                self.cwnd = min(float(self.MAX_CWND), self.cwnd + self.MSS)
        # (4) ACK tiến lên: có dữ liệu mới được xác nhận.
        elif in_window and seq_gt(ack, self.send_window.send_base):
            if self.in_fast_recovery:
                # Full ACK (>= _recover): mọi gói của đợt mất đã được lấp -> thoát recovery.
                # Partial ACK (NewReno): vẫn còn lỗ khác trong cùng cửa sổ -> gửi lại ngay segment
                # tiếp theo thay vì chờ RTO, và ở lại recovery.
                if seq_gte(ack, self._recover):
                    self.cwnd = self.ssthresh
                    self.in_fast_recovery = False
                    self.epoch_start = time.monotonic()
                else:
                    self.cwnd = max(float(self.MIN_CWND),
                                    self.cwnd - seq_diff(ack, self.send_window.send_base) + self.MSS)
                    self._retransmit_seq_locked(ack)
            self.dup_ack_count = 0

        if in_window:
            # (5) Trượt cửa sổ gửi. Mỗi ACK tiến lên khởi động lại RTO timer và xóa backoff
            # (tính lại RTO từ SRTT) vì kết nối đang đi được.
            old_base = self.send_window.send_base
            self.send_window.process_ack(ack)

            acked = seq_diff(ack, old_base)
            if acked > 0:
                self._rto_start = time.monotonic()
                if self.srtt is not None:
                    self.rto = max(self.MIN_RTO, min(self.MAX_RTO, self.srtt + 4 * self.rttvar))
                else:
                    self.rto = self.INITIAL_RTO
            if acked > 0 and not self.in_fast_recovery:
                # Slow start: cwnd tăng theo số byte được ACK (gấp đôi mỗi RTT). Đạt ssthresh thì
                # chuyển sang congestion avoidance theo đường cong CUBIC.
                if self.cwnd < self.ssthresh:
                    self.cwnd = min(float(self.MAX_CWND), self.cwnd + acked)
                else:
                    target_cwnd = self._cubic_update()
                    if target_cwnd > self.cwnd:
                        # RFC 8312: cnt = số ACK cần để tăng 1 MSS, tính theo đơn vị MSS.
                        # Mỗi segment được ACK tăng MSS/cnt, nên với `acked` byte thì tăng acked/cnt.
                        # cnt >= 2 giới hạn tốc độ tăng tối đa 1,5x mỗi RTT (như Linux).
                        cwnd_mss = self.cwnd / self.MSS
                        cnt = max(2.0, cwnd_mss / ((target_cwnd - self.cwnd) / self.MSS))
                        self.cwnd = min(float(self.MAX_CWND), self.cwnd + acked / cnt)
                    else:
                        self.cwnd = min(float(self.MAX_CWND), self.cwnd + (self.MSS * acked / self.cwnd))

            # Dọn sổ sách của các segment đã được ACK để bộ nhớ không phình ra.
            outstanding = {seq for seq, _ in self.send_window.get_unacked_packets()}
            for seq in list(self.sent_at):
                if seq not in outstanding:
                    self.sent_at.pop(seq, None)
                    self.retry_counts.pop(seq, None)
                    self.retransmitted.discard(seq)
                    end = self.seq_end_map.pop(seq, None)
                    if end is not None:
                        self.start_of_end.pop(end, None)

        if in_window:
            # (6) Chỉ tin window từ ACK mới nhất, tránh ACK cũ đến trễ làm cửa sổ co lại sai.
            if self.last_ack_num is None or seq_gte(ack, self.last_ack_num):
                self.peer_window = packet.window_size
                self.last_ack_num = ack
                self.send_window_right = (ack + packet.window_size) % MAX_SEQ

        self.condition.notify_all()

    def _handle_fin_locked(self, packet):
        """Xử lý FIN của peer (FIN chiếm 1 số thứ tự, đặt ngay sau payload).

        FIN đúng thứ tự -> chấp nhận, đặt EOF. FIN đến sớm (còn thiếu dữ liệu phía
        trước) -> nhớ lại, chờ dữ liệu lấp đủ. FIN trùng (ACK bị mất) -> chỉ ACK lại.
        Luôn trả ACK để peer biết đã nhận.
        """
        if self.receive_window is None:
            return

        fin_seq = (packet.seq_num + len(packet.payload)) % MAX_SEQ

        if self._fin_accepted_seq is not None:
            if fin_seq == self._fin_accepted_seq:
                ack = self._make_packet(flags=TCPFlags.ACK, ack_num=self.receive_window.receive_next)
                self._send_packet(ack)
                return

        expected = self.receive_window.receive_next
        if fin_seq == expected:
            self.receive_window.receive_next = (self.receive_window.receive_next + 1) % MAX_SEQ
            self.peer_fin_received = True
            self._eof = True
            self._fin_accepted_seq = fin_seq
            self._pending_fin_seq = None
        else:
            self._pending_fin_seq = fin_seq

        ack = self._make_packet(flags=TCPFlags.ACK, ack_num=self.receive_window.receive_next)
        self._send_packet(ack)
        self.condition.notify_all()

    def _try_resolve_pending_fin_locked(self):
        """Sau khi dữ liệu lấp chỗ trống: nếu đã tới vị trí của FIN đến sớm thì chấp nhận nó."""
        if self._pending_fin_seq is None or self.receive_window is None:
            return
        if self.receive_window.receive_next == self._pending_fin_seq:
            self.receive_window.receive_next = (self.receive_window.receive_next + 1) % MAX_SEQ
            self.peer_fin_received = True
            self._eof = True
            self._fin_accepted_seq = self._pending_fin_seq
            self._pending_fin_seq = None
            try:
                ack = self._make_packet(flags=TCPFlags.ACK, ack_num=self.receive_window.receive_next)
                self._send_packet(ack)
            except (OSError, ProtocolError):
                pass
            self.condition.notify_all()

    def _retransmit_seq_locked(self, seq):
        """Gửi lại đúng một segment chưa ACK (dùng cho fast retransmit / partial ACK).

        Đánh dấu retransmitted để không lấy mẫu RTT từ nó (Karn) và khởi động lại
        RTO timer."""
        for s, (p, _) in self.send_window.get_unacked_packets():
            if s == seq:
                try:
                    self._send_packet(p)
                except (OSError, ProtocolError):
                    pass
                now = time.monotonic()
                self.sent_at[seq] = now
                self.retransmitted.add(seq)
                self._rto_start = now
                return True
        return False

    def _retransmit_due_locked(self):
        """Xử lý RTO: có MỘT timer cho cả kết nối (như TCP), tính từ lần gửi/ACK-tiến gần nhất.

        Hết giờ thì chỉ gửi lại segment cũ nhất chưa ACK. Receiver đã buffer các gói đến
        sớm nên ACK tích lũy sẽ nhảy qua chúng: không gửi thừa, không backoff nhiều lần
        cho cùng một lần mất gói. Đồng thời: cwnd = 1 MSS, ssthresh giảm theo CUBIC,
        RTO gấp đôi. Quá max_retries lần cho cùng một segment -> FAILED.
        """
        unacked = self.send_window.get_unacked_packets()
        if not unacked:
            return
        now = time.monotonic()
        if self._rto_start is None:
            self._rto_start = now
            return
        if now - self._rto_start < self.rto:
            return

        # Segment cũ nhất = gần send_base nhất theo khoảng cách modulo (đúng cả khi seq wrap-around).
        base = self.send_window.send_base
        seq, (pkt, _) = min(unacked, key=lambda item: seq_diff(item[0], base))
        retries = self.retry_counts.get(seq, 0)
        if retries >= self.max_retries:
            self.state = "FAILED"
            self.condition.notify_all()
            raise ConnectionTimeoutError(f"Không nhận được ACK cho packet seq={seq}!")

        self.w_max = self.cwnd
        self.epoch_start = time.monotonic()
        self.ssthresh = max(float(self.MIN_CWND), self.cwnd * self.cubic_beta)
        self.cwnd = float(self.MIN_CWND)
        self._rto_backoff()
        self.dup_ack_count = 0
        self.in_fast_recovery = False

        self._send_packet(pkt)
        self.sent_at[seq] = now
        self.retry_counts[seq] = retries + 1
        self.retransmitted.add(seq)
        self._rto_start = now

    def send(self, data: bytes, max_payload_size=None):
        """Gửi toàn bộ `data` và CHẶN đến khi mọi byte được ACK; trả len(data).

        Atomic: các lời gọi song song được xếp hàng bằng _send_lock nên một message
        gửi bằng một lần send() luôn liền mạch trong stream (không xen kẽ).
        Raise ConnectionTimeoutError (FAILED), ConnectionResetError (CLOSED),
        ProtocolError (chưa thiết lập).
        """
        with self._send_lock:
            return self._send_impl(data, max_payload_size)

    def _send_impl(self, data: bytes, max_payload_size=None):
        """Thân của send(): cắt data thành segment ≤ MSS, gửi trong giới hạn cửa sổ."""
        if self.state == "FAILED":
            raise ConnectionTimeoutError("Connection FAILED — không thể gửi tiếp!")
        if self.state == "CLOSED":
            raise ConnectionResetError("Connection đã đóng — không thể gửi tiếp!")
        if self.state != "ESTABLISHED":
            raise ProtocolError("Connection chưa được thiết lập!")
        if not isinstance(data, bytes):
            raise TypeError("data phải có kiểu bytes!")
        mss = max_payload_size if max_payload_size else self.MSS
        if mss <= 0:
            raise ValueError("max_payload_size phải lớn hơn 0!")
        if not data:
            return 0

        offset = 0
        while offset < len(data):
            with self.condition:
                while True:
                    if self.state == "FAILED":
                        raise ConnectionTimeoutError("Connection FAILED — không thể gửi tiếp!")
                    if self.state == "CLOSED":
                        raise ConnectionResetError("Connection đã đóng — không thể gửi tiếp!")
                    if self.state != "ESTABLISHED":
                        raise ProtocolError("Connection không còn hoạt động!")

                    if self.send_window_right is None:
                        self.send_window_right = (self.send_window.send_base + self.peer_window) % MAX_SEQ
                    
                    # Được gửi đến đâu = min của ba giới hạn tính từ send_base:
                    #   peer_window : flow control (bên nhận còn chỗ bao nhiêu)
                    #   cwnd        : congestion control (mạng chịu được bao nhiêu)
                    #   send_window_size: dung lượng cửa sổ gửi của ta
                    # Nếu chưa còn chỗ thì chờ ACK/window update đánh thức.
                    effective_right = (self.send_window.send_base + min(self.peer_window, int(self.cwnd), self.send_window.send_window_size)) % MAX_SEQ
                    available = seq_diff(effective_right, self.send_window.next_seq_num)
                    
                    if available > 0:
                        break
                    self.condition.wait(timeout=0.05)

                remaining = len(data) - offset
                payload_size = min(available, min(mss, remaining))
                payload = data[offset:offset + payload_size]
                seq_num = self.send_window.next_seq_num

                packet = self._make_packet(flags=TCPFlags.ACK, seq_num=seq_num, payload=payload)

                # add_packet từ chối nếu vượt dung lượng cửa sổ gửi: chờ một nhịp rồi thử lại.
                if not self.send_window.add_packet(seq_num, packet, len(payload)):
                    self.condition.wait(timeout=0.05)
                    continue

                if self.send_window.send_base == seq_num:
                    self._rto_start = time.monotonic()
                self.sent_at[seq_num] = time.monotonic()
                self.retry_counts[seq_num] = 0
                self.seq_end_map[seq_num] = (seq_num + len(payload)) % MAX_SEQ
                self.start_of_end[(seq_num + len(payload)) % MAX_SEQ] = seq_num
                self._send_packet(packet)
                offset += payload_size

        # Đã đẩy hết dữ liệu vào cửa sổ: chờ ACK tích lũy phủ tới byte cuối. Nhờ vậy khi
        # send() trả về, dữ liệu chắc chắn đã đến peer (hoặc đã raise lỗi).
        final_seq = self.send_window.next_seq_num
        with self.condition:
            while seq_lt(self.send_window.send_base, final_seq):
                if self.state == "FAILED":
                    raise ConnectionTimeoutError("Connection FAILED — không thể hoàn tất gửi!")
                if self.state == "CLOSED":
                    raise ConnectionResetError("Connection đã đóng — không thể hoàn tất gửi!")
                if self.state != "ESTABLISHED":
                    raise ProtocolError("Connection không còn hoạt động!")
                self.condition.wait(timeout=0.05)
        return len(data)

    def receive(self, timeout=None):
        """Lấy khối dữ liệu tiếp theo; chờ tối đa `timeout` giây (None = chờ mãi).

        Trả về:
            bytes không rỗng : dữ liệu.
            b""              : EOF bình thường (peer đã FIN và dữ liệu đã đọc hết).
        Raise:
            ConnectionTimeoutError : hết giờ chờ, hoặc kết nối FAILED (mất mạng).
            PeerResetError / ConnectionResetError : peer RST / đóng bất thường.
        Dữ liệu còn trong hàng đợi luôn được trả hết trước khi báo lỗi.
        Sau mỗi lần đọc, nếu bộ đệm trống thêm đáng kể thì gửi window update.
        """
        if self.receive_window is None:
            raise ProtocolError("Connection chưa được thiết lập!")

        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            # EOF chỉ được báo SAU khi đã đọc hết dữ liệu còn trong hàng đợi.
            if self._eof and self.received_data.empty():
                return b""

            if self.received_data.empty():
                # Hàng đợi rỗng mà kết nối đã chết: báo lỗi chứ không trả EOF, để caller không nhầm
                # 'file bị cắt' với 'file hoàn tất'.
                if self.state == "FAILED":
                    raise ConnectionTimeoutError("Connection FAILED (timeout/RST) — dữ liệu có thể bị cắt!")
                if self.state == "CLOSED" and not self._eof:
                    exc = (PeerResetError if self._reset_by_peer else ConnectionResetError)
                    raise exc("Connection bị đóng bất thường (RST) — dữ liệu có thể bị cắt!")

            wait = 0.1
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if self._eof and self.received_data.empty():
                        return b""
                    if self.state in ("FAILED", "CLOSED"):
                        raise ConnectionTimeoutError("Connection chết trong khi chờ dữ liệu!")
                    raise ConnectionTimeoutError("Hết thời gian chờ dữ liệu!")
                wait = min(wait, remaining)

            try:
                data = self.received_data.get(timeout=wait)
            except Empty:
                continue

            with self.condition:
                self._received_bytes -= len(data)
                if self._received_bytes < 0:
                    self._received_bytes = 0
                new_win = self._advertised_window()
                # Bộ đệm vừa trống thêm: báo ngay cho peer (window update). Nếu không, bên gửi đang bị
                # cửa sổ 0 chặn sẽ phải chờ đến lần keepalive kế tiếp.
                if new_win > self._last_advertised_window:
                    try:
                        self._send_packet(self._make_packet(flags=TCPFlags.ACK))
                    except (OSError, ProtocolError):
                        pass
            return data

    def close(self, timeout=3):
        """Đóng kết nối kiểu đối xứng: FIN -> chờ ACK -> chờ FIN của peer -> TIME_WAIT.

        Trả True nếu peer cũng đã gửi FIN (đóng sạch hai chiều), False nếu peer không
        FIN kịp trong `timeout` hoặc đã RST. Raise ConnectionTimeoutError nếu FIN
        không được ACK. close() luôn mất ít nhất ~TIME_WAIT_SECONDS: ta ở lại
        TIME_WAIT để re-ACK FIN nếu ACK cuối bị mất.
        """
        if self.state not in ("ESTABLISHED", "FAILED"):
            self.running = False
            self.state = "CLOSED"
            return True
        if self.state == "FAILED":
            self.running = False
            self.state = "CLOSED"
            return True

        self.fin_ack_event.clear()
        # FIN nằm sau mọi dữ liệu đã gửi (send() đảm bảo chúng đã được ACK).
        self.fin_seq_num = self.send_window.next_seq_num
        fin = self._make_packet(flags=TCPFlags.FIN | TCPFlags.ACK, seq_num=self.fin_seq_num)

        deadline = time.monotonic() + timeout
        retries = 0
        # Gửi FIN và chờ ACK; chưa có thì gửi lại theo RTO. Peer đã RST thì không còn gì để chờ.
        while not self.fin_ack_event.is_set():
            if self._reset_by_peer:
                self.running = False
                self.state = "CLOSED"
                return False
            self._send_packet(fin)
            remaining = deadline - time.monotonic()
            if remaining <= 0 or retries >= self.max_retries:
                self.running = False
                self.state = "CLOSED"
                raise ConnectionTimeoutError("Không nhận được ACK cho FIN!")
            self.fin_ack_event.wait(min(self.rto, remaining))
            retries += 1

        # FIN của ta đã được ACK. Chờ FIN của peer để đóng đối xứng; không thấy thì vẫn đóng
        # nhưng trả graceful = False.
        peer_deadline = time.monotonic() + timeout
        with self.condition:
            while not self.peer_fin_received:
                remaining = peer_deadline - time.monotonic()
                if remaining <= 0:
                    break
                self.condition.wait(timeout=min(0.1, remaining))

        graceful = self.peer_fin_received

        with self.condition:
            # TIME_WAIT: receiver thread vẫn chạy để re-ACK nếu peer gửi lại FIN (ACK cuối của ta
            # có thể bị mất); timer thread sẽ đưa về CLOSED khi hết hạn.
            self.state = "TIME_WAIT"
            self._time_wait_deadline = time.monotonic() + self.TIME_WAIT_SECONDS
            self.condition.notify_all()

        if self.receiver_thread is not None and self.receiver_thread is not threading.current_thread():
            self.receiver_thread.join(timeout=self.TIME_WAIT_SECONDS + 1)
        if self.timer_thread is not None and self.timer_thread is not threading.current_thread():
            self.timer_thread.join(timeout=self.TIME_WAIT_SECONDS + 1)

        self.running = False
        with self.condition:
            self.state = "CLOSED"
            self.condition.notify_all()
        return graceful