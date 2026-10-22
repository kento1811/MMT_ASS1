import random
import socket
import struct
import threading
import time
from queue import Queue, Empty, Full

from packet import Packet
from constants import TCPFlags
from exceptions import ConnectionTimeoutError, ProtocolError
from algorithms import SlidingWindow


class PacketService:
    MSS = 1024
    RECEIVE_BUFFER_BYTES = 256 * 1024
    KEEPALIVE_INTERVAL = 5.0
    IDLE_TIMEOUT = 30.0
    RETRANSMIT_TICK = 0.05
    TIME_WAIT_SECONDS = 2.0

    INITIAL_RTO = 1.0
    MIN_RTO = 0.2
    MAX_RTO = 60.0

    INITIAL_CWND = 4 * MSS
    MIN_CWND = MSS
    MAX_CWND = 64 * MSS
    DUP_ACK_THRESHOLD = 3

    def __init__(self, sock, src_port, dst_port, shared_socket=False):
        self.sock = sock
        self.src_port = src_port
        self.dst_port = dst_port
        self.shared_socket = shared_socket

        self.state = "CLOSED"
        # ISN random 16-bit. Seq/ack là 32-bit (Packet dùng 'I'), tăng theo byte.
        # KHÔNG hỗ trợ wrap-around 32-bit — giới hạn ~4GB/connection.
        self.seq_num = random.randint(0, 0xFFFF)
        self.peer_address = None

        self.send_window = None
        self.receive_window = None
        self.max_retries = 5

        self.rto = self.INITIAL_RTO
        self.srtt = None
        self.rttvar = None
        self.sent_at = {}
        self.retry_counts = {}
        self.retransmitted = set()
        self.seq_end_map = {}
        self.start_of_end = {}

        self.peer_window = 65535
        self.last_ack_num = None
        self.send_window_right = None
        self._last_advertised_window = 65535

        self.cwnd = float(self.INITIAL_CWND)
        self.ssthresh = float(self.MAX_CWND)
        self.dup_ack_count = 0
        self.in_fast_recovery = False

        self.received_data = Queue()
        self._received_bytes = 0
        self._eof = False
        self._pending_fin_seq = None
        self._fin_accepted_seq = None
        self.incoming_packets = Queue()

        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        self.fin_ack_event = threading.Event()

        self.fin_seq_num = None
        self.peer_fin_received = False
        self.running = False
        self.receiver_thread = None
        self.timer_thread = None
        self._last_error = None

        self.last_recv_time = time.monotonic()
        self.last_keepalive_sent = time.monotonic()
        self._time_wait_deadline = None

    def _make_packet(self, flags, seq_num=None, ack_num=None, payload=b""):
        if seq_num is None:
            seq_num = (self.send_window.next_seq_num
                       if self.send_window is not None else self.seq_num)
        if ack_num is None:
            ack_num = (self.receive_window.receive_next
                       if self.receive_window is not None else 0)
        return Packet(
            src_port=self.src_port, dst_port=self.dst_port,
            seq_num=seq_num, ack_num=ack_num, flags=flags,
            window_size=self._advertised_window(), payload=payload,
        )

    def _advertised_window(self):
        if self.receive_window is None:
            return 65535
        buffered = self._received_bytes + self._ooo_buffered_bytes()
        free = self.RECEIVE_BUFFER_BYTES - buffered
        cap = getattr(self.receive_window, "receive_window_size",
                      self.RECEIVE_BUFFER_BYTES)
        return max(0, min(free, cap, 65535))

    def _ooo_buffered_bytes(self):
        if self.receive_window is None:
            return 0
        try:
            return self.receive_window.buffered_bytes()
        except AttributeError as e:
            raise ProtocolError(
                "SlidingWindow phải có buffered_bytes()!") from e

    def _send_packet(self, packet):
        if self.peer_address is None:
            raise ProtocolError("Chưa xác định địa chỉ peer!")
        self.sock.sendto(packet.pack(), self.peer_address)

    def _decode_packet(self, raw_data, address):
        if address != self.peer_address:
            return None
        try:
            packet, _ = Packet.unpack(raw_data)
        except (ValueError, struct.error):
            return None
        if packet.src_port != self.dst_port:
            return None
        if packet.dst_port != self.src_port:
            return None
        return packet

    def _init_windows(self, local_next_seq, peer_next_seq):
        # send_window_size phải >= MAX_CWND để không chặn cwnd.
        # receive_window_size = RECEIVE_BUFFER_BYTES để advertised window tối đa.
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
        self.seq_num = local_next_seq
        self.last_ack_num = local_next_seq - 1
        self.send_window_right = local_next_seq - 1 + self.peer_window

    def _rtt_sample(self, rtt):
        if self.srtt is None:
            self.srtt = rtt
            self.rttvar = rtt / 2
        else:
            self.rttvar = 0.75 * self.rttvar + 0.25 * abs(self.srtt - rtt)
            self.srtt = 0.875 * self.srtt + 0.125 * rtt
        self.rto = max(self.MIN_RTO, min(self.MAX_RTO,
                                         self.srtt + 4 * self.rttvar))

    def _rto_backoff(self):
        self.rto = min(self.MAX_RTO, self.rto * 2)

    def connect(self, server_address, timeout=1, max_retries=5):
        if self.shared_socket:
            raise ProtocolError("connect() không dùng với shared socket!")
        if self.state != "CLOSED":
            raise ProtocolError("Connection không ở trạng thái CLOSED!")

        self.peer_address = server_address
        self.max_retries = max_retries
        self.state = "SYN_SENT"

        initial_seq = self.seq_num
        syn = self._make_packet(flags=TCPFlags.SYN,
                                seq_num=initial_seq, ack_num=0)

        response = None
        current_timeout = timeout

        for _ in range(max_retries):
            self.sock.settimeout(current_timeout)
            self._send_packet(syn)
            try:
                raw_data, address = self.sock.recvfrom(65535)
            except socket.timeout:
                current_timeout *= 2
                continue
            except ConnectionResetError:
                continue

            packet = self._decode_packet(raw_data, address)
            if packet is None:
                continue
            if packet.flags & TCPFlags.RST:
                self.state = "CLOSED"
                raise ProtocolError("Peer từ chối kết nối (RST).")
            if (packet.flags & TCPFlags.SYN and packet.flags & TCPFlags.ACK
                    and packet.ack_num == initial_seq + 1):
                response = packet
                break

        if response is None:
            self.state = "CLOSED"
            raise ConnectionTimeoutError("Không nhận được SYN-ACK hợp lệ!")

        if response.window_size is not None:
            self.peer_window = response.window_size

        self._init_windows(initial_seq + 1, response.seq_num + 1)

        self._send_packet(self._make_packet(flags=TCPFlags.ACK))
        self._start_receiver()
        return True

    def establish_server_side(self, peer_address, peer_port,
                              local_next_seq, peer_next_seq,
                              peer_window=65535):
        if self.state != "CLOSED":
            raise ProtocolError("Connection không ở trạng thái CLOSED!")
        self.peer_address = peer_address
        self.dst_port = peer_port
        self.peer_window = peer_window
        self._init_windows(local_next_seq, peer_next_seq)
        self.state = "ESTABLISHED"
        self._start_receiver()

    def feed_datagram(self, raw_data, address):
        if not self.shared_socket:
            raise ProtocolError("feed_datagram chỉ dùng với shared socket!")
        if not self.running:
            return
        try:
            self.incoming_packets.put_nowait((raw_data, address))
        except Full:
            pass

    def _start_receiver(self):
        self.state = "ESTABLISHED"
        self.running = True
        self.last_recv_time = time.monotonic()
        if not self.shared_socket:
            self.sock.settimeout(0.2)

        self.receiver_thread = threading.Thread(
            target=self._receive_loop, daemon=True, name="pkt-recv")
        self.receiver_thread.start()
        self.timer_thread = threading.Thread(
            target=self._timer_loop, daemon=True, name="pkt-timer")
        self.timer_thread.start()

    def _receive_loop(self):
        while self.running:
            try:
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
                    except ConnectionResetError:
                        continue
            except OSError:
                break

            packet = self._decode_packet(raw_data, address)
            if packet is None:
                continue

            try:
                self._handle_packet(packet)
            except (OSError, ProtocolError) as e:
                self._last_error = e
                with self.condition:
                    if self.state in ("CLOSED", "FAILED"):
                        break

    def _timer_loop(self):
        while self.running:
            time.sleep(self.RETRANSMIT_TICK)
            try:
                with self.condition:
                    if not self.running:
                        break
                    now = time.monotonic()

                    if self._time_wait_deadline is not None:
                        if now >= self._time_wait_deadline:
                            self.state = "CLOSED"
                            self.running = False
                            self.condition.notify_all()
                            break
                        continue

                    if (self.state == "ESTABLISHED"
                            and now - self.last_recv_time > self.IDLE_TIMEOUT):
                        self.state = "FAILED"
                        self.condition.notify_all()
                        break

                    if (self.state == "ESTABLISHED"
                            and now - self.last_keepalive_sent >= self.KEEPALIVE_INTERVAL):
                        try:
                            self._send_packet(self._make_packet(flags=TCPFlags.ACK))
                        except (OSError, ProtocolError):
                            pass
                        self.last_keepalive_sent = now

                    if self.send_window is not None and self.state == "ESTABLISHED":
                        self._retransmit_due_locked()
            except ConnectionTimeoutError as e:
                self._last_error = e
                break
            except (OSError, ProtocolError) as e:
                self._last_error = e

    def _handle_packet(self, packet):
        with self.condition:
            self.last_recv_time = time.monotonic()

            if packet.flags & TCPFlags.RST:
                self.state = "CLOSED"
                self.running = False
                self.condition.notify_all()
                return

            if packet.flags & TCPFlags.ACK:
                self._handle_ack_locked(packet)

            if packet.payload and self.receive_window is not None:
                buffered = self._received_bytes + self._ooo_buffered_bytes()
                free = self.RECEIVE_BUFFER_BYTES - buffered
                if len(packet.payload) > free:
                    ack = self._make_packet(
                        flags=TCPFlags.ACK,
                        ack_num=self.receive_window.receive_next)
                    self._send_packet(ack)
                else:
                    data, ack_num = self.receive_window.receive_packet(
                        packet.seq_num, packet.payload)
                    if ack_num is not None:
                        ack = self._make_packet(
                            flags=TCPFlags.ACK, ack_num=ack_num)
                        self._send_packet(ack)
                    if data:
                        self.received_data.put(data)
                        self._received_bytes += len(data)
                    self._try_resolve_pending_fin_locked()

            if packet.flags & TCPFlags.FIN:
                self._handle_fin_locked(packet)

    def _handle_ack_locked(self, packet):
        if (self.fin_seq_num is not None
                and self.state in ("ESTABLISHED", "TIME_WAIT")
                and packet.ack_num == self.fin_seq_num + 1
                and self.send_window is not None
                and packet.ack_num <= self.send_window.next_seq_num + 1):
            self.fin_ack_event.set()

        if self.send_window is None:
            return

        ack = packet.ack_num
        in_window = self.send_window.send_base <= ack <= self.send_window.next_seq_num

        if in_window:
            start = self.start_of_end.get(ack)
            if (start is not None
                    and start in self.sent_at
                    and start not in self.retransmitted):
                self._rtt_sample(time.monotonic() - self.sent_at[start])

        if (ack == self.send_window.send_base
                and self.last_ack_num is not None
                and ack == self.last_ack_num
                and packet.window_size == self.peer_window
                and self.send_window.send_base < self.send_window.next_seq_num):
            self.dup_ack_count += 1
            if self.dup_ack_count == self.DUP_ACK_THRESHOLD:
                self.in_fast_recovery = True
                target = self.send_window.send_base
                pkt = None
                for seq, (p, _) in self.send_window.get_unacked_packets():
                    if seq == target:
                        pkt = p
                        break
                if pkt is not None:
                    try:
                        self._send_packet(pkt)
                    except (OSError, ProtocolError):
                        pass
                    self.sent_at[target] = time.monotonic()
                    self.retransmitted.add(target)
                self.ssthresh = max(float(self.MIN_CWND), self.cwnd / 2)
                self.cwnd = self.ssthresh + self.DUP_ACK_THRESHOLD * self.MSS
            elif self.in_fast_recovery:
                self.cwnd = min(float(self.MAX_CWND), self.cwnd + self.MSS)
        else:
            if self.in_fast_recovery and ack > (self.last_ack_num or 0):
                self.cwnd = self.ssthresh
                self.in_fast_recovery = False
            self.dup_ack_count = 0

        if in_window:
            old_base = self.send_window.send_base
            self.send_window.process_ack(ack)

            acked = ack - old_base
            if acked > 0 and not self.in_fast_recovery:
                if self.cwnd < self.ssthresh:
                    self.cwnd = min(float(self.MAX_CWND),
                                    self.cwnd + acked)
                else:
                    self.cwnd = min(float(self.MAX_CWND),
                                    self.cwnd + self.MSS * acked / self.cwnd)

            outstanding = {seq for seq, _ in self.send_window.get_unacked_packets()}
            for seq in list(self.sent_at):
                if seq not in outstanding:
                    self.sent_at.pop(seq, None)
                    self.retry_counts.pop(seq, None)
                    self.retransmitted.discard(seq)
                    end = self.seq_end_map.pop(seq, None)
                    if end is not None:
                        self.start_of_end.pop(end, None)

        # FIX: chỉ cập nhật window từ ACK hợp lệ (trong window)
        if in_window:
            if self.last_ack_num is None or ack >= self.last_ack_num:
                self.peer_window = packet.window_size
                self.last_ack_num = ack
                self.send_window_right = ack + packet.window_size

        self.condition.notify_all()

    def _handle_fin_locked(self, packet):
        if self.receive_window is None:
            return

        fin_seq = packet.seq_num + len(packet.payload)

        if self._fin_accepted_seq is not None:
            if fin_seq == self._fin_accepted_seq:
                ack = self._make_packet(
                    flags=TCPFlags.ACK,
                    ack_num=self.receive_window.receive_next)
                self._send_packet(ack)
                return

        expected = self.receive_window.receive_next
        if fin_seq == expected:
            self.receive_window.receive_next += 1
            self.peer_fin_received = True
            self._eof = True
            self._fin_accepted_seq = fin_seq
            self._pending_fin_seq = None
        else:
            self._pending_fin_seq = fin_seq

        ack = self._make_packet(
            flags=TCPFlags.ACK, ack_num=self.receive_window.receive_next)
        self._send_packet(ack)
        self.condition.notify_all()

    def _try_resolve_pending_fin_locked(self):
        if self._pending_fin_seq is None or self.receive_window is None:
            return
        if self.receive_window.receive_next == self._pending_fin_seq:
            self.receive_window.receive_next += 1
            self.peer_fin_received = True
            self._eof = True
            self._fin_accepted_seq = self._pending_fin_seq
            self._pending_fin_seq = None
            try:
                ack = self._make_packet(
                    flags=TCPFlags.ACK,
                    ack_num=self.receive_window.receive_next)
                self._send_packet(ack)
            except (OSError, ProtocolError):
                pass
            self.condition.notify_all()

    def _retransmit_due_locked(self):
        now = time.monotonic()
        due = []
        for seq, (pkt, _) in self.send_window.get_unacked_packets():
            sent_time = self.sent_at.get(seq, now)
            if now - sent_time >= self.rto:
                due.append((seq, pkt))

        if not due:
            return

        self.ssthresh = max(float(self.MIN_CWND), self.cwnd / 2)
        self.cwnd = float(self.MIN_CWND)
        self._rto_backoff()
        self.dup_ack_count = 0
        self.in_fast_recovery = False

        for seq, pkt in due:
            retries = self.retry_counts.get(seq, 0)
            if retries >= self.max_retries:
                self.state = "FAILED"
                self.condition.notify_all()
                raise ConnectionTimeoutError(
                    f"Không nhận được ACK cho packet seq={seq}!")
            self._send_packet(pkt)
            self.sent_at[seq] = now
            self.retry_counts[seq] = retries + 1
            self.retransmitted.add(seq)

    def send(self, data: bytes, max_payload_size=None):
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
                        raise ConnectionTimeoutError(
                            "Connection FAILED — không thể gửi tiếp!")
                    if self.state == "CLOSED":
                        raise ConnectionResetError(
                            "Connection đã đóng — không thể gửi tiếp!")
                    if self.state != "ESTABLISHED":
                        raise ProtocolError("Connection không còn hoạt động!")

                    if self.send_window_right is None:
                        self.send_window_right = (
                            self.send_window.send_base + self.peer_window)
                    peer_right = self.send_window_right
                    cong_right = self.send_window.send_base + int(self.cwnd)
                    sws_right = (self.send_window.send_base
                                 + self.send_window.send_window_size)
                    effective_right = min(peer_right, cong_right, sws_right)

                    available = effective_right - self.send_window.next_seq_num
                    if available > 0:
                        break
                    self.condition.wait(timeout=0.05)

                remaining = len(data) - offset
                payload_size = min(available, min(mss, remaining))
                payload = data[offset:offset + payload_size]
                seq_num = self.send_window.next_seq_num

                packet = self._make_packet(
                    flags=TCPFlags.ACK, seq_num=seq_num, payload=payload)

                if not self.send_window.add_packet(seq_num, packet, len(payload)):
                    self.condition.wait(timeout=0.05)
                    continue

                self.sent_at[seq_num] = time.monotonic()
                self.retry_counts[seq_num] = 0
                self.seq_end_map[seq_num] = seq_num + len(payload)
                self.start_of_end[seq_num + len(payload)] = seq_num
                self._send_packet(packet)
                offset += payload_size

        final_seq = self.send_window.next_seq_num
        with self.condition:
            while self.send_window.send_base < final_seq:
                if self.state == "FAILED":
                    raise ConnectionTimeoutError(
                        "Connection FAILED — không thể hoàn tất gửi!")
                if self.state == "CLOSED":
                    raise ConnectionResetError(
                        "Connection đã đóng — không thể hoàn tất gửi!")
                if self.state != "ESTABLISHED":
                    raise ProtocolError("Connection không còn hoạt động!")
                self.condition.wait(timeout=0.05)
        return len(data)

    def receive(self, timeout=None):
        if self.state not in ("ESTABLISHED", "FAILED") and not self.peer_fin_received:
            raise ProtocolError("Connection chưa được thiết lập!")

        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._eof and self.received_data.empty():
                return b""

            if self.received_data.empty():
                if self.state == "FAILED":
                    raise ConnectionTimeoutError(
                        "Connection FAILED (timeout/RST) — dữ liệu có thể bị cắt!")
                if self.state == "CLOSED" and not self._eof:
                    raise ConnectionResetError(
                        "Connection bị đóng bất thường (RST) — dữ liệu có thể bị cắt!")

            wait = 0.1
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if self._eof and self.received_data.empty():
                        return b""
                    if self.state in ("FAILED", "CLOSED"):
                        raise ConnectionTimeoutError(
                            "Connection chết trong khi chờ dữ liệu!")
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
                if new_win > self._last_advertised_window:
                    try:
                        self._send_packet(self._make_packet(flags=TCPFlags.ACK))
                    except (OSError, ProtocolError):
                        pass
                self._last_advertised_window = new_win
            return data

    def close(self, timeout=3):
        if self.state not in ("ESTABLISHED", "FAILED"):
            self.running = False
            self.state = "CLOSED"
            return True
        if self.state == "FAILED":
            self.running = False
            self.state = "CLOSED"
            return True

        self.fin_ack_event.clear()
        self.fin_seq_num = self.send_window.next_seq_num
        fin = self._make_packet(flags=TCPFlags.FIN | TCPFlags.ACK,
                                seq_num=self.fin_seq_num)

        deadline = time.monotonic() + timeout
        retries = 0
        while not self.fin_ack_event.is_set():
            self._send_packet(fin)
            remaining = deadline - time.monotonic()
            if remaining <= 0 or retries >= self.max_retries:
                self.running = False
                self.state = "CLOSED"
                raise ConnectionTimeoutError("Không nhận được ACK cho FIN!")
            self.fin_ack_event.wait(min(self.rto, remaining))
            retries += 1

        peer_deadline = time.monotonic() + timeout
        with self.condition:
            while not self.peer_fin_received:
                remaining = peer_deadline - time.monotonic()
                if remaining <= 0:
                    break
                self.condition.wait(timeout=min(0.1, remaining))

        graceful = self.peer_fin_received

        with self.condition:
            self.state = "TIME_WAIT"
            self._time_wait_deadline = time.monotonic() + self.TIME_WAIT_SECONDS
            self.condition.notify_all()

        if (self.receiver_thread is not None
                and self.receiver_thread is not threading.current_thread()):
            self.receiver_thread.join(timeout=self.TIME_WAIT_SECONDS + 1)
        if (self.timer_thread is not None
                and self.timer_thread is not threading.current_thread()):
            self.timer_thread.join(timeout=self.TIME_WAIT_SECONDS + 1)

        self.running = False
        with self.condition:
            self.state = "CLOSED"
            self.condition.notify_all()
        return graceful