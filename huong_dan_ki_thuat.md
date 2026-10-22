# PacketService — Tài liệu kỹ thuật & Hướng dẫn sử dụng

`PacketService` là lớp mô phỏng TCP trên nền UDP: cung cấp kết nối tin cậy, có thứ tự, có kiểm soát luồng, kiểm soát tắc nghẽn, retransmission, và đóng kết nối đối xứng.

---

## Mục lục

1. [Kiến trúc tổng quan](#1-kiến-trúc-tổng-quan)
2. [Khởi tạo & packet helpers](#nhóm-1-khởi-tạo--packet-helpers)
3. [RTO / RTT](#nhóm-2-rto--rtt)
4. [Handshake](#nhóm-3-handshake)
5. [Threads](#nhóm-4-threads)
6. [Xử lý packet đến](#nhóm-5-xử-lý-packet-đến)
7. [Retransmission](#nhóm-6-retransmission)
8. [Gửi dữ liệu](#nhóm-7-gửi-dữ-liệu)
9. [Nhận dữ liệu](#nhóm-8-nhận-dữ-liệu)
10. [Đóng kết nối](#nhóm-9-đóng-kết-nối)
11. [Luồng phối hợp](#11-luồng-phối-hợp-giữa-các-thành-phần)
12. [Hướng dẫn sử dụng](#12-hướng-dẫn-sử-dụng)

---

## 1. Kiến trúc tổng quan

`PacketService` giao tiếp với 3 module khác: `Packet` (đóng gói/giải gói, verify checksum), `SlidingWindow` (quản lý cửa sổ gửi/nhận, buffer out-of-order), và `TCPFlags` (định nghĩa cờ SYN/ACK/FIN/RST).

Mỗi connection có 2 thread chạy song song. Thread thứ nhất là `_receive_loop`, chuyên nhận packet và dispatch xử lý. Thread thứ hai là `_timer_loop`, chuyên xử lý retransmit, keepalive, idle timeout và TIME_WAIT. Dữ liệu nhận được đưa vào `received_data` queue; caller đọc ra qua `receive()`.

---

## Nhóm 1: Khởi tạo & packet helpers

### `__init__`

Hàm khởi tạo đưa connection về trạng thái `CLOSED`, chưa có window. Nó lưu socket, port và cờ `shared_socket` (cho biết server có dùng chung socket hay không), sinh ISN ngẫu nhiên 16-bit, rồi khởi tạo toàn bộ state: RTO, RTT (`srtt`, `rttvar`), congestion (`cwnd`, `ssthresh`), buffer nhận, lock/condition, và các event FIN. Ở giai đoạn này `send_window` và `receive_window` chưa được tạo — chúng đợi đến `_init_windows()` sau handshake.

### `_make_packet`

Hàm tạo đối tượng `Packet` với các trường mặc định hợp lý. Nếu caller không truyền `seq_num`, nó lấy `send_window.next_seq_num` (hoặc `self.seq_num` nếu chưa có window). Nếu không truyền `ack_num`, nó lấy `receive_window.receive_next`. Trường `window_size` luôn được tính qua `_advertised_window()` để quảng cáo buffer còn trống.

### `_advertised_window`

Hàm tính window quảng cáo (theo byte) gửi cho peer. Nó cộng `_received_bytes` (in-order đã đọc) với `_ooo_buffered_bytes()` (out-of-order đang buffer), lấy `RECEIVE_BUFFER_BYTES` trừ đi để ra chỗ trống, rồi lấy nhỏ nhất giữa chỗ trống, `receive_window_size`, và 65535 (vì header packet chỉ có 16 bit cho window). Kết quả được clamp về 0 nếu âm.

### `_ooo_buffered_bytes`

Hàm lấy tổng byte đang buffer out-of-order bằng cách gọi `receive_window.buffered_bytes()`. Nếu method này không tồn tại, nó raise `ProtocolError` thay vì fallback về 0, vì fallback sẽ khiến việc kiểm soát bộ nhớ bị under-count và dẫn tới OOM.

### `_send_packet`

Hàm pack packet rồi gửi qua UDP socket bằng `sendto`. Nếu `peer_address` chưa được set, nó raise `ProtocolError`.

### `_decode_packet`

Hàm parse raw bytes thành `Packet` và lọc packet không hợp lệ. Đầu tiên nó kiểm tra địa chỉ gửi có khớp `peer_address` không (lọc theo IP:port). Sau đó gọi `Packet.unpack(raw_data)` — hàm này tự verify checksum và raise `ValueError` nếu sai. Cuối cùng nó kiểm tra `src_port`/`dst_port` có khớp connection không. Trả về `packet` hoặc `None` (drop).

### `_init_windows`

Hàm tạo `send_window` và `receive_window` sau handshake. Nó tạo 2 `SlidingWindow` với `send_window_size` bằng `MAX_CWND` và `receive_window_size` bằng `RECEIVE_BUFFER_BYTES` (để không chặn cwnd và advertised window), set `last_ack_num` bằng `local_next_seq - 1`, và set `send_window_right` bằng `local_next_seq - 1 + peer_window` (cạnh phải ban đầu).

---

## Nhóm 2: RTO / RTT

### `_rtt_sample`

Hàm cập nhật RTO theo công thức Jacobson/Karels. Lần đo đầu tiên đặt `srtt` bằng RTT mẫu và `rttvar` bằng một nửa RTT mẫu. Các lần sau cập nhật `rttvar` theo trung bình trọng số 0.75/0.25 giữa giá trị cũ và độ lệch tuyệt đối, rồi cập nhật `srtt` theo trọng số 0.875/0.125. Cuối cùng `rto` được tính bằng `srtt + 4*rttvar`, clamp trong khoảng `MIN_RTO` và `MAX_RTO`.

### `_rto_backoff`

Hàm tăng RTO gấp đôi khi timeout (exponential backoff), nhưng không vượt quá `MAX_RTO`.

---

## Nhóm 3: Handshake

### `connect`

Hàm client chủ động mở kết nối qua 3-way handshake. Trước tiên nó guard rằng không dùng với shared socket và state phải là `CLOSED`. Sau đó set `state = "SYN_SENT"` và gửi SYN với `seq = initial_seq`. Nó lặp tối đa `max_retries` lần: mỗi lần gửi SYN rồi chờ `recvfrom` với timeout (nhân đôi sau mỗi lần timeout), parse packet, kiểm tra có `SYN|ACK` và `ack_num == initial_seq + 1` không. Nếu nhận RST thì raise `ProtocolError`. Khi nhận SYN-ACK hợp lệ, nó gọi `_init_windows(initial_seq+1, response.seq_num+1)`, gửi ACK, gọi `_start_receiver()` và trả `True`. Nếu hết retries mà không nhận được gì, nó set `state = "CLOSED"` và raise `ConnectionTimeoutError`.

### `establish_server_side`

Hàm server-side khởi tạo connection sau khi đã nhận SYN và gửi SYN-ACK (việc gửi SYN-ACK do dispatcher của server làm). Nó set `peer_address`, `dst_port`, `peer_window`, gọi `_init_windows(local_next_seq, peer_next_seq)`, set `state = "ESTABLISHED"` và gọi `_start_receiver()`. Bản thân hàm không gửi gì thêm.

### `feed_datagram`

Hàm đưa datagram vào queue cho receiver thread xử lý, chỉ dùng khi `shared_socket=True`. Nó gọi `incoming_packets.put_nowait((raw_data, address))`, bỏ qua nếu queue đầy hoặc `running=False`.

---

## Nhóm 4: Threads

### `_start_receiver`

Hàm khởi động 2 thread. Nó set `state = "ESTABLISHED"` và `running = True`, set timeout socket 0.2s nếu không dùng shared socket (để receiver loop không block vô hạn), rồi tạo `receiver_thread` chạy `_receive_loop` và `timer_thread` chạy `_timer_loop`. Cả hai là daemon thread.

### `_receive_loop`

Hàm vòng lặp nhận packet và xử lý. Khi `running` còn True, nó lấy packet từ `incoming_packets` queue (nếu shared socket) hoặc từ `sock.recvfrom(65535)` (nếu không), bắt `ConnectionResetError` trên Windows (do ICMP unreachable) để continue. Sau đó parse packet qua `_decode_packet`, bỏ nếu `None`, và gọi `_handle_packet(packet)` trong try/except để thread không chết vì lỗi đơn lẻ.

### `_timer_loop`

Hàm vòng lặp định kỳ xử lý retransmit, keepalive, idle timeout và TIME_WAIT. Mỗi `RETRANSMIT_TICK` (0.05s) nó kiểm tra: nếu đang TIME_WAIT và hết deadline thì đóng và break; nếu idle quá `IDLE_TIMEOUT` thì set `state = "FAILED"`, notify và break; nếu đến hạn keepalive thì gửi ACK rỗng; nếu có packet quá RTO thì gọi `_retransmit_due_locked()`. Nó bắt `ConnectionTimeoutError` để break (dừng thread khi FAILED), và bắt `OSError`/`ProtocolError` để lưu `_last_error` rồi tiếp tục.

---

## Nhóm 5: Xử lý packet đến

### `_handle_packet`

Hàm dispatch packet theo flag, chạy dưới `with self.condition`. Đầu tiên nó cập nhật `last_recv_time`. Nếu packet có cờ RST, nó set `state = "CLOSED"`, `running = False`, notify và return. Nếu có cờ ACK, nó gọi `_handle_ack_locked`. Nếu có payload, nó tính `free = RECEIVE_BUFFER_BYTES - (received + ooo_buffered)`; nếu `len(payload) > free` thì reject và gửi ACK cũ (không advance), ngược lại gọi `receive_window.receive_packet(seq, payload)`, gửi ACK với `receive_next` mới, đưa data in-order vào `received_data` queue và tăng `_received_bytes`, rồi gọi `_try_resolve_pending_fin_locked()` nếu có FIN pending. Nếu có cờ FIN, nó gọi `_handle_fin_locked`.

### `_handle_ack_locked`

Hàm xử lý ACK: advance window, RTT sample, fast retransmit, cập nhật flow control. Các bước:

1. Nếu ACK khớp `fin_seq_num + 1` và state hợp lệ thì set `fin_ack_event`.
2. Nếu ACK nằm trong window và `start_of_end[ack]` chưa retransmit thì lấy RTT sample.
3. Phát hiện dup ACK: nếu `ack == send_base == last_ack_num`, window không đổi và còn packet chưa ACK thì tăng `dup_ack_count`. Đủ 3 dup thì vào fast recovery: retransmit `send_base`, set `ssthresh = cwnd/2`, `cwnd = ssthresh + 3*MSS`. Mỗi dup tiếp theo trong recovery thì inflate `cwnd += MSS`.
4. Nếu ACK mới: nếu đang fast recovery và `ack > last_ack_num` thì thoát recovery và set `cwnd = ssthresh`. Gọi `process_ack(ack)` để advance `send_base`, tăng `cwnd` theo slow start (`cwnd += acked`) hoặc congestion avoidance (`cwnd += MSS*acked/cwnd`), và dọn `sent_at`, `retry_counts`, `retransmitted`, `seq_end_map`, `start_of_end` cho packet đã ACK.
5. Nếu ACK trong window thì cập nhật `peer_window`, `last_ack_num`, `send_window_right = ack + window_size`.
6. Cuối cùng gọi `notify_all()` để đánh thức `send()` đang chờ.

### `_handle_fin_locked`

Hàm xử lý FIN từ peer. Nó tính `fin_seq = packet.seq_num + len(payload)` vì FIN tiêu thụ 1 sequence number sau payload. Nếu `_fin_accepted_seq` đã set và `fin_seq` khớp thì đây là FIN trùng, nó ACK lại và return (không tăng `receive_next` lần hai). Nếu `fin_seq == receive_next` thì accept: tăng `receive_next`, set `peer_fin_received`, `_eof`, lưu `_fin_accepted_seq`. Ngược lại thì lưu `_pending_fin_seq` (FIN đến out-of-order). Cuối cùng gửi ACK với `receive_next` và notify.

### `_try_resolve_pending_fin_locked`

Hàm xử lý FIN pending khi data vừa lấp đầy khoảng trống. Nếu `receive_next == _pending_fin_seq` thì accept FIN, tăng `receive_next`, set `_eof`, gửi ACK.

---

## Nhóm 6: Retransmission

### `_retransmit_due_locked`

Hàm retransmit các packet đã quá RTO. Nó duyệt `send_window.get_unacked_packets()`, gom các packet có `now - sent_at >= rto` vào danh sách `due`. Nếu `due` rỗng thì return. Ngược lại, nó xử lý timeout event một lần cho cả đợt: set `ssthresh = max(MIN_CWND, cwnd/2)`, `cwnd = MIN_CWND`, gọi `_rto_backoff()`, reset `dup_ack_count` và `in_fast_recovery`. Sau đó với mỗi packet trong `due`: nếu `retries >= max_retries` thì set `state = "FAILED"`, notify và raise `ConnectionTimeoutError`; ngược lại gửi lại packet và cập nhật `sent_at`, `retry_counts`, `retransmitted`.

---

## Nhóm 7: Gửi dữ liệu

### `send`

Hàm gửi toàn bộ `data` và block đến khi mọi byte được ACK. Đầu tiên nó guard state (`FAILED`/`CLOSED`/`ESTABLISHED`), type và `mss`. Sau đó lặp theo `offset`: dưới `condition`, nó tính `effective_right = min(peer_right, cwnd_right, sws_right)` và `available = effective_right - next_seq_num`, chờ đến khi `available > 0`. Khi có chỗ, nó gửi partial với `payload_size = min(available, min(mss, remaining))`, tạo packet, gọi `add_packet(seq, packet, len(payload))`, ghi `sent_at`, `retry_counts`, `seq_end_map`, `start_of_end`, gửi packet và tăng `offset`. Sau khi gửi hết, nó chờ đến khi `send_base >= final_seq` trong `condition` (timer thread và ACK handler sẽ notify). Trả về `len(data)`.

---

## Nhóm 8: Nhận dữ liệu

### `receive`

Hàm lấy chunk data tiếp theo từ buffer, block có timeout. Trong vòng lặp, nếu `_eof` và queue rỗng thì trả `b""` (EOF bình thường). Nếu queue rỗng và state `FAILED` thì raise `ConnectionTimeoutError`; nếu state `CLOSED` chưa EOF thì raise `ConnectionResetError`. Nó gọi `received_data.get(timeout=wait)` với `wait ≤ 0.1s`. Khi có data, nó giảm `_received_bytes`, tính `new_win`, nếu window mở rộng thì gửi ACK window update, cập nhật `_last_advertised_window`, rồi trả data.

---

## Nhóm 9: Đóng kết nối

### `close`

Hàm đóng kết nối đối xứng (FIN handshake) và trả `graceful` flag. Nếu state không phải `ESTABLISHED`/`FAILED` thì đóng ngay và trả `True`. Ngược lại, nó set `fin_seq_num = send_window.next_seq_num`, tạo FIN packet, rồi gửi FIN và chờ ACK trong vòng lặp đến khi `fin_ack_event` set hoặc hết retries (hết retries thì set `state = "CLOSED"` và raise `ConnectionTimeoutError`). Sau khi có ACK cho FIN, nó chờ FIN từ peer tối đa `timeout` giây. `graceful = peer_fin_received`. Sau đó nó vào TIME_WAIT (set state, deadline, notify) để giữ receiver re-ACK FIN nếu peer retransmit. Nó join receiver và timer thread (timeout `TIME_WAIT_SECONDS + 1`), set `running = False`, `state = "CLOSED"`, notify, và trả `graceful`.

---

## 11. Luồng phối hợp giữa các thành phần

`send()` gửi packet đến peer. Khi ACK đến, `_receive_loop` nhận và gọi `_handle_packet`. Hàm này dispatch đến `_handle_ack_locked` (advance window, cập nhật cwnd, notify `send()` đang chờ), đến `receive_packet` (đẩy data vào `received_data` queue cho `receive()` đọc), hoặc đến `_handle_fin_locked` (set `_eof`). Song song đó, `_timer_loop` chạy độc lập, gọi `_retransmit_due_locked` khi có packet timeout, gửi keepalive định kỳ, và xử lý idle timeout hoặc TIME_WAIT expiry.

Tóm lại: `send()` chờ trong `condition` và được đánh thức bởi `_handle_ack_locked` hoặc bởi timeout. `receive()` chờ trong `received_data.get()` và được đẩy data bởi `_handle_packet`. `_timer_loop` độc lập, xử lý retransmit/keepalive/timeout mà không cần caller gọi `send()`. `close()` gửi FIN, chờ ACK qua `fin_ack_event`, chờ FIN peer qua `peer_fin_received`, rồi vào TIME_WAIT.

---

## 12. Hướng dẫn sử dụng

### 12.1. Yêu cầu

Bạn cần Python 3.8 trở lên, cùng các module `packet.py`, `constants.py`, `exceptions.py`, `algorithms.py` trong cùng project. Quan trọng nhất: `SlidingWindow` phải có method `buffered_bytes()` trả về tổng độ dài payload của các packet đang buffer out-of-order. Nếu thiếu method này, `PacketService` sẽ raise `ProtocolError` ngay khi nhận packet đầu tiên, vì nó không thể tính đúng bộ nhớ đang dùng.

### 12.2. Khởi tạo socket

Với client, bạn tự tạo một UDP socket, bind vào port ngẫu nhiên (truyền `0` cho hệ điều hành chọn), rồi tạo `PacketService` với `shared_socket=False`, truyền `src_port` là port vừa bind và `dst_port` là port server. Sau đó gọi `connect(("127.0.0.1", 9000))` để thực hiện handshake.

Với server, bạn tạo một UDP socket bind vào port cố định (ví dụ 9000), rồi chạy một vòng lặp dispatcher: nhận raw datagram bằng `recvfrom`, nếu địa chỉ gửi đã có trong bảng connections thì gọi `feed_datagram(raw, addr)` để đưa vào queue của connection đó; nếu chưa có và packet là SYN thì tạo `PacketService` mới với `shared_socket=True`, set `peer_address`, gửi SYN-ACK, gọi `establish_server_side(...)` với các sequence number thích hợp, và lưu connection vào bảng. Mọi packet khác từ địa chỉ lạ đều bị bỏ.

### 12.3. Gửi dữ liệu

Bạn gọi `send(data)` với `data` là bytes. Hàm sẽ block đến khi toàn bộ `data` được ACK. Với file lớn, nên đọc theo chunk (ví dụ 64 KB) ở caller rồi gọi `send` cho từng chunk, thay vì đọc cả file vào bộ nhớ rồi gửi một lần — vừa tiết kiệm RAM, vừa dễ hiển thị progress bar.

### 12.4. Nhận dữ liệu

Bạn gọi `receive(timeout=...)` trong vòng lặp. Có ba trường hợp cần phân biệt rõ:

- Hàm trả về `b""` nghĩa là **EOF bình thường** — peer đã gửi FIN và mọi data đã được đọc hết. Đây là dấu hiệu file/message hoàn tất.
- Hàm raise `ConnectionTimeoutError` nghĩa là peer mất mạng hoặc idle timeout. Dữ liệu có thể đã bị cắt giữa chừng.
- Hàm raise `ConnectionResetError` nghĩa là peer gửi RST hoặc đóng bất thường. Dữ liệu cũng có thể bị cắt.

Vì vậy, với file transfer, caller **bắt buộc** phải bọc `receive()` trong `try/except` để không nhầm file bị cắt với file hoàn tất. Nếu bạn chỉ viết `while data := svc.receive():` mà không bắt exception, chương trình sẽ văng ra khi peer chết giữa chừng, và bạn sẽ không biết file đã nhận được bao nhiêu.

### 12.5. Đóng kết nối

Bạn gọi `close(timeout=3)`. Hàm trả về `True` nếu đóng đối xứng (cả hai bên đã gửi FIN), `False` nếu peer không gửi FIN trong `timeout` giây (đóng cưỡng bức). Bạn nên kiểm tra giá trị trả về và log cảnh báo nếu là `False`, vì điều đó có nghĩa peer có thể đã chết hoặc chưa kịp đóng.

Lưu ý: `close()` sẽ chặn tối thiểu `TIME_WAIT_SECONDS` (mặc định 2 giây) ở cả hai phía, vì nó giữ receiver thread chạy thêm để re-ACK FIN nếu ACK cuối bị mất. Đây là hành vi có chủ ý, không phải bug.

### 12.6. Xử lý lỗi

Có bốn loại exception bạn cần bắt:

- `ProtocolError`: xảy ra khi gọi sai thứ tự (chưa connect đã send, sai state), hoặc khi `SlidingWindow` thiếu `buffered_bytes()`. Đây là lỗi logic của caller hoặc lỗi cấu hình, không phải lỗi mạng.
- `ConnectionTimeoutError`: xảy ra khi connect/send/receive timeout, hoặc peer mất mạng. Với file transfer, đây là dấu hiệu file bị cắt.
- `ConnectionResetError`: xảy ra khi peer gửi RST hoặc đóng bất thường. Cũng là dấu hiệu file bị cắt.
- `OSError`: lỗi socket (mạng down, ICMP unreachable). Thường gặp trên Windows khi peer không tồn tại.

### 12.7. Ví dụ đầy đủ — file transfer

Về mặt ý tưởng, hàm gửi file đọc file theo chunk 64 KB, gọi `send(chunk)` cho từng chunk, rồi gọi `close()` ở cuối. Hàm nhận file thì lặp `receive(timeout=10)` và append vào list, bắt `ConnectionTimeoutError` và `ConnectionResetError` để raise `IOError` với thông báo "file bị cắt" kèm số byte đã nhận so với kích thước mong đợi. Sau khi EOF, kiểm tra tổng byte nhận được có khớp kích thước mong đợi không; nếu không khớp thì cũng raise `IOError` với thông báo "EOF sớm". Cuối cùng join các chunk lại thành bytes.

### 12.8. Lưu ý & giới hạn

`PacketService` có một số giới hạn cần biết trước khi dùng:

- Không hỗ trợ sequence number wrap-around 32-bit. Giới hạn khoảng 4 GB mỗi connection. Với assignment (vài MB), an toàn.
- `connect()` không dùng được với shared socket. Client phải tự quản socket riêng.
- Keepalive là ACK rỗng, không phải TCP keepalive probe đầy đủ. Đủ để phát hiện peer chết qua idle timeout, nhưng không probe được nếu peer chỉ ACK khi có data.
- RTO dùng chung cho mọi gói, không per-segment như TCP thật.
- `close()` chặn tối thiểu `TIME_WAIT_SECONDS` (2 giây) ở cả hai phía.
- `send()` là blocking — chờ đến khi mọi byte ACK. Với file lớn, nên gửi theo chunk ở caller.
- HoL blocking vẫn còn trong một stream duy nhất. Để loại bỏ HoL giữa các stream, caller nên mở nhiều `PacketService` song song (mỗi file hoặc mỗi message một connection). Đây là cách đáp ứng yêu cầu "eliminate HoL" của assignment mà không cần refactor `PacketService` thành multi-stream.

### 12.9. Cấu hình

Bạn có thể override các class attribute trước khi dùng để thay đổi hành vi. Ví dụ, tăng `MSS` lên 1400, tăng `RECEIVE_BUFFER_BYTES` lên 512 KB, tăng `IDLE_TIMEOUT` lên 60 giây, hoặc tăng `INITIAL_CWND`/`MAX_CWND` để tăng throughput. Các thuộc tính quan trọng gồm: `MSS` (kích thước segment tối đa, mặc định 1024 byte), `RECEIVE_BUFFER_BYTES` (tổng byte đệm nhận tối đa, mặc định 256 KB), `KEEPALIVE_INTERVAL` (chu kỳ keepalive, mặc định 5 giây), `IDLE_TIMEOUT` (thời gian không nhận gì thì chuyển FAILED, mặc định 30 giây), `TIME_WAIT_SECONDS` (thời gian giữ TIME_WAIT, mặc định 2 giây), `INITIAL_RTO`/`MIN_RTO`/`MAX_RTO` (RTO ban đầu và clamp, mặc định 1.0/0.2/60 giây), `INITIAL_CWND`/`MAX_CWND` (cwnd ban đầu và tối đa, mặc định 4×MSS và 64×MSS), và `DUP_ACK_THRESHOLD` (số dup ACK để fast retransmit, mặc định 3).