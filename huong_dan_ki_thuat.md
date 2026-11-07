# PacketService — Tài liệu kỹ thuật & Hướng dẫn sử dụng

`PacketService` là một **"TCP tự cài trên UDP"**. UDP chỉ biết ném gói đi, không đảm bảo gói đến nơi, đến đúng thứ tự hay không bị lặp. `PacketService` bổ sung các tính chất mà ứng dụng cần từ TCP:

| Tính chất của TCP | Vấn đề nó giải quyết | Phần hiện thực |
|---|---|---|
| Quản lý kết nối | Hai bên phải "bắt tay" trước khi nói chuyện và "chào" trước khi cúp máy | [§3](#3-quản-lý-kết-nối) |
| Tin cậy & đúng thứ tự | Gói bị mất, lặp, đảo thứ tự | [§4](#4-độ-tin-cậy--đúng-thứ-tự) |
| Đo RTT & RTO | Chờ bao lâu thì coi là gói đã mất | [§5](#5-đo-rtt-và-tính-rto) |
| Kiểm soát luồng | Bên gửi không được làm tràn bên nhận | [§6](#6-kiểm-soát-luồng-flow-control) |
| Kiểm soát tắc nghẽn | Bên gửi không được làm nghẽn mạng | [§7](#7-kiểm-soát-tắc-nghẽn-congestion-control) |
| Mux / Demux | Nhiều client dùng chung một cổng UDP của server | [§9](#9-mux--demux-mux-py) |
| Bảo mật | Nghe lén trên mạng | [§10](#10-bảo-mật-security-py) |

## Mục lục

1. [Kiến trúc tổng quan](#1-kiến-trúc-tổng-quan)
2. [Cấu trúc packet](#2-cấu-trúc-packet)
3. [Quản lý kết nối](#3-quản-lý-kết-nối)
4. [Độ tin cậy & đúng thứ tự](#4-độ-tin-cậy--đúng-thứ-tự)
5. [Đo RTT và tính RTO](#5-đo-rtt-và-tính-rto)
6. [Kiểm soát luồng](#6-kiểm-soát-luồng-flow-control)
7. [Kiểm soát tắc nghẽn](#7-kiểm-soát-tắc-nghẽn-congestion-control)
8. [API gửi / nhận dữ liệu](#8-api-gửi--nhận-dữ-liệu)
9. [Mux / Demux](#9-mux--demux-mux-py)
10. [Bảo mật](#10-bảo-mật-security-py)
11. [Hướng dẫn sử dụng](#11-hướng-dẫn-sử-dụng)
12. [Xử lý lỗi](#12-xử-lý-lỗi)
13. [Giới hạn & cấu hình](#13-giới-hạn--cấu-hình)
14. [Tra cứu nhanh: hàm nào thuộc phần nào](#14-tra-cứu-nhanh-hàm-nào-thuộc-phần-nào)
15. [Lịch sử sửa đổi](#15-lịch-sử-sửa-đổi)

---

## 1. Kiến trúc tổng quan

### 1.1. Các file

| File | Vai trò |
|---|---|
| `packet.py` | Đóng gói / giải gói, tính và kiểm tra checksum |
| `algorithms.py` | `checksum()` và `SlidingWindow` (cửa sổ gửi, cửa sổ nhận, buffer out-of-order) |
| `constants.py` | Cờ `TCPFlags`: FIN, SYN, RST, PSH, ACK |
| `exceptions.py` | `TransportError`, `ProtocolError`, `ConnectionTimeoutError`, `PeerResetError` |
| `packet_service.py` | **Lõi TCP**: một `PacketService` = một kết nối |
| `mux.py` | `ServerMux`: nhiều kết nối dùng chung một socket UDP phía server |
| `security.py` | Mã hóa AES-GCM, trao đổi khóa, băm mật khẩu |

### 1.2. Các lớp xếp chồng

```
┌────────────────────────────────────────────────────┐
│  Ứng dụng (chat, file transfer, ...)               │
├────────────────────────────────────────────────────┤
│  SecureStream     mã hóa + tách message (security) │
├────────────────────────────────────────────────────┤
│  PacketService    "TCP": tin cậy, luồng, tắc nghẽn │
├────────────────────────────────────────────────────┤
│  ServerMux        chia datagram cho từng kết nối   │
├────────────────────────────────────────────────────┤
│  Socket UDP                                        │
└────────────────────────────────────────────────────┘
```

Client không cần `ServerMux` vì mỗi client chỉ có một socket riêng và gọi `connect()` trực tiếp.

### 1.3. Mỗi kết nối chạy 2 thread

- **`_receive_loop`** — chờ packet đến, kiểm tra hợp lệ rồi chuyển cho `_handle_packet`.
- **`_timer_loop`** — cứ mỗi 0,05 giây dậy một lần để: gửi lại gói quá hạn, gửi keepalive, phát hiện peer chết (idle), và đếm giờ TIME_WAIT.

Ứng dụng gọi `send()` / `receive()` từ thread của mình. Dữ liệu nhận được xếp vào hàng đợi `received_data`; `receive()` lấy ra từ đó.

### 1.4. Các trạng thái

```
CLOSED ──connect()──> SYN_SENT ──nhận SYN-ACK──> ESTABLISHED ──close()──> TIME_WAIT ──hết 2s──> CLOSED
   ▲                                                  │
   │            (server: establish_server_side)       ├── im lặng > 30s hoặc gửi lại quá 5 lần ──> FAILED
   └──────────────── nhận RST ────────────────────────┘                                              │
                                                                                         close() ──> CLOSED
```

Chỉ có 5 trạng thái: `CLOSED`, `SYN_SENT`, `ESTABLISHED`, `TIME_WAIT`, `FAILED`. `FAILED` nghĩa là "kết nối chết vì mạng" (khác `CLOSED` là "đã đóng").

---

## 2. Cấu trúc packet

Header cố định **18 byte** (định dạng `"!HHIIHHH"`, big-endian), theo sau là payload:

```
 0        2        4            8            12     14       16       18
 ┌────────┬────────┬────────────┬────────────┬──────┬────────┬────────┬──────────┐
 │src_port│dst_port│  seq_num   │  ack_num   │flags │ window │checksum│ payload  │
 │ 2 byte │ 2 byte │   4 byte   │   4 byte   │2 byte│ 2 byte │ 2 byte │ ≤ 1024 B │
 └────────┴────────┴────────────┴────────────┴──────┴────────┴────────┴──────────┘
```

| Trường | Ý nghĩa |
|---|---|
| `seq_num` | Số thứ tự của **byte đầu tiên** trong payload (đếm theo byte, như TCP thật) |
| `ack_num` | "Tôi đã nhận đủ mọi byte **trước** số này; hãy gửi tiếp từ số này" |
| `flags` | `SYN` mở kết nối, `ACK` có ack hợp lệ, `FIN` đóng, `RST` hủy ngay, `PSH` (chưa dùng) |
| `window` | Chỗ trống trong bộ đệm nhận của người gửi packet (xem [§6](#6-kiểm-soát-luồng-flow-control)) |
| `checksum` | Tổng bù 1 (one's complement) 16-bit của cả header và payload |

**Kiểm tra checksum:** tính checksum trên toàn bộ packet nhận được (đã gồm cả trường checksum); kết quả phải bằng 0. Sai thì `Packet.unpack` raise `ValueError` và packet bị bỏ im lặng; bên gửi sẽ gửi lại khi quá hạn.

**Quy ước FIN:** FIN tiêu thụ **1 số thứ tự**, giống TCP thật. Nếu FIN có `seq = n` thì ACK cho nó là `n + 1`.

---

## 3. Quản lý kết nối

### 3.1. Bắt tay 3 bước

```
Client                                  Server (ServerMux)
  │── SYN  seq=X ──────────────────────────>│  tạo PacketService, gửi SYN-ACK
  │<───────── SYN-ACK seq=Y, ack=X+1 ───────│  establish_server_side() → ESTABLISHED
  │── ACK  seq=X+1, ack=Y+1 ───────────────>│
  ▼ ESTABLISHED                              ▼ ESTABLISHED
```

**`connect(server_address, timeout=1, max_retries=5)`** — phía client.

1. Kiểm tra: không dùng với `shared_socket`, và trạng thái phải là `CLOSED`.
2. Gửi SYN, chờ SYN-ACK. Hết thời gian chờ thì gửi lại SYN và **nhân đôi** thời gian chờ (1s → 2s → 4s → ...).
3. Nếu nhận `ConnectionResetError` (Windows báo ICMP "port không có ai nghe"): **chờ một khoảng rồi mới thử lại**, không thử lại ngay. Nếu không, cả 5 lần thử sẽ bị đốt hết trong vài mili giây.
4. Nhận RST → `ProtocolError("Peer từ chối kết nối")`.
5. Nhận SYN-ACK hợp lệ (đúng cờ, `ack_num == X+1`) → tạo window, gửi ACK, bật 2 thread.
6. Hết số lần thử → `ConnectionTimeoutError`.

**`establish_server_side(peer_address, peer_port, local_next_seq, peer_next_seq, peer_window)`** — phía server. Hàm này **không gửi gì**. Việc gửi SYN-ACK do `ServerMux` làm ([§9](#9-mux--demux-mux-py)); hàm chỉ ghi nhận thông tin peer, tạo window và bật 2 thread.

**`feed_datagram(raw, addr)`** — chỉ dùng khi `shared_socket=True`. Mux nhận datagram từ socket chung rồi "đút" vào hàng đợi của kết nối tương ứng. Hàng đợi giới hạn 2048 gói; đầy thì bỏ gói (UDP vốn cho phép mất gói, `PacketService` sẽ gửi lại).

### 3.2. Đóng kết nối

```
A                                B
│── FIN ──────────────────────>  │
│<───────────────────── ACK ──   │
│<─────────────────── FIN ────   │   (B gọi close() khi đọc được EOF)
│── ACK ──────────────────────>  │
▼ TIME_WAIT (2s) → CLOSED        ▼ CLOSED
```

**`close(timeout=3)`**

1. Nếu kết nối không còn `ESTABLISHED`/`FAILED` thì đóng ngay, trả `True`.
2. Gửi FIN, chờ ACK. Gửi lại FIN theo RTO, tối đa `max_retries` lần.
   - Hết lần thử mà không có ACK → `ConnectionTimeoutError`.
   - Nếu giữa chừng nhận **RST** từ peer → dừng ngay, trả `False` (không còn gì để chờ).
3. Có ACK cho FIN → chờ FIN của peer tối đa `timeout` giây.
4. Vào **TIME_WAIT 2 giây**: vẫn nhận và trả lời ACK nếu peer gửi lại FIN (phòng trường hợp ACK cuối của mình bị mất).
5. Trả `True` nếu peer cũng đã gửi FIN (đóng "đối xứng"), `False` nếu không.

Một bên đóng trước vẫn đọc được dữ liệu peer gửi tiếp cho đến khi peer đóng (đóng một chiều, half-close).

**Xử lý FIN đến (`_handle_fin_locked`, `_try_resolve_pending_fin_locked`)**

- FIN đến đúng thứ tự → tăng `receive_next` thêm 1, đặt cờ EOF, gửi ACK.
- FIN đến **sớm** (dữ liệu trước nó chưa đủ) → ghi nhớ vị trí FIN; khi dữ liệu lấp đầy chỗ trống mới chấp nhận FIN.
- FIN **trùng** (đã nhận rồi) → chỉ ACK lại, không đếm lần hai.

### 3.3. Phát hiện peer chết

- **Keepalive:** cứ 5 giây gửi một ACK rỗng. Việc này vừa báo "tôi còn sống" vừa phát lại giá trị window hiện tại (xem [§6.3](#63-cửa-sổ-bằng-0)).
- **Idle timeout:** 30 giây không nhận được gì từ peer → state `FAILED`. Từ đó `send()` và `receive()` raise `ConnectionTimeoutError`.
- **RST:** peer gửi RST → state `CLOSED` ngay lập tức, `receive()` raise `PeerResetError`.

---

## 4. Độ tin cậy & đúng thứ tự

### 4.1. Ý tưởng

Mọi byte có một số thứ tự. Bên nhận trả lời "tôi đã có đủ mọi thứ đến byte N" (**ACK tích lũy**). Bên gửi giữ lại các gói chưa được ACK để gửi lại nếu cần.

### 4.2. Bên gửi: `SlidingWindow` (phía gửi)

```
    đã ACK     │   đã gửi, chưa ACK    │   được phép gửi    │ chưa được phép
 ──────────────┼───────────────────────┼────────────────────┼──────────────
            send_base              next_seq_num      effective_right
```

- `send_base`: byte đầu tiên chưa được ACK.
- `next_seq_num`: byte tiếp theo sẽ gửi.
- `effective_right = min(cửa sổ của peer, cwnd, kích thước cửa sổ gửi)`: giới hạn phải. Ba giới hạn này nói về flow control, congestion control và dung lượng bộ nhớ gửi.

### 4.3. Bên nhận: `SlidingWindow` (phía nhận)

- Gói **đúng thứ tự** (`seq == receive_next`): giao cho ứng dụng, đẩy `receive_next` lên.
- Gói **đến sớm** (có "lỗ" phía trước): **giữ trong buffer**, chưa giao. Khi lỗ được lấp, giao cả chùm một lúc.
- Gói **cũ / trùng / chồng lấn**: bỏ, nhưng vẫn ACK lại để bên gửi biết.

Mỗi gói nhận được đều được ACK ngay. Nếu gói đến sớm, ACK vẫn mang `receive_next` cũ — đó chính là **dup ACK**, tín hiệu cho bên gửi biết "có một gói bị mất".

### 4.4. Gửi lại gói mất

Có hai cơ chế, cái nào đến trước thì chạy trước:

**(a) Fast retransmit — nhanh, dựa trên dup ACK.** Nhận đủ 3 dup ACK liên tiếp (`DUP_ACK_THRESHOLD`) thì gửi lại ngay gói ở `send_base`, không chờ hết giờ.

Chỉ **ACK thuần** mới được tính là dup ACK (không có payload, không phải SYN/FIN). Packet dữ liệu của peer cũng mang cờ ACK nên nếu đếm cả chúng, khi hai bên cùng gửi dữ liệu sẽ có "fast retransmit giả" làm cwnd tụt vô cớ.

**(b) Retransmission timeout (RTO) — chậm, làm lưới an toàn.** Có **một timer cho cả kết nối**, tính từ lúc gửi gói cũ nhất chưa ACK (hoặc từ lần ACK tiến lên gần nhất). Hết `rto` giây mà chưa có ACK mới:

1. Chỉ gửi lại **một gói**: gói cũ nhất chưa ACK.
2. Tăng gấp đôi `rto` (exponential backoff).
3. Giảm cwnd về 1 MSS (xem [§7](#7-kiểm-soát-tắc-nghẽn-congestion-control)).
4. Sau `max_retries` (mặc định 5) lần gửi lại **cùng một gói** mà vẫn không có ACK → state `FAILED`, `ConnectionTimeoutError`.

Vì bên nhận đã giữ các gói đến sớm trong buffer, nên chỉ cần gửi lại gói bị mất; ACK tiếp theo sẽ "nhảy" qua các gói đã có. Cách này gửi lại **đúng phần thiếu**, không gửi thừa.

### 4.5. Dọn dẹp

Khi ACK tiến lên, các bản ghi của gói đã ACK (`sent_at`, `retry_counts`, `retransmitted`, `seq_end_map`, `start_of_end`) được xóa để bộ nhớ không phình ra.

---

## 5. Đo RTT và tính RTO

**RTT** (round-trip time) là thời gian từ lúc gửi gói đến lúc nhận ACK cho nó. **RTO** là thời gian chờ trước khi coi gói đã mất. RTO phải đủ lớn để không gửi lại oan, đủ nhỏ để phục hồi nhanh.

### 5.1. Công thức (Jacobson/Karels) — `_rtt_sample`

```
Lần đo đầu:   srtt = rtt          rttvar = rtt / 2
Các lần sau:  rttvar = 0.75·rttvar + 0.25·|srtt − rtt|
              srtt   = 0.875·srtt + 0.125·rtt
RTO = srtt + 4·rttvar            (giới hạn trong [0.2s, 60s])
```

`srtt` là RTT trung bình làm mượt; `rttvar` đo độ dao động. RTO ban đầu là 1 giây.

### 5.2. Quy tắc Karn (mở rộng): khi nào KHÔNG lấy mẫu

Một ACK **không** được dùng làm mẫu RTT nếu nó ACK qua một gói **đã bị gửi lại**. Lý do: không biết ACK này trả lời cho lần gửi nào, và với ACK tích lũy thì thời gian đo còn gồm cả thời gian chờ lấp lỗ. Nếu vẫn lấy mẫu, RTO sẽ phình lên hàng chục giây và kết nối gần như đứng.

### 5.3. Hết backoff

Khi hết giờ thì `rto` nhân đôi. Khi ACK **tiến lên** (có dữ liệu mới được xác nhận), `rto` được tính lại từ `srtt` và `rttvar`, quay về mức bình thường.

---

## 6. Kiểm soát luồng (flow control)

**Mục đích:** bên nhận chậm không bị bên gửi nhanh làm tràn bộ đệm.

### 6.1. Cách hoạt động

Mỗi packet gửi đi mang trường `window` = "tôi còn chỗ nhận bao nhiêu byte". Bên gửi không được có quá `window` byte đang bay.

`_advertised_window()` tính như sau:

```
đã_dùng = _received_bytes (đã nhận đủ nhưng app chưa đọc) + byte out-of-order đang giữ
chỗ_trống = RECEIVE_BUFFER_BYTES (256 KB) − đã_dùng
window = min(chỗ_trống, receive_window_size, 65535)       # ≥ 0
```

Giới hạn 65535 vì trường `window` trong header chỉ có 16 bit.

`_ooo_buffered_bytes()` gọi `SlidingWindow.buffered_bytes()`. Nếu hàm này không tồn tại sẽ raise `ProtocolError` (không âm thầm trả về 0, vì sẽ đếm thiếu bộ nhớ).

### 6.2. Bảo vệ bên nhận

Nếu một gói có payload lớn hơn chỗ trống thì bị **từ chối**: không nhận, chỉ gửi lại ACK cũ kèm window hiện tại.

### 6.3. Cửa sổ bằng 0

Bên nhận đầy → quảng cáo `window = 0` → bên gửi dừng. Khi ứng dụng gọi `receive()` lấy dữ liệu ra, chỗ trống tăng lại và bên nhận gửi một **ACK cập nhật window**. Để biết "window peer đã biết là bao nhiêu", `_make_packet` ghi nhớ giá trị window mỗi lần gửi (`_last_advertised_window`); chỉ khi window mới **lớn hơn** giá trị đó mới gửi bản cập nhật.

Nếu ACK cập nhật này bị mất, bên gửi không bị treo mãi: keepalive 5 giây một lần cũng mang window mới nhất, nên tối đa sau 5 giây kết nối tự phục hồi.

---

## 7. Kiểm soát tắc nghẽn (congestion control)

**Mục đích:** bên gửi không được dồn quá nhiều dữ liệu vào mạng làm mạng nghẽn (mạng nghẽn → mất gói → gửi lại → càng nghẽn). Khác với flow control (bảo vệ bên nhận), phần này bảo vệ **mạng**.

Bên gửi giữ một biến **`cwnd`** (congestion window): số byte tối đa được phép đang bay. Thuật toán là **Reno với NewReno** (kiểu cải tiến xử lý mất nhiều gói).

| Giai đoạn | Khi nào | `cwnd` thay đổi |
|---|---|---|
| **Slow start** | `cwnd < ssthresh` (lúc mới bắt đầu) | Mỗi ACK: `cwnd += số byte vừa được ACK` → tăng gần như gấp đôi mỗi RTT |
| **Congestion avoidance** | `cwnd ≥ ssthresh` | Mỗi ACK: `cwnd += MSS·acked/cwnd` → tăng khoảng 1 MSS mỗi RTT |
| **Fast recovery** | Nhận đủ 3 dup ACK | `ssthresh = cwnd/2`; `cwnd = ssthresh + 3·MSS`; mỗi dup ACK tiếp: `cwnd += MSS` |
| **Timeout (RTO)** | Hết giờ không có ACK | `ssthresh = cwnd/2`; `cwnd = 1 MSS`; về lại slow start |

Giá trị: `INITIAL_CWND = 4·MSS`, `MIN_CWND = MSS`, `MAX_CWND = 64·MSS`; `ssthresh` ban đầu = `MAX_CWND`.

### 7.1. NewReno: mất nhiều gói trong một cửa sổ

Khi vào fast recovery, bên gửi ghi nhớ `_recover` = `next_seq_num` lúc đó (vùng cần phục hồi). Sau đó:

- ACK mới **bằng hoặc vượt** `_recover` ("full ACK") → mọi gói đã được lấp, `cwnd = ssthresh`, thoát recovery.
- ACK mới **nhỏ hơn** `_recover` ("partial ACK") → vẫn còn gói mất khác trong cùng đợt. Gửi lại ngay gói tiếp theo bị thiếu, **ở lại** recovery, không chờ RTO.

Reno thuần thoát recovery ngay ở partial ACK nên khi mất nhiều gói phải chờ RTO — rất chậm.

### 7.2. Phối hợp ba giới hạn

Mỗi lần gửi, `send()` tính:

```
effective_right = min( send_window_right ,  send_base + cwnd ,  send_base + send_window_size )
                      └ flow control ┘     └ congestion ┘      └ bộ nhớ cửa sổ gửi ┘
available = effective_right − next_seq_num
```

Không phải CUBIC hay BBR: các thuật toán đó có thể gắn thêm bằng cách thay phần cập nhật `cwnd` trong `_handle_ack_locked` (xem [§13](#13-giới-hạn--cấu-hình)).

---

## 8. API gửi / nhận dữ liệu

### 8.1. `send(data, max_payload_size=None)`

Gửi toàn bộ `data` (kiểu `bytes`) và **chặn cho đến khi mọi byte được ACK**; trả về `len(data)`.

- **Atomic:** `send()` giữ một khóa riêng (`_send_lock`), nên nhiều thread gọi `send()` cùng lúc sẽ được xếp hàng; byte của hai lần gửi **không bị xen kẽ** vào nhau. Nhờ vậy một message gửi bằng một lần `send()` luôn liền mạch trong stream.
- Mỗi vòng, `send()` chờ đến khi `available > 0`, cắt một đoạn tối đa `MSS` (1024 byte) và gửi.
- Lỗi: `ConnectionTimeoutError` nếu `FAILED`; `ConnectionResetError` nếu `CLOSED` (kể cả khi chưa từng kết nối); `ProtocolError` nếu đang ở trạng thái khác `ESTABLISHED` (ví dụ `TIME_WAIT`); `TypeError` nếu `data` không phải `bytes`.

### 8.2. `receive(timeout=None)`

Lấy khối dữ liệu tiếp theo từ hàng đợi, chờ tối đa `timeout` giây. Kết quả:

| Kết quả | Ý nghĩa |
|---|---|
| `bytes` không rỗng | Dữ liệu |
| `b""` | **EOF bình thường**: peer đã gửi FIN và mọi dữ liệu đã được đọc |
| `ConnectionTimeoutError` | Hết `timeout` mà chưa có dữ liệu, hoặc kết nối `FAILED` (peer mất mạng) |
| `PeerResetError` | Peer gửi RST (là một dạng của `ConnectionResetError`) |
| `ConnectionResetError` | Kết nối bị đóng bất thường khi chưa có EOF |
| `ProtocolError` | Chưa từng kết nối |

Dữ liệu còn trong hàng đợi luôn được trả hết trước khi báo lỗi reset/EOF.

Mỗi lần `receive()` lấy dữ liệu ra, bộ đệm trống thêm và có thể gửi ACK cập nhật window ([§6.3](#63-cửa-sổ-bằng-0)).

### 8.3. Về Head-of-Line blocking

Trong **một** kết nối, dữ liệu phải giao đúng thứ tự nên một gói mất sẽ làm các gói sau nó phải chờ. Đây là bản chất của luồng tin cậy. Để các luồng không chặn nhau (ví dụ chat không bị kẹt sau một file lớn), hãy **dùng nhiều `PacketService` song song** — mỗi file hoặc mỗi loại dữ liệu một kết nối. Mỗi kết nối có socket riêng ở client, nên `ServerMux` (khóa theo `(ip, port)`) xử lý được chúng như các client độc lập.

---

## 9. Mux / Demux (`mux.py`)

Server chỉ có **một** cổng UDP nhưng phục vụ nhiều client. Mỗi client là một `PacketService` riêng. `ServerMux` làm hai việc:

- **Demux** (chia ra): nhận datagram từ socket chung, tra bảng `(ip, port) → PacketService` rồi gọi `feed_datagram`.
- **Mux** (gộp vào): các `PacketService` gửi thẳng bằng `sock.sendto()` trên cùng socket (an toàn khi nhiều thread cùng gọi).

```
 client A ─┐                                ┌─> PacketService A ─┐
 client B ─┼──> UDP :9000 ──> ServerMux ───┤   PacketService B ─┼──> accept()
 client C ─┘                (tra theo addr) └─> PacketService C ─┘
```

### 9.1. API

| Hàm | Công dụng |
|---|---|
| `ServerMux(sock, max_connections=64)` | `sock` là socket UDP đã `bind` |
| `start()` / `stop()` | Bật / tắt thread nhận (không đóng socket; socket do caller quản lý) |
| `accept(timeout)` | Trả `(PacketService, addr)` của kết nối mới, hoặc `None` nếu hết giờ |
| `connections()` | Danh sách `[(addr, PacketService)]` — dùng để theo dõi IP và port client đang nối |
| `get(addr)` | Lấy kết nối theo địa chỉ, hoặc `None` |
| `remove(addr, close=False)` | Gỡ một kết nối khỏi bảng |

### 9.2. Quy tắc xử lý datagram

| Gặp phải | Xử lý |
|---|---|
| Không giải gói được (hỏng checksum, rác) | Bỏ |
| SYN từ địa chỉ **mới** | Tạo `PacketService`, gửi SYN-ACK, đưa vào `accept()` |
| SYN **trùng** (cùng địa chỉ, cùng ISN) | Gửi lại đúng SYN-ACK cũ (SYN-ACK trước đó có thể đã mất) |
| SYN cùng địa chỉ nhưng ISN **khác** | Client khởi động lại: hủy kết nối cũ, tạo mới |
| SYN khi số kết nối đạt `max_connections` | Trả **RST** (server đầy) |
| Packet khác từ địa chỉ **đã có** kết nối | `feed_datagram` |
| Packet khác từ địa chỉ **lạ** | Trả **RST** (trừ khi chính nó là RST, để tránh hai bên cứ RST qua lại) |

Bảng kết nối được dọn tự động: kết nối `CLOSED` bị xóa; kết nối `FAILED` được giữ thêm 10 giây (`FAILED_GRACE`) để ứng dụng kịp nhận lỗi rồi mới dọn.

---

## 10. Bảo mật (`security.py`)

Dùng thư viện `cryptography` (`pip install cryptography`); không tự cài đặt thuật toán mã hóa.

### 10.1. Ba mức dùng

| Nhu cầu | Dùng | Cơ chế |
|---|---|---|
| Lưu / kiểm tra mật khẩu đăng nhập | `hash_password()`, `verify_password()` | PBKDF2-HMAC-SHA256, salt ngẫu nhiên, 200 000 vòng |
| Gói UDP độc lập (heartbeat, discovery broadcast) | `CryptoBox` | AES-256-GCM, nonce ngẫu nhiên trong mỗi gói |
| Luồng tin cậy qua `PacketService` | `SecureStream` | Trao đổi khóa X25519 + AES-256-GCM |

### 10.2. `SecureStream`: kênh mã hóa trên `PacketService`

`PacketService.receive()` trả về dữ liệu theo từng khối tùy ý, không theo ranh giới message. `SecureStream` thêm phần **tách message** (4 byte độ dài ở đầu) và **mã hóa**.

```
Client                                       Server
  │── public key (32 byte) ──────────────────>│
  │<────────────────── public key (32 byte) ──│   → mỗi bên tính khóa chung (ECDH)
  │   HKDF → khóa client→server, khóa server→client
  │── [độ dài][AES-GCM(message)] ────────────>│
  │<────────────── [độ dài][AES-GCM(message)]│
```

| Hàm | Công dụng |
|---|---|
| `SecureStream(conn, is_server, psk=b"")` | Bọc một `PacketService` |
| `handshake(timeout=10)` | Trao đổi khóa; gọi một lần sau khi kết nối xong |
| `send_message(bytes)` | Mã hóa và gửi một message |
| `recv_message(timeout=None)` | Nhận và giải mã một message; `None` nếu peer đóng sạch |
| `close()` | Đóng kết nối bên dưới |

Tính chất:

- **Mỗi chiều một khóa**, nonce là bộ đếm tăng dần → không bao giờ dùng lại cặp (khóa, nonce).
- **Chống sửa / đảo / mất frame:** AES-GCM có thẻ xác thực; bất kỳ thay đổi nào sẽ làm `recv_message` raise `SecurityError`.
- **Khóa phiên tạm thời** (mỗi kết nối một khóa mới): lộ khóa của kết nối này không làm lộ kết nối khác.
- **`psk`** (mật khẩu chung, tùy chọn): trộn vào bước sinh khóa. Hai bên đặt khác nhau → giải mã thất bại.
- `recv_message` timeout giữa chừng không làm lệch ranh giới message; gọi lại là đọc tiếp được.

### 10.3. Giới hạn

Handshake X25519 **chống nghe lén** nhưng nếu không đặt `psk` thì **không chống được kẻ đứng giữa** (MITM) chủ động thay public key. Trong mạng LAN nên đặt `psk`.

---

## 11. Hướng dẫn sử dụng

### 11.1. Yêu cầu

- Python 3.8 trở lên.
- Các file `packet.py`, `constants.py`, `exceptions.py`, `algorithms.py`, `packet_service.py`, `mux.py`, `security.py` trong thư mục `shared/`, và thư mục này nằm trong `sys.path`.
- `pip install cryptography` (cho `security.py`).
- `SlidingWindow` phải có `buffered_bytes()`.
- Các module import nhau theo kiểu phẳng (`from packet import Packet`). **Hãy import cùng một kiểu ở mọi nơi**: nếu chỗ này `import exceptions`, chỗ khác `import shared.exceptions` thì Python tạo hai class `ProtocolError` khác nhau và `except` sẽ không bắt được.

### 11.2. Client

```python
import socket
from packet_service import PacketService
from security import SecureStream

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("0.0.0.0", 0))                      # 0 = hệ điều hành chọn cổng
conn = PacketService(sock, sock.getsockname()[1], 9000)   # shared_socket=False
conn.connect(("127.0.0.1", 9000))              # bắt tay 3 bước

chan = SecureStream(conn, is_server=False, psk=b"mat-khau-chung")
chan.handshake()

chan.send_message(b"xin chao")
reply = chan.recv_message(timeout=10)

chan.close()
```

### 11.3. Server

```python
import socket, threading
from mux import ServerMux
from security import SecureStream

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("0.0.0.0", 9000))
mux = ServerMux(sock, max_connections=64)
mux.start()

def handle(conn, addr):
    chan = SecureStream(conn, is_server=True, psk=b"mat-khau-chung")
    try:
        chan.handshake()
        while True:
            msg = chan.recv_message(timeout=60)
            if msg is None:          # client đóng sạch
                break
            chan.send_message(b"echo: " + msg)
    except Exception as e:           # timeout / reset / giải mã lỗi: không làm sập server
        print(addr, "lỗi:", e)
    finally:
        chan.close()

while True:
    item = mux.accept(timeout=1)
    if item is None:
        continue
    conn, addr = item
    threading.Thread(target=handle, args=(conn, addr), daemon=True).start()
```

### 11.4. Gửi và nhận file

Gửi theo chunk để tiết kiệm RAM và có chỗ cập nhật thanh tiến độ:

```python
CHUNK = 64 * 1024
def send_file(chan, path):
    size = os.path.getsize(path)
    chan.send_message(json.dumps({"name": os.path.basename(path), "size": size}).encode())
    sent = 0
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK):
            chan.send_message(chunk)
            sent += len(chunk)           # cập nhật progress bar tại đây

def recv_file(chan, out_path):
    meta = json.loads(chan.recv_message(timeout=10))
    got = 0
    with open(out_path, "wb") as f:
        while got < meta["size"]:
            try:
                chunk = chan.recv_message(timeout=10)
            except (ConnectionTimeoutError, ConnectionResetError):
                raise IOError(f"file bị cắt: nhận {got}/{meta['size']} byte")
            if chunk is None:
                raise IOError(f"EOF sớm: nhận {got}/{meta['size']} byte")
            f.write(chunk); got += len(chunk)
```

**Quan trọng:** luôn bọc việc nhận trong `try/except` và so sánh số byte nhận với kích thước mong đợi. Nếu chỉ viết `while data := conn.receive():`, một peer chết giữa chừng sẽ làm chương trình văng, hoặc tệ hơn là coi file bị cắt như đã nhận xong.

### 11.5. Đóng kết nối

`close()` trả `True` nếu hai bên cùng đóng, `False` nếu peer không gửi FIN kịp hoặc đã RST. Nên ghi log khi `False`. Lưu ý `close()` luôn mất tối thiểu khoảng 2 giây (TIME_WAIT) — đây là chủ ý.

### 11.6. Dùng `PacketService` trực tiếp (không mã hóa)

Vẫn dùng được `conn.send(bytes)` / `conn.receive(timeout)`. Khi đó dữ liệu là luồng byte thô, **ứng dụng tự lo việc tách message** và dữ liệu **không được mã hóa**.

---

## 12. Xử lý lỗi

| Exception | Khi nào | Ghi chú |
|---|---|---|
| `ProtocolError` | Gọi sai thứ tự (gửi/nhận khi chưa connect, `connect()` với shared socket), `SlidingWindow` thiếu `buffered_bytes()`, nhận RST khi đang `connect()` | Lỗi lập trình hoặc từ chối kết nối; không nên thử lại mù quáng |
| `ConnectionTimeoutError` | `connect()` không có SYN-ACK; `send()` không có ACK sau `max_retries`; `receive()` hết giờ; peer im lặng quá 30s; `close()` không có ACK cho FIN | Với file: coi như file bị cắt |
| `PeerResetError` | Peer gửi RST | Là con của `ConnectionResetError` |
| `ConnectionResetError` | Kết nối đã `CLOSED` khi đang gửi/nhận | |
| `SecurityError` | Giải mã thất bại (sai khóa, dữ liệu bị sửa), frame hỏng, stream đứt giữa message | Nên đóng kết nối |
| `OSError` | Lỗi socket (mạng sập, ICMP unreachable) | Hay gặp trên Windows |

Thread nhận và thread timer **bắt mọi exception** của từng packet và ghi vào `_last_error` rồi chạy tiếp, để một packet lỗi không làm chết cả kết nối.

---

## 13. Giới hạn & cấu hình

### 13.1. Giới hạn

- **Không wrap-around số thứ tự 32-bit:** tối đa khoảng 4 GB mỗi kết nối. Vượt quá, `struct.pack` lỗi.
- **ACK tích lũy, không có SACK:** khi mất nhiều gói trong một cửa sổ, các lỗ được lấp lần lượt (nhờ NewReno) thay vì cùng lúc.
- **Congestion control là Reno/NewReno**, chưa phải CUBIC hoặc BBR.
- **Gửi byte lẻ:** nếu cửa sổ còn trống rất nhỏ, có thể gửi gói nhỏ hơn MSS (chưa chống "silly window syndrome").
- **`send()` chặn** đến khi mọi byte được ACK; hãy gửi theo chunk.
- **Keepalive** là ACK rỗng, không phải probe đầy đủ như TCP thật.
- **Handshake không xác thực** nếu không có `psk` ([§10.3](#103-giới-hạn)).

### 13.2. Cấu hình

Các thuộc tính của lớp, ghi đè **trước** khi tạo kết nối:

| Thuộc tính | Mặc định | Ý nghĩa |
|---|---|---|
| `MSS` | 1024 | Payload tối đa mỗi packet |
| `RECEIVE_BUFFER_BYTES` | 256 KB | Bộ đệm nhận |
| `KEEPALIVE_INTERVAL` | 5 s | Chu kỳ keepalive |
| `IDLE_TIMEOUT` | 30 s | Im lặng bao lâu thì `FAILED` |
| `TIME_WAIT_SECONDS` | 2 s | Thời gian giữ TIME_WAIT |
| `RETRANSMIT_TICK` | 0.05 s | Chu kỳ timer thread |
| `INITIAL_RTO` / `MIN_RTO` / `MAX_RTO` | 1.0 / 0.2 / 60 s | RTO khởi đầu và giới hạn |
| `INITIAL_CWND` / `MIN_CWND` / `MAX_CWND` | 4·MSS / MSS / 64·MSS | Cửa sổ tắc nghẽn |
| `DUP_ACK_THRESHOLD` | 3 | Số dup ACK để fast retransmit |
| `max_retries` (tham số `connect()`) | 5 | Số lần gửi lại tối đa |
| `ServerMux.FAILED_GRACE` | 10 s | Giữ kết nối `FAILED` trước khi dọn |

Lưu ý `MAX_CWND` bị chặn thêm bởi cửa sổ quảng cáo tối đa 65535 byte (giới hạn 16 bit của header), nên tăng `MAX_CWND` quá 64 KB không làm tăng thông lượng nếu không đổi định dạng header.

---

## 14. Tra cứu nhanh: hàm nào thuộc phần nào

| Hàm / lớp | Phần | Ghi chú |
|---|---|---|
| `__init__` | Chung | Khởi tạo state, chưa có window |
| `_make_packet`, `_send_packet`, `_decode_packet` | Packet | Dựng packet kèm window quảng cáo; gửi; lọc theo địa chỉ, cổng, checksum |
| `connect`, `establish_server_side`, `feed_datagram` | [§3](#3-quản-lý-kết-nối) | Bắt tay |
| `close`, `_handle_fin_locked`, `_try_resolve_pending_fin_locked` | [§3](#3-quản-lý-kết-nối) | Đóng kết nối |
| `_start_receiver`, `_receive_loop`, `_timer_loop` | [§1.3](#13-mỗi-kết-nối-chạy-2-thread) | Thread |
| `_init_windows` | [§4](#4-độ-tin-cậy--đúng-thứ-tự) | Tạo 2 `SlidingWindow` |
| `_handle_packet` | [§4](#4-độ-tin-cậy--đúng-thứ-tự) | Điều phối theo cờ |
| `_handle_ack_locked` | [§4](#4-độ-tin-cậy--đúng-thứ-tự), [§5](#5-đo-rtt-và-tính-rto), [§6](#6-kiểm-soát-luồng-flow-control), [§7](#7-kiểm-soát-tắc-nghẽn-congestion-control) | Trung tâm: xử lý ACK |
| `_retransmit_due_locked`, `_retransmit_seq_locked` | [§4.4](#44-gửi-lại-gói-mất) | Gửi lại |
| `_rtt_sample`, `_rto_backoff` | [§5](#5-đo-rtt-và-tính-rto) | RTT / RTO |
| `_advertised_window`, `_ooo_buffered_bytes` | [§6](#6-kiểm-soát-luồng-flow-control) | Flow control |
| `send`, `receive` | [§8](#8-api-gửi--nhận-dữ-liệu) | API cho ứng dụng |
| `ServerMux` | [§9](#9-mux--demux-mux-py) | |
| `SecureStream`, `CryptoBox`, `hash_password` | [§10](#10-bảo-mật-security-py) | |

### Chuỗi sự kiện khi gửi một đoạn dữ liệu

1. `send()` chờ đến khi `available > 0`, tạo packet và gửi đi, ghi `sent_at`.
2. Packet đến peer. `_receive_loop` → `_handle_packet` → `receive_packet`: nếu đúng thứ tự thì xếp vào `received_data`, gửi ACK.
3. ACK về bên gửi: `_handle_ack_locked` tiến `send_base`, lấy mẫu RTT, tăng `cwnd`, cập nhật window của peer, `notify_all()` đánh thức `send()`.
4. Nếu ACK không về: `_timer_loop` → `_retransmit_due_locked` gửi lại sau `rto`; hoặc 3 dup ACK kích hoạt fast retransmit.
5. Ứng dụng bên nhận gọi `receive()` để lấy dữ liệu; chỗ trống tăng và có thể gửi ACK cập nhật window.

---

## 15. Lịch sử sửa đổi

So với bản đầu tiên của `packet_service.py`:

| # | Thay đổi | Lý do |
|---|---|---|
| 1 | Dup ACK chỉ tính cho ACK thuần (không payload, không SYN/FIN) | Packet dữ liệu cũng mang cờ ACK nên khi truyền hai chiều gây fast retransmit giả, `cwnd` tụt |
| 2 | `send()` atomic bằng `_send_lock` | Hai thread gọi `send()` cùng lúc làm byte bị xen kẽ, hỏng ranh giới message |
| 3 | RTO: một timer, chỉ gửi lại gói cũ nhất | Bản cũ gửi lại cả loạt, backoff nhiều lần, gửi thừa gói đã nằm ở buffer bên nhận |
| 4 | Bỏ mẫu RTT khi ACK đi qua gói đã gửi lại; hết backoff khi ACK tiến lên | RTO phình đến hàng chục giây khi mất gói, kết nối gần như đứng |
| 5 | Fast recovery theo NewReno (xử lý partial ACK) | Mất nhiều gói trong một cửa sổ phải chờ RTO |
| 6 | `_last_advertised_window` cập nhật trong `_make_packet` | Window update có thể không được gửi sau khi cửa sổ đầy rồi mở lại |
| 7 | Khởi tạo `last_ack_num`, `send_window_right` đúng (bỏ lệch 1) | Dup ACK đầu tiên bị bỏ sót; cửa sổ gửi ban đầu thiếu 1 byte |
| 8 | `receive()` sau RST ném `PeerResetError`, vẫn trả hết dữ liệu còn trong hàng đợi trước | Bản cũ ném `ProtocolError("chưa thiết lập")` gây hiểu nhầm |
| 9 | `close()` thoát ngay khi nhận RST | Tránh chờ hết timeout rồi báo lỗi |
| 10 | Thread nhận và timer bắt mọi `Exception` | Lỗi ngoài `OSError`/`ProtocolError` (ví dụ `struct.error`) từng làm thread chết im lặng |
| 11 | `connect()` chờ giữa các lần thử khi gặp `ConnectionResetError` | 5 lần thử bị đốt hết trong vài mili giây khi server chưa chạy |
| 12 | Hàng đợi `incoming_packets` giới hạn 2048 | Hàng đợi không giới hạn làm nhánh `Full` không bao giờ chạy, có nguy cơ phình RAM |
| 13 | Thêm `mux.py` và `security.py` | Hoàn thiện phần còn thiếu theo đề bài |