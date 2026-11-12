# HUDP Platform — Tài liệu Đặc tả Kỹ thuật & Kiến trúc Hệ thống

Tài liệu này mô tả chi tiết thiết kế, thuật toán và luồng hoạt động của toàn bộ hệ thống **HUDP Collaboration Platform**, bao gồm **3 tầng chính**:
1. **Tầng Giao thức Lõi & Bảo mật (`shared/`)**: Giao thức truyền vận tin cậy tự cài đặt trên UDP (`PacketService`) và kênh mã hóa đầu cuối (`SecureStream`).
2. **Tầng Máy chủ (`server/`)**: Kiến trúc định tuyến đa kết nối trên một socket UDP duy nhất, quản lý phiên người dùng và điều phối tín hiệu.
3. **Tầng Máy khách (`client/` & `ui/`)**: Kiến trúc đa luồng kết hợp cầu nối HTTP-UDP cho Web UI và cơ chế truyền tệp ngang hàng (P2P).

---

## Mục lục

- **Phần I: Lõi Giao thức Truyền vận & Bảo mật (`shared/`)**
  1. [Tổng quan kiến trúc phân tầng](#1-tổng-quan-kiến-trúc-phân-tầng)
  2. [Cấu trúc gói tin & Số học Wrap-around 32-bit](#2-cấu-trúc-gói-tin--số-học-wrap-around-32-bit)
  3. [Quản lý kết nối & Máy trạng thái](#3-quản-lý-kết-nối--máy-trạng-thái)
  4. [Độ tin cậy, Sliding Window & Truyền lại](#4-độ-tin-cậy-sliding-window--truyền-lại)
  5. [Đo RTT và tính RTO](#5-đo-rtt-và-tính-rto)
  6. [Kiểm soát luồng (Flow Control)](#6-kiểm-soát-luồng-flow-control)
  7. [Kiểm soát tắc nghẽn (CUBIC & NewReno)](#7-kiểm-soát-tắc-nghẽn-cubic--newreno)
  8. [Tầng Bảo mật (`SecureStream` & `CryptoBox`)](#8-tầng-bảo-mật-securestream--cryptobox)
  9. [Giao thức bản tin ứng dụng (`schemas.py`)](#9-giao-thức-bản-tin-ứng-dụng-schemaspy)
- **Phần II: Kiến trúc Phía Máy chủ (`server/`)**
  10. [Tổng quan & Luồng xử lý Datagram tại Server](#10-tổng-quan--luồng-xử-lý-datagram-tại-server)
  11. [Quản lý kết nối & Trạng thái (`StreamManager` & `StateManager`)](#11-quản-lý-kết-nối--trạng-thái-streammanager--statemanager)
  12. [Định tuyến & Xử lý nghiệp vụ (`Router`, `ChatHandler`, `FileHandler`)](#12-định-tuyến--xử-lý-nghiệp-vụ-router-chathandler-filehandler)
- **Phần III: Kiến trúc Phía Máy khách (`client/` & `ui/`)**
  13. [Mô hình đa luồng & Cầu nối Giao diện (`UIBridge`)](#13-mô-hình-đa-luồng--cầu-nối-giao-diện-uibridge)
  14. [Cơ chế truyền tệp ngang hàng P2P (`client/file_handler.py`)](#14-cơ-chế-truyền-tệp-ngang-hàng-p2p-clientfile_handlerpy)
- **Phần IV: Vận hành, Xử lý Lỗi & Tinh chỉnh**
  15. [Hệ thống Ngoại lệ & Xử lý lỗi](#15-hệ-thống-ngoại-lệ--xử-lý-lỗi)
  16. [Tham số cấu hình & Giới hạn hệ thống](#16-tham-số-cấu-hình--giới-hạn-hệ-thống)

---

# PHẦN I: LÕI GIAO THỨC TRUYỀN VẬN & BẢO MẬT (`shared/`)

## 1. Tổng quan kiến trúc phân tầng

Mỗi kết nối logic trong hệ thống đều đi qua ngăn xếp giao thức 4 lớp từ trên xuống dưới:

```text
┌──────────────────────────────────────────────────────────────────┐
│  Tầng Ứng dụng (Server Handlers / Client UIBridge & FileHandler) │
├──────────────────────────────────────────────────────────────────┤
│  Tầng Định dạng Bản tin (Pydantic JSON Schemas)                 │
├──────────────────────────────────────────────────────────────────┤
│  Tầng Bảo mật & Đóng khung (SecureStream: X25519 + AES-256-GCM)  │
├──────────────────────────────────────────────────────────────────┤
│  Tầng Truyền vận Tin cậy (PacketService: CUBIC, SlidingWindow)   │
├──────────────────────────────────────────────────────────────────┤
│  Tầng Phân kênh & Socket OS (UDP Socket / StreamManager / Mux)   │
└──────────────────────────────────────────────────────────────────┘
```

### Mô hình luồng (Threading Model) của một `PacketService`
Mỗi đối tượng `PacketService` sau khi bắt tay thành công sẽ khởi chạy **2 daemon thread** độc lập:
- **`_receive_loop` (`pkt-recv`):** Đọc datagram (từ `sock.recvfrom` nếu `shared_socket=False`, hoặc từ hàng đợi `incoming_packets` nếu `shared_socket=True`), kiểm tra checksum/port rồi chuyển vào `_handle_packet`.
- **`_timer_loop` (`pkt-timer`):** Thức dậy mỗi `RETRANSMIT_TICK` (0.05s) để kiểm tra lần lượt: hết hạn `TIME_WAIT`, kiểm tra `IDLE_TIMEOUT`, gửi gói `KEEPALIVE`, và kích hoạt truyền lại khi quá hạn `RTO`.

Mọi truy cập vào trạng thái giao thức đều được đồng bộ hóa qua `self.condition` (`threading.Condition` bọc `RLock`). Riêng hàm `send()` dùng thêm `self._send_lock` để đảm bảo tính **atomic** (nhiều thread gọi `send()` đồng thời sẽ xếp hàng tuần tự, không bị xen kẽ byte giữa các message).

---

## 2. Cấu trúc gói tin & Số học Wrap-around 32-bit

### 2.1. Định dạng Header (18 Bytes)
Header sử dụng định dạng big-endian `"!HHIIHHH"`, theo sau là payload tối đa `MSS = 1024` bytes (đảm bảo tổng kích thước gói UDP + IP luôn nhỏ hơn MTU 1500 bytes, tránh phân mảnh ở tầng IP):

```text
 0        2        4            8            12     14       16       18
 ┌────────┬────────┬────────────┬────────────┬──────┬────────┬────────┬──────────┐
 │src_port│dst_port│  seq_num   │  ack_num   │flags │ window │checksum│ payload  │
 │ 2 byte │ 2 byte │   4 byte   │   4 byte   │2 byte│ 2 byte │ 2 byte │ ≤ 1024 B │
 └────────┴────────┴────────────┴────────────┴──────┴────────┴────────┴──────────┘
```

- **`flags` (`TCPFlags`):** `FIN (0x01)`, `SYN (0x02)`, `RST (0x04)`, `PSH (0x08)`, `ACK (0x10)`.
- **`checksum`:** Tổng bù 1 (One's Complement) 16-bit tính trên toàn bộ header (với trường checksum ban đầu bằng 0) và payload. Khi giải gói (`Packet.unpack`), tổng bù 1 của toàn bộ gói nhận được phải bằng `0`; nếu khác `0` hoặc thiếu 18 byte header, gói tin bị loại bỏ ngay lập tức.

### 2.2. Xử lý Wrap-around 32-bit (`shared/algorithms.py`)
`seq_num` và `ack_num` là số nguyên không dấu 32-bit ($0 \rightarrow 2^{32}-1$), đếm theo từng **byte** dữ liệu. Khi truyền vượt quá 4 GB trên một kết nối, số thứ tự sẽ quay vòng về 0. Để tránh lỗi so sánh sai khi quay vòng, toàn bộ giao thức không dùng toán tử `<`, `>` thông thường mà dùng số học Modulo $2^{32}$:

$$\text{seq\_diff}(a, b) = \begin{cases} ((a - b) \bmod 2^{32}) - 2^{32} & \text{nếu } ((a - b) \bmod 2^{32}) \ge 2^{31} \\ (a - b) \bmod 2^{32} & \text{ngược lại} \end{cases}$$

Các hàm `seq_lt`, `seq_lte`, `seq_gt`, `seq_gte` đều dựa trên dấu của `seq_diff(a, b)`.

---

## 3. Quản lý kết nối & Máy trạng thái

### 3.1. Sơ đồ chuyển trạng thái
```text
CLOSED ──connect()──> SYN_SENT ──nhận SYN-ACK──> ESTABLISHED ──close()──> TIME_WAIT ──(2s)──> CLOSED
   ▲                                                  │
   │         (server: establish_server_side)          ├── im lặng > 30s / quá 5 lần retry ──> FAILED
   └────────────────── nhận RST ──────────────────────┘                                          │
                                                                                     close() ──> CLOSED
```

### 3.2. Bắt tay 3 bước (3-Way Handshake)
1. **Client (`connect`):** Chọn ngẫu nhiên 32-bit `ISN = X`, gửi gói `SYN (seq=X)` và chờ `SYN-ACK` với cơ chế Exponential Backoff (`1s -> 2s -> 4s...`, tối đa `max_retries = 5`). Trên Windows, nếu gặp `ConnectionResetError` (ICMP Port Unreachable do server chưa mở), client sẽ chủ động `sleep(current_timeout)` trước khi thử lại để tránh đốt sạch 5 lần retry trong vài mili-giây.
2. **Server (`_accept_new_connection` / `ServerMux`):** Nhận `SYN (seq=X)`, chọn `ISN = Y`, tạo `PacketService(shared_socket=True)`, gửi `SYN-ACK (seq=Y, ack=X+1)` và gọi `establish_server_side()` để chuyển thẳng sang `ESTABLISHED`.
3. **Client:** Nhận `SYN-ACK`, khởi tạo hai cửa sổ trượt với `local_next_seq = X + 1`, `peer_next_seq = Y + 1`, gửi gói `ACK` hoàn tất bắt tay.

### 3.3. Đóng kết nối (Graceful Close & Half-Close)
- Cờ `FIN` tiêu thụ **1 số thứ tự** giống như một byte dữ liệu.
- Khi gọi `close(timeout=3)`, bên chủ động gửi `FIN | ACK (seq=fin_seq)` và chờ `ACK` với `ack_num = fin_seq + 1`.
- Nếu nhận `FIN` đến sớm (khi các gói dữ liệu trước đó chưa tới đủ), bên nhận lưu vị trí vào `_pending_fin_seq` và chỉ kích hoạt `EOF` sau khi dữ liệu đã lấp đầy khoảng trống (`_try_resolve_pending_fin_locked`).
- Sau khi hoàn tất trao đổi `FIN`, kết nối đi vào trạng thái **`TIME_WAIT` trong 2.0 giây** để tiếp tục phản hồi `ACK` nếu bên kia gửi lại `FIN` do mất gói `ACK` cuối cùng.

### 3.4. Duy trì kết nối (Keepalive & Idle Timeout)
- Mỗi `5.0s` (`KEEPALIVE_INTERVAL`), nếu ở trạng thái `ESTABLISHED`, `PacketService` gửi một gói `ACK` rỗng. Gói này vừa duy trì trạng thái sống, vừa quảng bá lại kích thước `window` mới nhất.
- Nếu quá `30.0s` (`IDLE_TIMEOUT`) không nhận được bất kỳ gói tin nào từ peer, kết nối chuyển sang trạng thái `FAILED`.

---

## 4. Độ tin cậy, Sliding Window & Truyền lại

### 4.1. Cửa sổ trượt hai chiều (`SlidingWindow`)
- **Phía gửi (`send_window`):** Theo dõi `send_base` (byte cũ nhất chưa được ACK) và `next_seq_num` (byte tiếp theo sẽ gửi). Mọi gói gửi đi được lưu trong `unacked_packets` cho đến khi nhận được **Cumulative ACK** vượt qua biên cuối của gói đó.
- **Phía nhận (`receive_window`):** Theo dõi `receive_next` (byte tiếp theo đang chờ).
  - Gói đến đúng `seq_num == receive_next`: ráp nối với các gói đang nằm chờ phía sau trong `receive_buffer`, đẩy vào hàng đợi `received_data` cho ứng dụng và tăng `receive_next`.
  - Gói đến sớm (`seq_num > receive_next`): lưu vào `receive_buffer` (out-of-order) và gửi ngay một gói `ACK` mang giá trị `receive_next` cũ (tạo thành **Duplicate ACK**).
  - Gói trùng lặp hoặc ngoài cửa sổ nhận: loại bỏ và gửi lại `ACK` hiện tại.

### 4.2. Hai cơ chế phục hồi mất gói
1. **Fast Retransmit (Phản ứng nhanh):** Khi bên gửi nhận được **3 Dup-ACK liên tiếp** (`DUP_ACK_THRESHOLD = 3`), hệ thống lập tức gửi lại gói tại `send_base` mà không cần chờ hết RTO.
   - *Lưu ý thiết kế:* Chỉ **Pure ACK** (không mang payload, không cờ `SYN`/`FIN`) mới được tính là Dup-ACK, tránh trường hợp gói dữ liệu từ chiều ngược lại mang kèm `ack_num` cũ gây kích hoạt Fast Retransmit giả trong giao tiếp song công (Full-Duplex).
2. **RTO Timer (Lưới an toàn):** Mỗi kết nối dùng **một bộ đếm thời gian duy nhất** (`_rto_start`) tính từ thời điểm gửi gói cũ nhất hoặc lần cuối `send_base` tiến lên. Khi hết `rto` giây, chỉ gói cũ nhất tại `send_base` được gửi lại, đồng thời kích hoạt Exponential Backoff (`rto *= 2`). Nếu một gói gửi lại quá `max_retries` (5 lần) không có phản hồi, kết nối chuyển sang `FAILED`.

---

## 5. Đo RTT và tính RTO

Hệ thống áp dụng thuật toán **Jacobson/Karels (RFC 6298)**:
- **Mẫu đầu tiên:** $\text{SRTT} = R$, $\text{RTTVAR} = R / 2$.
- **Các mẫu tiếp theo:**
  $$\text{RTTVAR} = 0.75 \times \text{RTTVAR} + 0.25 \times |\text{SRTT} - R|$$
  $$\text{SRTT} = 0.875 \times \text{SRTT} + 0.125 \times R$$
  $$\text{RTO} = \text{clamp}(\text{SRTT} + 4 \times \text{RTTVAR},\; 0.2\text{s},\; 60.0\text{s})$$

**Quy tắc Karn mở rộng:** Hệ thống không lấy mẫu RTT nếu gói tin đã từng được truyền lại (`seq in self.retransmitted`) hoặc nếu Cumulative ACK nhảy qua một gói đã từng truyền lại (`crossed_retx`). Điều này loại bỏ sự nhập nhằng giữa lần gửi gốc và lần gửi lại, ngăn chặn tình trạng RTO phình to bất thường. Khi có ACK tiến lên hợp lệ, RTO được tính lại ngay từ `SRTT` để xóa trạng thái backoff.

---

## 6. Kiểm soát luồng (Flow Control)

Mỗi gói tin gửi đi đều đính kèm trường `window_size` (16-bit, tối đa 65,535 bytes) phản ánh dung lượng trống thực tế của bộ đệm nhận (`RECEIVE_BUFFER_BYTES = 256 KB`):

$$\text{advertised\_window} = \min\big(256\text{ KB} - (\text{\_received\_bytes} + \text{buffered\_bytes()}),\; 65535\big)$$

- **Từ chối khi tràn đệm:** Nếu payload của gói đến lớn hơn dung lượng trống `free`, bên nhận từ chối lưu gói và gửi lại `ACK` kèm kích thước cửa sổ nhỏ hiện tại.
- **Window Update:** Khi ứng dụng gọi `receive()` và rút dữ liệu khỏi hàng đợi làm `new_win > _last_advertised_window`, bên nhận chủ động gửi một gói `ACK` cập nhật cửa sổ để đánh thức bên gửi đang bị chặn. Nếu gói cập nhật này bị thất lạc, gói `KEEPALIVE` định kỳ mỗi 5 giây sẽ tự động đồng bộ lại giá trị cửa sổ.

---

## 7. Kiểm soát tắc nghẽn (CUBIC & NewReno)

Bên gửi duy trì cửa sổ tắc nghẽn `cwnd` (khởi đầu `4 * MSS`, tối đa `64 * MSS`) và ngưỡng `ssthresh`:

1. **Slow Start (`cwnd < ssthresh`):** Mỗi khi nhận ACK xác nhận `acked` bytes mới, `cwnd` tăng thêm `acked` bytes (tăng gấp đôi sau mỗi RTT).
2. **Congestion Avoidance — CUBIC (`cwnd >= ssthresh`, RFC 8312):**
   Thay vì tăng tuyến tính như Reno truyền thống, `cwnd` bám theo hàm bậc ba tính từ thời điểm mất gói gần nhất (`epoch_start`):
   $$W(t) = C \cdot (t - K)^3 + W_{\max}, \quad K = \sqrt[3]{\frac{W_{\max} \cdot (1 - \beta)}{C}}$$
   Trong đó $C = 0.4$, $\beta = 0.7$, và $W_{\max}$ là giá trị `cwnd` ngay trước thời điểm xảy ra mất gói. Đường cong tăng nhanh khi còn xa $W_{\max}$, đi ngang thăm dò ổn định quanh $W_{\max}$, sau đó tăng tốc trở lại để dò băng thông khả dụng mới.
3. **NewReno Fast Recovery:**
   - Khi chạm ngưỡng 3 Dup-ACK: Lưu `_recover = next_seq_num`, gửi lại gói mất, đặt $W_{\max} = \text{cwnd}$, $\text{ssthresh} = 0.7 \times \text{cwnd}$, và $\text{cwnd} = \text{ssthresh} + 3 \cdot \text{MSS}$.
   - **Partial ACK** (`ack < _recover`): Xác định vẫn còn gói bị mất trong cùng cửa sổ gửi; hệ thống lập tức gửi lại gói tại vị trí `ack` và giữ nguyên trạng thái Fast Recovery thay vì thoát ra sớm.
   - **Full ACK** (`ack >= _recover`): Thoát Fast Recovery, đặt `cwnd = ssthresh` và bắt đầu chu kỳ CUBIC mới.
4. **RTO Timeout:** Đặt $W_{\max} = \text{cwnd}$, $\text{ssthresh} = 0.7 \times \text{cwnd}$, hạ $\text{cwnd} = 1 \cdot \text{MSS}$ và quay về Slow Start.

Tại mọi thời điểm, số byte được phép gửi thêm được giới hạn bởi công thức hợp nhất:
$$\text{available} = \text{seq\_diff}\Big(\text{send\_base} + \min(\text{peer\_window},\; \lfloor\text{cwnd}\rfloor,\; \text{send\_window\_size}),\; \text{next\_seq\_num}\Big)$$

---

## 8. Tầng Bảo mật (`SecureStream` & `CryptoBox`)

Tầng bảo mật (`shared/security.py`) sử dụng thư viện chuẩn `cryptography` với 3 thành phần:

1. **`SecureStream` (Bảo mật luồng tin cậy trên `PacketService`):**
   - **Trao đổi khóa (ECDH):** Ngay sau khi `PacketService` thiết lập kết nối, hai bên thực hiện handshake trao đổi khóa công khai **X25519** (32 bytes mỗi chiều).
   - **Dẫn xuất khóa (HKDF-SHA256):** Bí mật chung từ X25519 được trộn cùng Pre-Shared Key (`psk`) qua `HKDF` để sinh ra **2 khóa AES-256 độc lập** cho hai chiều (`client->server` và `server->client`), đảm bảo **Forward Secrecy** (lộ khóa phiên này không ảnh hưởng phiên khác) và xác thực lẫn nhau qua PSK.
   - **Đóng khung & Mã hóa (AEAD):** Vì `PacketService` truyền luồng byte thô không có ranh giới gói tin, `SecureStream` bổ sung header 4-byte độ dài (`!I`) trước mỗi bản tin đã mã hóa **AES-256-GCM**. Nonce 96-bit được sinh từ bộ đếm tăng dần 64-bit (`_send_ctr`, `_recv_ctr`) theo từng chiều, đảm bảo không bao giờ trùng nonce và phát hiện mọi hành vi sửa đổi, chèn, xóa hoặc đảo thứ tự gói tin (`SecurityError`).
2. **`CryptoBox`:** Cung cấp mã hóa AES-256-GCM kèm nonce ngẫu nhiên 12-byte cho các gói tin UDP độc lập không kết nối.
3. **`hash_password` / `verify_password`:** Băm mật khẩu bằng **PBKDF2-HMAC-SHA256** (200,000 vòng lặp + salt 16-byte ngẫu nhiên) và so sánh hằng thời gian (`hmac.compare_digest`).

---

## 9. Giao thức bản tin ứng dụng (`schemas.py`)

Dữ liệu trao đổi bên trong `SecureStream` là các chuỗi JSON được kiểm tra kiểu dữ liệu nghiêm ngặt bằng **Pydantic v2** thông qua trường phân loại `action` (`ActionType`):

| Nhóm | `action` | Chiều truyền | Các trường dữ liệu chính |
|---|---|---|---|
| **Request** | `REQ_LOGIN` | Client $\rightarrow$ Server | `username: str` |
| **Request** | `REQ_CHAT` | Client $\rightarrow$ Server | `to_user: str`, `msg: str` |
| **Request** | `REQ_BROADCAST` | Client $\rightarrow$ Server | `msg: str` |
| **Request** | `REQ_FILE_SEND` | Client $\rightarrow$ Server | `to_user: str`, `filename: str`, `filesize: int` |
| **Response** | `RES_FILE_ACCEPT` | Client $\rightarrow$ Server | `to_user: str`, `session_id: str` (`ip:port:filename`) |
| **Response** | `RES_FILE_REJECT` | Client $\rightarrow$ Server | `to_user: str`, `reason: str` |
| **Response** | `RES_GENERAL` | Server $\rightarrow$ Client | `status: "ok" \| "error"`, `message: str` |
| **Feedback** | `INCOMING_CHAT` | Server $\rightarrow$ Client | `from_user: str`, `is_broadcast: bool`, `msg: str` |
| **Feedback** | `INCOMING_FILE_REQ` | Server $\rightarrow$ Client | `from_user: str`, `filename: str`, `filesize: int` |
| **Feedback** | `SIGNAL_FILE_READY` | Server $\rightarrow$ Client | `from_user: str`, `session_id: str` |
| **System** | `USER_LIST` | Server $\rightarrow$ Client | `users: list[str]` (cập nhật danh sách online) |

---

# PHẦN II: KIẾN TRÚC PHÍA MÁY CHỦ (`server/`)

## 10. Tổng quan & Luồng xử lý Datagram tại Server

Server chỉ mở **một socket UDP duy nhất** tại `0.0.0.0:9000` (`server/config.py`) để phục vụ đồng thời tối đa `MAX_CLIENTS = 40` kết nối.

```text
UDP Socket (:9000)
       │
       ▼  recvfrom(65535)
┌──────────────────────────────────────┐
│  server/main.py (Vòng lặp I/O lõi)   │
└──────────────────┬───────────────────┘
                   │ handle_incoming_datagram(raw_data, addr)
                   ▼
┌──────────────────────────────────────┐
│  server/stream_manager.py            │
│  ├─ addr đã tồn tại? ──> svc.feed_datagram() ──> SecureStream ──> _client_read_worker
│  └─ addr mới + SYN?  ──> _accept_new_connection() (Tạo svc, gửi SYN-ACK, mở thread đọc)
└──────────────────────────────────────┘
                                                                          │
                                                                          ▼ payload (JSON)
                                                              ┌───────────────────────┐
                                                              │   server/router.py    │
                                                              └───┬───────────────┬───┘
                                                                  │               │
                                          Login / Chat / Broadcast│               │ File Signaling
                                                                  ▼               ▼
                                                       ┌────────────────┐ ┌────────────────┐
                                                       │  ChatHandler   │ │  FileHandler   │
                                                       └───────┬────────┘ └───────┬────────┘
                                                               │                  │
                                                               └────────┬─────────┘
                                                                        ▼
                                                       ┌────────────────────────────────┐
                                                       │ StateManager (username ↔ addr) │
                                                       └────────────────────────────────┘
```

- **Tách biệt I/O và Nghiệp vụ:** `server/main.py` chỉ làm một nhiệm vụ duy nhất là nhận datagram thô qua `sock.recvfrom(65535)` và chuyển ngay cho `StreamManager`. (Trên Windows, lỗi `ConnectionResetError` / WinError 10054 do ICMP unreachable được bắt và bỏ qua để không làm sập vòng lặp Server).
- **Dependency Injection:** `main.py` khởi tạo `StateManager`, `StreamManager`, `ChatHandler`, `FileHandler`, sau đó tiêm vào `Router` và gắn ngược `Router` vào `StreamManager` để tránh lỗi circular import.

---

## 11. Quản lý kết nối & Trạng thái (`StreamManager` & `StateManager`)

### 11.1. `server/stream_manager.py` (Mux/Demux & Worker per Client)
- **Phân kênh (Demultiplexing):** Lưu bảng ánh xạ `self.connections[(ip, port)] -> PacketService` và `self.channels[(ip, port)] -> SecureStream`.
  - Khi nhận datagram từ địa chỉ đã có trong `connections`, gọi `svc.feed_datagram(raw_data, addr)` để đẩy vào hàng đợi riêng của kết nối đó.
  - Khi nhận datagram từ địa chỉ mới: chỉ chấp nhận nếu là gói **SYN thuần** (có cờ `SYN`, không có cờ `ACK`). Mọi gói rác/scan port đều bị bỏ qua lặng lẽ. Nếu đạt ngưỡng `MAX_CLIENTS`, server gửi trả gói `RST` để từ chối.
- **Luồng đọc riêng biệt (`_client_read_worker`):** Mỗi client được cấp một daemon thread riêng (`Reader-<port>`). Thread này thực hiện `chan.handshake(timeout=10)` (với `psk=b"HUDP_SECRET_KEY"`), sau đó lặp vô tận `chan.recv_message()` và đẩy chuỗi JSON sang `Router`. Nhờ mỗi client có thread đọc riêng, việc xử lý chậm ở một client không làm nghẽn các client khác.
- **Gửi bất đồng bộ (`send_to`):** Khi `ChatHandler` hoặc `FileHandler` muốn gửi tin tới một địa chỉ, `send_to()` tạo một worker thread ngắn hạn (`_async_send_worker`) gọi `chan.send_message(data)` để tránh block luồng gọi.
- **Dọn dẹp tự động (`_cleanup_connection`):** Khi client ngắt kết nối (nhận `EOF`, timeout hoặc reset), khối `finally` của `_client_read_worker` sẽ xóa `svc` và `chan`, đóng kết nối, gọi `state_manager.logout_by_address(addr)` và tự động broadcast danh sách `USER_LIST` mới cho toàn bộ người dùng còn lại.

### 11.2. `server/state_manager.py`
Quản lý ánh xạ hai chiều giữa danh tính ứng dụng (`username`) và địa chỉ mạng (`(ip, port)`):
- `self.users`: `dict[str, tuple]` (`username -> (ip, port)`)
- `self.addresses`: `dict[tuple, str]` (`(ip, port) -> username`)
- Mọi thao tác `login`, `logout_by_username`, `logout_by_address`, `get_address`, `get_username` đều được bảo vệ bằng `threading.Lock()` để đảm bảo an toàn luồng (thread-safe).

---

## 12. Định tuyến & Xử lý nghiệp vụ (`Router`, `ChatHandler`, `FileHandler`)

1. **`server/router.py`:** Dùng `TypeAdapter(ClientMessage).validate_json(payload)` của Pydantic để kiểm tra tính hợp lệ và tự động phân tích cú pháp JSON thành các model tương ứng. Nếu sai cấu trúc, trả về `GeneralResponse(status="error")`.
2. **`server/chat_handler.py`:**
   - `REQ_LOGIN`: Đăng ký `username` vào `StateManager`. Nếu thành công, trả `GeneralResponse(status="ok")` và gửi bản tin `USER_LIST` tới tất cả client đang online.
   - `REQ_CHAT`: Tra cứu địa chỉ người nhận từ `StateManager` và chuyển tiếp bản tin `IncomingChat(is_broadcast=False)`. Nếu người nhận offline, báo lỗi cho người gửi.
   - `REQ_BROADCAST`: Lặp qua toàn bộ `get_all_users_online()` (trừ người gửi) và gửi `IncomingChat(is_broadcast=True)`.
3. **`server/file_handler.py` (Signaling Server):**
   Server **không** trung chuyển dữ liệu file (tránh nghẽn cổ chai băng thông tại Server) mà chỉ đóng vai trò **Signaling** (trao đổi thông tin địa chỉ P2P):
   - Chuyển tiếp `REQ_FILE_SEND` thành `IncomingFileRequest` tới người nhận.
   - Chuyển tiếp `RES_FILE_ACCEPT` (chứa `session_id` gồm IP và Port P2P của người nhận) thành `SignalFileReady` tới người gửi.
   - Chuyển tiếp `RES_FILE_REJECT` thành thông báo lỗi tới người gửi.

---

# PHẦN III: KIẾN TRÚC PHÍA MÁY KHÁCH (`client/` & `ui/`)

## 13. Mô hình đa luồng & Cầu nối Giao diện (`UIBridge`)

Phía Client vận hành kết hợp giao thức UDP tùy chỉnh ở tầng dưới và giao diện Web hiện đại (HTML/JS) ở tầng trên thông qua kiến trúc **Local HTTP Bridge**:

```text
┌───────────────────────────┐   HTTP POST /api/command    ┌──────────────────────────────┐
│   Trình duyệt (Web UI)    │ ──────────────────────────> │  client/ui_bridge.py         │
│   ui/index.html + app.js  │ <────────────────────────── │  (HTTPServer 127.0.0.1:port) │
└───────────────────────────┘   HTTP GET /api/messages    └──────────────┬───────────────┘
                                  (Polling mỗi 500ms)                    │
                                                          ┌──────────────▼───────────────┐
                                                          │       MESSAGE_QUEUE          │
                                                          └──────────────▲───────────────┘
                                                                         │
┌───────────────────────────┐   HUDP + SecureStream       ┌──────────────┴───────────────┐
│   Server (UDP :9000)      │ <─────────────────────────> │  client/stream_manager.py    │
└───────────────────────────┘                             └──────────────────────────────┘
```

- **`client/stream_manager.py`:** Mở một socket UDP cổng ngẫu nhiên (`bind(("0.0.0.0", 0))`), kết nối tới Server (`SERVER_IP:SERVER_PORT`), thiết lập `SecureStream` (`psk=b"HUDP_SECRET_KEY"`), và chạy luồng `_receive_worker` liên tục nhận JSON từ Server đẩy vào `MESSAGE_QUEUE` thông qua `handle_incoming_message()`.
- **`client/ui_bridge.py`:** Chạy một `HTTPServer` cục bộ trên luồng riêng tại `127.0.0.1:<http_port>`:
  - Phục vụ các file tĩnh `index.html`, `style.css`, `app.js` từ thư mục `ui/`.
  - **`POST /api/command`:** Nhận thao tác JSON từ giao diện Web (`login`, `chat`, `all`, `file`, `accept`, `reject`), chuyển đổi thành các model Pydantic tương ứng và gửi qua `ClientStreamManager`.
  - **`GET /api/messages`:** Rút toàn bộ sự kiện đang chờ trong `MESSAGE_QUEUE` trả về cho trình duyệt (Frontend polling mỗi `500ms`).

---

## 14. Cơ chế truyền tệp ngang hàng P2P (`client/file_handler.py`)

Khi hai người dùng (ví dụ **Alice** gửi file cho **Bob**) thực hiện truyền tệp, hệ thống thiết lập một **kênh kết nối HUDP + SecureStream độc lập hoàn toàn** trực tiếp giữa Alice và Bob mà không đi qua Server:

```text
  Alice (Người gửi)                   Server (Signaling)                  Bob (Người nhận)
         │                                    │                                  │
         │── 1. REQ_FILE_SEND(filename,size) >│                                  │
         │                                    │── 2. INCOMING_FILE_REQ ─────────>│
         │                                    │                                  │ (Bob bấm Đồng ý)
         │                                    │                                  ├─ Mở UDP Socket port P
         │                                    │<─ 3. RES_FILE_ACCEPT(IP:P:file) ─┤  & chạy _receive_file_worker
         │<─ 4. SIGNAL_FILE_READY(IP:P:file) ─┤                                  │
         │                                    │                                  │
         │══════════════ 5. KẾT NỐI P2P TRỰC TIẾP QUA UDP (Port P) ══════════════│
         │── SYN ───────────────────────────────────────────────────────────────>│
         │<──────────────────────────────────────────────────────────── SYN-ACK ─┤
         │── ACK + X25519 ECDH Handshake (psk = b"P2P_FILE_SECRET") ────────────>│
         │── [Chunk 1024B] + [Chunk 1024B] + ... (Cập nhật UI Progress Bar) ────>│
         │── FIN ───────────────────────────────────────────────────────────────>│
         │<────────────────────────────────────────────────────────── ACK + FIN ─┤
```

### Chi tiết kỹ thuật luồng P2P:
1. **Chuẩn bị phía Người nhận (`start_receiving`):**
   - Mở một UDP socket mới tại cổng ngẫu nhiên (`0.0.0.0:0`) và xác định địa chỉ IP LAN thực tế của máy (thông qua mẹo kết nối UDP tới `8.8.8.8:1` để đọc `getsockname()[0]`).
   - Gửi chuỗi định danh `session_id = "<listen_ip>:<listen_port>:<filename>"` qua Server để báo cho người gửi.
   - Luồng `_receive_file_worker` đợi tối đa `30.0s` cho gói `SYN` đầu tiên, phản hồi `SYN-ACK`, khởi tạo `PacketService(shared_socket=False)` và bọc trong `SecureStream(..., is_server=True, psk=b"P2P_FILE_SECRET")`.
2. **Đẩy dữ liệu phía Người gửi (`_send_file_worker`):**
   - Khi nhận được `SIGNAL_FILE_READY`, Alice tách `target_ip`, `target_port`, `filename` từ `session_id` và lấy đường dẫn file gốc từ `pending_uploads`.
   - Mở socket UDP mới, gọi `svc.connect((target_ip, target_port))` trực tiếp tới máy Bob và thực hiện handshake bảo mật `SecureStream(..., is_server=False, psk=b"P2P_FILE_SECRET")`.
   - Đọc file nhị phân theo từng khối **`1024 bytes`**, gọi `chan.send_message(chunk)` (tự động điều tiết tốc độ theo CUBIC & Flow Control của `PacketService`), đồng thời đẩy sự kiện `FILE_PROGRESS` vào `MESSAGE_QUEUE` để thanh tiến trình trên Web UI cập nhật theo thời gian thực.
3. **Kết thúc phiên P2P:** Khi đọc hết file, Alice gọi `close()`, phát tín hiệu `FIN` (`EOF`) để Bob lưu hoàn tất tệp vào thư mục `downloads/` và giải phóng socket.

---

# PHẦN IV: VẬN HÀNH, XỬ LÝ LỖI & TINH CHỈNH

## 15. Hệ thống Ngoại lệ & Xử lý lỗi

Các lớp ngoại lệ tầng truyền vận được định nghĩa tập trung tại `shared/exceptions.py`:

| Exception | Kế thừa từ | Nguyên nhân & Cách xử lý |
|---|---|---|
| `TransportError` | `Exception` | Lớp cơ sở cho mọi lỗi mạng của giao thức HUDP. |
| `ProtocolError` | `TransportError` | Lỗi logic/cấu hình (gọi `send`/`receive` trước `connect`, dùng `connect()` với `shared_socket=True`, hoặc peer trả về `RST` khi đang bắt tay). Không nên retry mù quáng. |
| `ConnectionTimeoutError` | `TransportError` | `connect()` không nhận được `SYN-ACK`; `send()` quá 5 lần retry; `receive()` hết timeout; hoặc peer im lặng quá `IDLE_TIMEOUT` (30s). Với truyền file, đây là dấu hiệu **file bị cắt giữa chừng**. |
| `PeerResetError` | `ConnectionResetError` | Peer chủ động gửi cờ `RST` hủy kết nối ở tầng giao thức. |
| `SecurityError` | `Exception` | Sai khóa PSK, gói tin bị giả mạo trên đường truyền (`InvalidTag`), độ dài frame bất thường, hoặc stream đứt giữa chừng một message. |

*Cơ chế chống sập luồng:* Cả `_receive_loop` và `_timer_loop` trong `PacketService` đều bọc xử lý từng gói tin trong khối `try...except Exception`, ghi nhận lỗi vào `self._last_error` thay vì để ngoại lệ làm chết luồng nền. Dữ liệu đã nhận hợp lệ trong hàng đợi `received_data` luôn được trả về hết cho ứng dụng trước khi ném lỗi `ConnectionTimeoutError` hoặc `PeerResetError`.

---

## 16. Tham số cấu hình & Giới hạn hệ thống

### 16.1. Bảng tham số mặc định

| Vị trí | Tham số | Giá trị mặc định | Ý nghĩa |
|---|---|---|---|
| `PacketService` | `MSS` | `1024` bytes | Kích thước payload tối đa mỗi packet |
| `PacketService` | `RECEIVE_BUFFER_BYTES` | `256 * 1024` (256 KB) | Dung lượng bộ đệm nhận tối đa |
| `PacketService` | `INITIAL_CWND` / `MAX_CWND` | `4 * MSS` / `64 * MSS` | Cửa sổ tắc nghẽn khởi đầu và tối đa |
| `PacketService` | `cubic_c` / `cubic_beta` | `0.4` / `0.7` | Hệ số đường cong và hệ số giảm của CUBIC |
| `PacketService` | `INITIAL_RTO` / `MIN` / `MAX` | `1.0s` / `0.2s` / `60.0s` | Thời gian chờ truyền lại và biên giới hạn |
| `PacketService` | `DUP_ACK_THRESHOLD` | `3` | Số Dup-ACK kích hoạt Fast Retransmit |
| `PacketService` | `KEEPALIVE_INTERVAL` | `5.0s` | Chu kỳ gửi ACK duy trì kết nối |
| `PacketService` | `IDLE_TIMEOUT` | `30.0s` | Thời gian im lặng tối đa trước khi `FAILED` |
| `PacketService` | `TIME_WAIT_SECONDS` | `2.0s` | Thời gian giữ trạng thái đóng kết nối |
| `server/config.py` | `HOST` / `PORT` | `"0.0.0.0"` / `9000` | Địa chỉ và cổng UDP của Server |
| `server/config.py` | `MAX_CLIENTS` | `40` | Số lượng kết nối đồng thời tối đa tại Server |
| `client/config.py` | `SERVER_IP` / `SERVER_PORT` | `"127.0.0.1"` / `9000` | Địa chỉ Server mặc định mà Client kết nối tới |

### 16.2. Các giới hạn đã biết
- **Kích thước cửa sổ quảng cáo tối đa 64 KB:** Trường `window_size` trong header có độ dài 16-bit (tối đa `65535` bytes), do đó băng thông trên các đường truyền có Bandwidth-Delay Product (BDP) lớn bị giới hạn ở mức 64 KB dữ liệu đang bay (in-flight).
- **Chưa hỗ trợ SACK (Selective ACK):** Giao thức sử dụng Cumulative ACK kết hợp NewReno Partial ACK; khi mất nhiều gói trong cùng một cửa sổ, các gói mất sẽ được phục hồi tuần tự qua từng RTT thay vì đồng thời trong một RTT.
- **Môi trường NAT nghiêm ngặt:** Luồng truyền file P2P sử dụng IP LAN nội bộ của máy nhận (`getsockname()[0]`), hoạt động tối ưu trong cùng mạng LAN/Wi-Fi/Hotspot nhưng chưa tích hợp STUN/TURN để xuyên qua Symmetric NAT trên Internet công cộng.