# HUDP Collaboration Platform

> **Distributed and Streaming-based Client-Server & P2P Collaboration Application over custom HUDP Protocol**

Đồ án môn **Mạng Máy Tính** — Trường Đại học Bách Khoa – ĐHQG TP.HCM.

## 1. Giới thiệu Tổng quan

**HUDP Collaboration Platform** là ứng dụng cộng tác thời gian thực cho phép nhắn tin (nhóm/riêng tư) và chia sẻ tệp tin trực tiếp ngang hàng (**Peer-to-Peer**) trong môi trường mạng LAN.

Toàn bộ hệ thống được xây dựng trên **Raw UDP Socket** thông qua giao thức truyền vận tùy chỉnh **HUDP (Hybrid/High User Datagram Protocol)**, tự hiện thực đầy đủ các tính chất cốt lõi của TCP và tầng bảo mật hiện đại:

* **Truyền tải tin cậy & Đúng thứ tự:** Đánh số thứ tự theo byte (32-bit hỗ trợ wrap-around), Cumulative ACK, bộ đệm ghép gói out-of-order, RTO linh hoạt (Jacobson/Karels + quy tắc Karn) và Fast Retransmit (3 Dup-ACK).

* **Kiểm soát luồng & Tắc nghẽn:** Sliding Window kết hợp **CUBIC Congestion Control** (RFC 8312) và **NewReno Fast Recovery**.

* **Bảo mật đầu cuối (E2EE):** Trao đổi khóa **ECDH (X25519)** kết hợp **HKDF-SHA256** và mã hóa luồng **AES-256-GCM** chống nghe lén, giả mạo và phát lại.

* **Kiến trúc Client-Server & P2P:** Server đa nhiệm trên 1 cổng UDP duy nhất; Client tích hợp Web UI cục bộ qua HTTP Bridge; truyền file P2P trực tiếp giữa hai Client có mã hóa riêng.

## 2. Cấu trúc Thư mục

```
.
├── client/                    # Tầng ứng dụng phía Máy khách
│   ├── config.py              # Cấu hình địa chỉ Server (IP, Port)
│   ├── main.py                # Điểm khởi chạy Client & điều phối sự kiện P2P
│   ├── stream_manager.py      # Quản lý kết nối HUDP & SecureStream tới Server
│   ├── file_handler.py        # Xử lý truyền/nhận file P2P trực tiếp qua UDP
│   └── ui_bridge.py           # HTTP Server cục bộ làm cầu nối với Web UI
├── server/                    # Tầng ứng dụng phía Máy chủ
│   ├── config.py              # Cấu hình cổng lắng nghe (9000), giới hạn kết nối
│   ├── main.py                # Vòng lặp nhận datagram UDP lõi
│   ├── stream_manager.py      # Mux/Demux các kết nối PacketService & SecureStream
│   ├── state_manager.py       # Quản lý trạng thái đăng nhập (username <-> addr)
│   ├── router.py              # Phân luồng bản tin JSON theo ActionType
│   ├── chat_handler.py        # Nghiệp vụ Login, Chat riêng, Broadcast & User List
│   └── file_handler.py        # Signaling điều phối kết nối P2P giữa 2 Client
├── shared/                    # Lõi giao thức HUDP & Bảo mật dùng chung
│   ├── constants.py           # Định nghĩa cờ điều khiển (SYN, ACK, FIN, RST, PSH)
│   ├── exceptions.py          # Hệ thống ngoại lệ tầng truyền vận
│   ├── algorithms.py          # Checksum 16-bit, số học Modulo 2^32, SlidingWindow
│   ├── packet.py              # Cấu trúc gói tin 18-byte (pack/unpack)
│   ├── packet_service.py      # Lõi giao thức TCP-over-UDP (CUBIC, RTO, Flow Control)
│   ├── mux.py                 # Bộ phân kênh ServerMux độc lập
│   ├── security.py            # X25519 ECDH, AES-256-GCM (SecureStream, CryptoBox)
│   └── schemas.py             # Định dạng bản tin giao tiếp chuẩn hóa bằng Pydantic
├── ui/                        # Giao diện người dùng Web (HTML5, CSS3, Vanilla JS)
│   ├── index.html
│   ├── style.css
│   └── app.js
├── tests/                     # Unit & Integration Tests (Pytest)
├── simulations/               # Các kịch bản mô phỏng & kiểm thử chịu tải
│   ├── simulate_ui.py         # Chạy tự động 1 Server + 3 Client Web UI
│   ├── chaos_network.py       # Giả lập rớt gói 30% & ngắt kết nối đột ngột
│   ├── p2p_stress.py          # Kiểm thử truyền nhiều file P2P song song & 2 chiều
│   ├── load_test.py           # Kiểm thử chịu tải 20 Client đồng thời
│   └── demo_advanced_cases.py # Demo Tamper Attack, Replay Attack & Benchmark
├── run.py                     # Bảng điều khiển tương tác (Interactive CLI Control Panel)
├── huong_dan_ki_thuat.md      # Tài liệu đặc tả kỹ thuật chi tiết (Core, Server, Client)
└── requirement.txt            # Danh sách thư viện phụ thuộc

```

## 3. Yêu cầu Hệ thống & Cài đặt

* **Môi trường:** Python **3.10+** (hỗ trợ Windows, Linux, macOS).

* **Cài đặt môi trường ảo & thư viện:**

```
# 1. Tạo môi trường ảo
python -m venv venv

# 2. Kích hoạt môi trường ảo
# Trên Windows:
venv\Scripts\activate
# Trên Linux / macOS:
source venv/bin/activate

# 3. Cài đặt thư viện
pip install -r requirement.txt

```

*(Lưu ý: Nếu chạy nhiều Client trên các máy khác nhau trong mạng LAN, hãy cập nhật `SERVER_IP` trong file `client/config.py` thành địa chỉ IP LAN của máy chạy Server).*

## 4. Hướng dẫn Chạy Ứng dụng

### Cách 1: Sử dụng Control Panel (`run.py`) — Khuyên dùng

Tại thư mục gốc dự án, chạy lệnh:

```
python run.py

```

Menu điều khiển sẽ hiển thị các tùy chọn:

* **`[1]` Khởi động Máy chủ:** Mở Server tại cổng `UDP 9000`.

* **`[2]` Khởi động Máy khách:** Nhập cổng Web UI (mặc định `8080`), sau đó mở trình duyệt tại `http://127.0.0.1:8080`.

* **`[3]` Chạy Demo Tự động:** Tự động bật 1 Server và 3 cửa sổ Client tại các cổng `8001`, `8002`, `8003`.

* **`[4]` – `[7]` Chạy mô phỏng & kiểm thử:** Chaos Network, P2P Stress Test, Load Test (20 bots), và Demo Bảo mật nâng cao.

### Cách 2: Chạy thủ công từng thành phần qua Terminal

Đảm bảo đứng ở thư mục gốc (`MMT_ASS1`) để Python nhận diện đúng các package `shared`, `server`, `client`:

```
# Terminal 1: Khởi chạy Server
python -m server.main

# Terminal 2: Khởi chạy Client 1 (Web UI cổng 8081)
python -m client.main 8081

# Terminal 3: Khởi chạy Client 2 (Web UI cổng 8082)
python -m client.main 8082

```

## 5. Kiểm thử Tự động (Unit Tests)

Chạy bộ kiểm thử lõi giao thức (đóng gói packet, checksum, wrap-around 32-bit, out-of-order reassembly và mã hóa AES-GCM):

```
pytest tests/test_protocol.py -v

```