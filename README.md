# HUDP Collaboration Platform

> **Distributed and Streaming-based Client-Server Collaboration Application over custom HUDP Protocol**

[Python Version: 3.10+]
[License: MIT]

Đồ án môn **Mạng Máy Tính** - Trường Đại học Bách Khoa - ĐHQG TP.HCM.

---

## 1. Giới thiệu Tổng quan

**HUDP Platform** là ứng dụng cộng tác thời gian thực cho phép nhắn tin và chia sẻ tệp tin trực tiếp (Peer-to-Peer) trong môi trường mạng LAN khép kín.

Dự án **hoàn toàn sử dụng Raw Socket UDP** thông qua giao thức tùy chỉnh **HUDP (High User Datagram Protocol)**, tích hợp các cơ chế:

- **Độ tin cậy:** Sequence Number, Cumulative ACK, RTO linh hoạt, Fast Retransmit.
- **Kiểm soát tắc nghẽn & luồng:** Cửa sổ trượt (Sliding Window) kết hợp thuật toán **CUBIC Congestion Control**.
- **Bảo mật:** Trao đổi khóa **ECDH (X25519)** và mã hóa dữ liệu **AES-256-GCM**.
- **UI & Truyền tệp P2P:** Giao diện Web (HTTP Bridge/JS) và Đục lỗ NAT kết hợp Chunking 1024-byte.

---

## 2. Cấu trúc Thư mục

``` tree
.
├── client/          # Logic Client (Stream Manager, File Handler, UI Bridge)
├── server/          # Logic Server (Mux/Demux, Router, State Manager)
├── shared/          # Lõi giao thức HUDP (Packet, Crypto, Sliding Window, CUBIC)
├── ui/              # Giao diện Web UI (HTML/CSS/JS)
├── tests/           # Unit & Integration Tests (Pytest)
├── simulations/     # Các kịch bản Chaos, Load Test, và UI Simulation
├── run.py           # Menu tương tác để chạy hệ thống (Interactive CLI)
└── README.txt       # Tài liệu bạn đang đọc
```

---

## 3. Yêu cầu Hệ thống & Cài đặt

- **Yêu cầu:** Python 3.10+
- **Cài đặt thư viện:**
Mở Terminal/Command Prompt tại thư mục gốc và chạy:

``` bash
  python -m venv venv
```

(Windows):

``` bash
venv\Scripts\activate
```

(macOS/Linux):

``` bash
source venv/bin/activate
```  

Cài các thư viện cần thiết.

``` bash
pip install -r requirements.txt
```

---

## 4. Hướng dẫn Chạy Ứng dụng (Sử dụng run.py)

Dự án sử dụng giao diện Menu tương tác thân thiện. Bạn chỉ cần chạy lệnh duy nhất sau tại thư mục gốc:

``` bash
  python run.py
```

Một Control Panel sẽ hiện ra với các lựa chọn:

  [1] Khởi động Server (Chạy máy chủ tại cổng UDP 9000).

  [2] Khởi động Client (Hệ thống sẽ hỏi bạn muốn dùng cổng bao nhiêu cho Web UI).

  [3] Chạy Demo Tự động (Tự bật 1 Server và 3 Client Web UI cùng lúc - Rất tiện để test nhanh).
  
  [4] -> [7] Chạy các kịch bản kiểm thử (Stress test, Load test, Security, Chaos Network).

Bạn chỉ cần nhập số tương ứng và nhấn Enter!

---

## 5. Hướng dẫn Kiểm thử tự động (Unit Tests)

Để chạy kiểm tra chuyên sâu lõi HUDP và các thuật toán mã hóa (không cần bật Server/Client):

``` bash
pytest tests/test_protocol.py -v
```
