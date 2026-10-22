# MMT_ASS1

``` tree
MMT_Ass1/
├── .venv/                      # Thư mục môi trường ảo Python (sẽ bị Git bỏ qua)
├── .env                        # Chứa các biến môi trường nhạy cảm (sẽ bị Git bỏ qua)
├── .env.example                # Chứa khung mẫu các biến môi trường để người khác biết cách setup
├── .gitignore                  # File thiết lập các file/thư mục không đưa lên Git (.env, .venv, __pycache__)
├── requirements.txt            # Danh sách các thư viện cần cài đặt (ví dụ: pydantic, cryptography)
├── README.md                   # Hướng dẫn chi tiết từng bước cách cài đặt và chạy server/client (Yêu cầu nộp bài bắt buộc)
│
├── run.py                      # File entry-point duy nhất: 'python run.py server' hoặc 'python run.py client'
│
├── scripts/                    # Các kịch bản hỗ trợ tự động hóa
│   ├── setup.sh                # Script cài đặt môi trường cho Linux/Mac
│   ├── setup.bat               # Script cài đặt môi trường cho Windows
│   └── simulate.py             # Tự động mở nhiều cửa sổ terminal chạy 1 Server và nhiều Client để test (Bước 10)[cite: 1]
│
├── tests/                      # Chứa các unit test
│   ├── __init__.py
│   ├── conftest.py             # Cấu hình test được sử dụng 
│   └── test_protocol.py        # Kịch bản test: Đóng gói (mux) và bóc tách (demux) xem data có bị suy hao không
│
├── shared/                     # Thư mục dùng chung (Bộ não của giao thức HUDP)
│   ├── __init__.py
│   ├── constants.py            # Định nghĩa các hằng số mạng (MTU, timeout) và loại tin nhắn (AUTH, MSG, FILE)
│   ├── schemas.py              # Pydantic models để mã hóa/giải mã dữ liệu có cấu trúc thành JSON
│   ├── protocol.py             # Dùng 'struct' thực hiện đóng/mở gói (mux/demux), gắn Custom HUDP Headers
│   ├── security.py             # Chứa hàm mã hóa/giải mã (AES) cho data payloads trước khi truyền
│   ├── algorithms.py           # Thuật toán tính toán cửa sổ trượt (sliding window), băm file (chunking)
│   └── exceptions.py           # Các class lỗi tự định nghĩa (Lệch sequence, sai checksum, rớt mạng)
│
├── server/                     # Máy chủ trung tâm (Điều phối & Định tuyến)
│   ├── __init__.py
│   ├── main.py                 # Điểm khởi chạy của server (khởi tạo raw socket, loop đa luồng)
│   ├── config.py               # Cấu hình IP Host, Port, giới hạn client (30-40 concurrent clients)
│   ├── state_manager.py        # Lưu trạng thái: ai đang online, IP và Port hiện tại là gì
│   ├── router.py               # Logic định tuyến: nhận tin nhắn và chuyển tiếp (forward) đến đúng người nhận
│   ├── stream_manager.py       # Duy trì số thứ tự (sequence number), xử lý xác nhận (ACK) và truyền lại
│   └── file_handler.py         # Quản lý luồng truyền file lớn đi qua server
│
├── client/                     # Máy khách (Xử lý giao tiếp cục bộ & Kết nối mạng)
│   ├── __init__.py
│   ├── main.py                 # Điểm khởi chạy của client
│   ├── config.py               # File đọc cấu hình IP/Port của server cần kết nối
│   ├── stream_manager.py       # Quản lý thứ tự UDP, điều khiển luồng (congestion control) phía client
│   ├── file_handler.py         # Chia nhỏ (chunk) file thành nhiều mảnh để gửi, hoặc ráp file lại khi nhận
│   └── ui_bridge.py            # HTTP Server nội bộ thu nhỏ, đóng vai trò cầu nối giữa logic mạng Python và giao diện Web
│
└── ui/                         # Giao diện người dùng (Theo yêu cầu: HTTP based UI)
    ├── index.html              # Khung giao diện chính
    ├── app.js                  # Chỉ sử dụng Client-side JavaScript để gọi API nội bộ tới 'client/ui_bridge.py'
    └── style.css               # Giao diện hiển thị tin nhắn, thanh tiến trình tải file, danh sách user online
```
