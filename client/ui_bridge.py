import json
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from queue import Queue

# Hàng đợi luân chuyển tin nhắn từ luồng Mạng (UDP) sang luồng Web (HTTP)
MESSAGE_QUEUE = Queue()

# Biến ngữ cảnh toàn cục để HTTP Request Handler truy cập được các Manager
APP_CONTEXT = {"stream_manager": None, "file_handler": None, "pending_uploads": {}}


class RequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Tắt log HTTP mặc định của Python để CLI không bị rác màn hình
        pass

    def _send_response_json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-type", "application/json")
        # Hỗ trợ CORS nếu chạy UI ở port khác khi debug
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def do_GET(self):
        # ==================================================
        # 1. API: Kênh Polling để Frontend lấy tin nhắn mới
        # ==================================================
        if self.path == "/api/messages":
            msgs = []
            # Rút toàn bộ tin nhắn đang kẹt trong hàng đợi ra
            while not MESSAGE_QUEUE.empty():
                msgs.append(MESSAGE_QUEUE.get())
            self._send_response_json(msgs)
            return

        # ==================================================
        # 2. ROUTER: Phục vụ các file tĩnh (Giao diện Web)
        # ==================================================
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "ui"))
        filepath = "/index.html" if self.path == "/" else self.path
        full_path = base_dir + filepath

        if os.path.exists(full_path) and os.path.isfile(full_path):
            self.send_response(200)
            if filepath.endswith(".html"):
                self.send_header("Content-type", "text/html")
            elif filepath.endswith(".css"):
                self.send_header("Content-type", "text/css")
            elif filepath.endswith(".js"):
                self.send_header("Content-type", "application/javascript")
            self.end_headers()

            with open(full_path, "rb") as f:
                self.wfile.write(f.read())
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"404 Not Found")

    def do_POST(self):
        # ==================================================
        # 3. API: Nhận lệnh thao tác từ giao diện Web (JS)
        # ==================================================
        if self.path == "/api/command":
            content_length = int(self.headers.get("Content-Length", 0))
            raw_data = self.rfile.read(content_length)
            try:
                payload = json.loads(raw_data.decode("utf-8"))
                self._process_command(payload)
                self._send_response_json({"status": "ok"})
            except Exception as e:
                self._send_response_json({"status": "error", "message": str(e)}, 400)
        else:
            self.send_response(404)
            self.end_headers()

    def _process_command(self, payload):
        """Dịch lệnh JSON từ UI thành các Model Pydantic và gửi đi"""
        from shared.schemas import (
            LoginRequest,
            ChatRequest,
            BroadcastRequest,
            FileSendRequest,
            FileAcceptResponse,
            FileRejectResponse,
        )

        cmd = payload.get("cmd")
        sm = APP_CONTEXT["stream_manager"]
        fh = APP_CONTEXT["file_handler"]

        if not sm or not sm.is_connected:
            raise Exception("Chưa kết nối tới Server mạng.")

        if cmd == "login":
            sm.send_json(LoginRequest(username=payload.get("username")))

        elif cmd == "chat":
            sm.send_json(
                ChatRequest(to_user=payload.get("to_user"), msg=payload.get("msg"))
            )

        elif cmd == "all":
            sm.send_json(BroadcastRequest(msg=payload.get("msg")))

        elif cmd == "file":
            to_user = payload.get("to_user")
            filepath = payload.get("filepath")
            if os.path.exists(filepath):
                filename = os.path.basename(filepath)
                # Lưu file gửi vào cache để chờ bên kia accept
                APP_CONTEXT["pending_uploads"][filename] = filepath

                req = FileSendRequest(
                    to_user=to_user,
                    filename=filename,
                    filesize=os.path.getsize(filepath),
                )
                sm.send_json(req)
                MESSAGE_QUEUE.put(
                    {"action": "SYSTEM", "msg": f"Đang xin phép {to_user} gửi file..."}
                )
            else:
                raise Exception(f"Không tìm thấy file: {filepath}")

        elif cmd == "accept":
            to_user = payload.get("to_user")
            filename = payload.get("filename")

            # Khởi động P2P Listener
            listen_port = fh.start_receiving(filename)
            session_id = f"{listen_port}:{filename}"
            sm.send_json(FileAcceptResponse(to_user=to_user, session_id=session_id))
            MESSAGE_QUEUE.put(
                {"action": "SYSTEM", "msg": f"Đang mở luồng P2P nhận '{filename}'..."}
            )

        elif cmd == "reject":
            sm.send_json(
                FileRejectResponse(to_user=payload.get("to_user"), reason="Từ chối")
            )


class UIBridge:
    def __init__(self, host="0.0.0.0", port=8080):
        self.host = host
        self.port = port
        self.server = HTTPServer((self.host, self.port), RequestHandler)

    def setup_context(self, stream_manager, file_handler, pending_uploads):
        """Kết nối UI Bridge với các Manager lõi"""
        APP_CONTEXT["stream_manager"] = stream_manager
        APP_CONTEXT["file_handler"] = file_handler
        APP_CONTEXT["pending_uploads"] = pending_uploads

    def push_message_to_ui(self, msg_obj: dict):
        """Được gọi bởi StreamManager khi có gói tin mạng mới tới"""
        MESSAGE_QUEUE.put(msg_obj)

    def start(self):
        """Khởi chạy HTTP Server trên một luồng riêng"""
        print(f"🌐 Bật trình duyệt, truy cập: http://{self.host}:{self.port}")
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
