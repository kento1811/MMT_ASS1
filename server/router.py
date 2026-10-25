from pydantic import TypeAdapter, ValidationError

from server.chat_handler import ChatHandler
from server.file_handler import FileHandler
from server.stream_manager import StreamManager
from shared.schemas import ClientMessage, ActionType, GeneralResponse

class Router:
    def __init__(
        self,
        stream_manager: StreamManager,
        chat_handler: ChatHandler,
        file_handler: FileHandler,
    ):
        self.stream_manager = stream_manager
        self.chat_handler = chat_handler
        self.file_handler = file_handler

        self.msg_adapter = TypeAdapter(ClientMessage)

    def process_message(
        self,
        sender_address: tuple,
        payload: bytes,
    ):
        try:
            msg_obj = self.msg_adapter.validate_json(payload)
            action = msg_obj.action

            if action in (ActionType.REQ_LOGIN, ActionType.REQ_CHAT, ActionType.REQ_BROADCAST):
                self.chat_handler.process_action(sender_address, msg_obj)

            elif action in (ActionType.REQ_FILE_SEND, ActionType.RES_FILE_ACCEPT, ActionType.RES_FILE_REJECT):
                self.file_handler.process_signaling(sender_address, msg_obj)
                
            else:
                print(f"[Router] Bỏ qua hành động không được hỗ trợ: {action}")

        except ValidationError as e:
            print(f"[Router] Lỗi cấu trúc JSON từ {sender_address}: {e}")
            self._send_error(sender_address, "Cấu trúc bản tin không hợp lệ.")
        except Exception as e:
            print(f"[Router] Lỗi nội bộ: {e}")
            self._send_error(sender_address, "Lỗi nội bộ Server.")

    def _send_error(self, address: tuple, message: str):
        """Router chỉ cần tự gửi lỗi khi bản tin bị sai cấu trúc ngay từ vòng gửi xe"""
        err_obj = GeneralResponse(status="error", message=message)
        try:
            self.stream_manager.send_to(address, err_obj.model_dump_json().encode('utf-8'))
        except:
            pass