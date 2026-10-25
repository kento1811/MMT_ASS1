from server.state_manager import StateManager
from server.stream_manager import StreamManager
from shared.schemas import (
    ActionType, FileSendRequest, FileAcceptResponse, FileRejectResponse,
    IncomingFileRequest, SignalFileReady, GeneralResponse
)

class FileHandler:
    def __init__(
        self,
        state_manager: StateManager,
        stream_manager: StreamManager,
    ):
        self.state_manager = state_manager
        self.stream_manager = stream_manager

    def process_signaling(self, sender_address: tuple, msg_obj):
        """
        Nhận gói tin JSON liên quan đến File từ Router và phân loại xử lý.
        """
        action = msg_obj.action
        
        if action == ActionType.REQ_FILE_SEND:
            self._handle_file_request(sender_address, msg_obj)
        elif action == ActionType.RES_FILE_ACCEPT:
            self._handle_file_accept(sender_address, msg_obj)
        elif action == ActionType.RES_FILE_REJECT:
            self._handle_file_reject(sender_address, msg_obj)

    # --- Các hàm logic đã được chuyển từ Router sang ---

    def _handle_file_request(self, sender_address: tuple, msg_obj: FileSendRequest):
        sender_username = self.state_manager.get_username(sender_address)
        if not sender_username: return

        receiver_address = self.state_manager.get_address(msg_obj.to_user)
        if receiver_address:
            req = IncomingFileRequest(
                from_user=sender_username,
                filename=msg_obj.filename,
                filesize=msg_obj.filesize
            )
            self._send_to_client(receiver_address, req)
        else:
            self._send_error(sender_address, f"'{msg_obj.to_user}' đang offline, không thể gửi file.")

    def _handle_file_accept(self, sender_address: tuple, msg_obj: FileAcceptResponse):
        bob_username = self.state_manager.get_username(sender_address)
        if not bob_username: return
        
        alice_address = self.state_manager.get_address(msg_obj.to_user)
        if alice_address:
            ready_msg = SignalFileReady(
                from_user=bob_username,
                session_id=msg_obj.session_id
            )
            self._send_to_client(alice_address, ready_msg)

    def _handle_file_reject(self, sender_address: tuple, msg_obj: FileRejectResponse):
        bob_username = self.state_manager.get_username(sender_address)
        alice_address = self.state_manager.get_address(msg_obj.to_user)
        
        if bob_username and alice_address:
            err_msg = GeneralResponse(
                status="error", 
                message=f"'{bob_username}' đã từ chối nhận file. Lý do: {msg_obj.reason}"
            )
            self._send_to_client(alice_address, err_msg)

    # Helper nội bộ cho FileHandler
    def _send_to_client(self, address: tuple, response_obj):
        try:
            json_bytes = response_obj.model_dump_json().encode('utf-8')
            self.stream_manager.send_to(address, json_bytes)
        except Exception as e:
            print(f"[FileHandler] Lỗi khi gửi dữ liệu tới {address}: {e}")

    def _send_error(self, address: tuple, message: str):
        err_obj = GeneralResponse(status="error", message=message)
        self._send_to_client(address, err_obj)