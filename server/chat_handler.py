from server.state_manager import StateManager
from server.stream_manager import StreamManager
from shared.schemas import (
    ActionType, LoginRequest, ChatRequest, BroadcastRequest,
    GeneralResponse, IncomingChat
)

class ChatHandler:
    def __init__(
        self,
        state_manager: StateManager,
        stream_manager: StreamManager,
    ):
        self.state_manager = state_manager
        self.stream_manager = stream_manager

    def process_action(self, sender_address: tuple, msg_obj):
        """Nhận gói tin JSON liên quan đến Chat/Login từ Router và xử lý"""
        action = msg_obj.action
        
        if action == ActionType.REQ_LOGIN:
            self._handle_login(sender_address, msg_obj)
        elif action == ActionType.REQ_CHAT:
            self._handle_chat(sender_address, msg_obj)
        elif action == ActionType.REQ_BROADCAST:
            self._handle_broadcast(sender_address, msg_obj)

    # ==========================================
    # LOGIC NGHIỆP VỤ CHAT
    # ==========================================
    def _handle_login(self, sender_address: tuple, msg_obj: LoginRequest):
        print(f"[ChatHandler] Yêu cầu LOGIN từ {msg_obj.username} tại {sender_address}")
        success = self.state_manager.login(msg_obj.username, sender_address)
        
        if success:
            res = GeneralResponse(status="ok", message="Đăng nhập thành công!")
            self._send_to_client(sender_address, res)
            
            # Broadcast danh sách user khi có người mới vào 
            all_users = self.state_manager.get_all_users_online()
            update_msg = {"action": "USER_LIST", "users": all_users}
            
            import json
            update_bytes = json.dumps(update_msg).encode('utf-8')
            for user in all_users:
                addr = self.state_manager.get_address(user)
                if addr:
                    self.stream_manager.send_to(addr, update_bytes)

    def _handle_chat(self, sender_address: tuple, msg_obj: ChatRequest):
        sender_username = self.state_manager.get_username(sender_address)
        if not sender_username:
            self._send_error(sender_address, "Bạn cần đăng nhập trước khi gửi tin nhắn.")
            return

        receiver_address = self.state_manager.get_address(msg_obj.to_user)
        if receiver_address:
            forward_msg = IncomingChat(
                from_user=sender_username,
                is_broadcast=False,
                msg=msg_obj.msg
            )
            self._send_to_client(receiver_address, forward_msg)
        else:
            self._send_error(sender_address, f"Người dùng '{msg_obj.to_user}' hiện không online.")

    def _handle_broadcast(self, sender_address: tuple, msg_obj: BroadcastRequest):
        sender_username = self.state_manager.get_username(sender_address)
        if not sender_username: return

        all_users = self.state_manager.get_all_users_online()
        forward_msg = IncomingChat(
            from_user=sender_username,
            is_broadcast=True,
            msg=msg_obj.msg
        )
        
        for target_user in all_users:
            if target_user != sender_username:
                target_address = self.state_manager.get_address(target_user)
                if target_address:
                    self._send_to_client(target_address, forward_msg)

    # --- Helpers nội bộ ---
    def _send_to_client(self, address: tuple, response_obj):
        try:
            json_bytes = response_obj.model_dump_json().encode('utf-8')
            self.stream_manager.send_to(address, json_bytes)
        except Exception as e:
            print(f"[ChatHandler] Lỗi khi gửi dữ liệu tới {address}: {e}")

    def _send_error(self, address: tuple, message: str):
        err_obj = GeneralResponse(status="error", message=message)
        self._send_to_client(address, err_obj)