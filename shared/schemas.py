
from pydantic import BaseModel, Field
from typing import Literal, Union
from enum import Enum
from typing_extensions import Annotated


# Enum định nghĩa các loại hành động (Action) giữa Client và Server
class ActionType(str, Enum):
    # Type 1: Các yêu cầu (Request) từ Client gửi lên Server
    REQ_LOGIN = "REQ_LOGIN"
    REQ_CHAT = "REQ_CHAT"
    REQ_BROADCAST = "REQ_BROADCAST"
    REQ_FILE_SEND = "REQ_FILE_SEND"

    # Type 2: Các phản hồi (Response) từ Client hoặc Server
    RES_GENERAL = "RES_GENERAL"
    RES_FILE_ACCEPT = "RES_FILE_ACCEPT"
    RES_FILE_REJECT = "RES_FILE_REJECT"

    # Type 3: Các thông báo (Feedback) từ Server gửi xuống Client
    INCOMING_CHAT = "INCOMING_CHAT"
    INCOMING_FILE_REQ = "INCOMING_FILE_REQ"
    SIGNAL_FILE_READY = "SIGNAL_FILE_READY"


# =================
# ---- CLIENT -----
# =================

# Request đăng nhập: Client gửi username lên Server
class LoginRequest(BaseModel):
    action: Literal[ActionType.REQ_LOGIN] = ActionType.REQ_LOGIN
    username: str


# Request gửi tin nhắn riêng đến một Client khác
class ChatRequest(BaseModel):
    action: Literal[ActionType.REQ_CHAT] = ActionType.REQ_CHAT
    to_user: str
    msg: str


# Request gửi tin nhắn đến tất cả Client đang kết nối
class BroadcastRequest(BaseModel):
    action: Literal[ActionType.REQ_BROADCAST] = ActionType.REQ_BROADCAST
    msg: str


# Request gửi file đến một Client khác
class FileSendRequest(BaseModel):
    action: Literal[ActionType.REQ_FILE_SEND] = ActionType.REQ_FILE_SEND
    to_user: str
    filename: str
    filesize: int


# Response chấp nhận yêu cầu nhận file từ Client khác
class FileAcceptResponse(BaseModel):
    action: Literal[ActionType.RES_FILE_ACCEPT] = ActionType.RES_FILE_ACCEPT
    to_user: str
    session_id: str


# Response từ chối yêu cầu nhận file
class FileRejectResponse(BaseModel):
    action: Literal[ActionType.RES_FILE_REJECT] = ActionType.RES_FILE_REJECT
    to_user: str
    reason: str = "Tự chối nhận file"


# Tổng hợp các loại message mà Client có thể gửi lên Server
# Union cho phép message thuộc một trong các model được khai báo
# discriminator="action" giúp Pydantic xác định model dựa trên trường action
ClientMessage = Annotated[
    Union[
        LoginRequest,
        ChatRequest,
        BroadcastRequest,
        FileSendRequest,
        FileAcceptResponse,
        FileRejectResponse
    ],
    Field(discriminator="action")
]


# ==================
# ---- SERVER ------
# ==================

# Response tổng quát: thông báo kết quả xử lý request
class GeneralResponse(BaseModel):
    action: Literal[ActionType.RES_GENERAL] = ActionType.RES_GENERAL
    status: Literal["ok", "error"]
    message: str


# Thông báo tin nhắn mới nhận được từ một Client khác
class IncomingChat(BaseModel):
    action: Literal[ActionType.INCOMING_CHAT] = ActionType.INCOMING_CHAT
    from_user: str
    is_broadcast: bool = False
    msg: str


# Thông báo có Client khác gửi yêu cầu truyền file
class IncomingFileRequest(BaseModel):
    action: Literal[ActionType.INCOMING_FILE_REQ] = ActionType.INCOMING_FILE_REQ
    from_user: str
    filename: str
    filesize: int


# Thông báo cho Client biết file đã sẵn sàng để truyền
class SignalFileReady(BaseModel):
    action: Literal[ActionType.SIGNAL_FILE_READY] = ActionType.SIGNAL_FILE_READY
    from_user: str
    session_id: str


# Tổng hợp các loại message mà Server có thể gửi xuống Client
# ServerMessage có thể là một trong các model được khai báo bên dưới
# discriminator="action" giúp xác định chính xác loại message
ServerMessage = Annotated[
    Union[
        GeneralResponse,
        IncomingChat,
        IncomingFileRequest,
        SignalFileReady
    ],
    Field(discriminator="action")
]
