from pydantic import BaseModel, Field
from typing import Literal, Union
from enum import Enum
from typing_extensions import Annotated


# Enum Type Action for Client Server
class ActionType(str, Enum):
    # Type 1: Request
    REQ_LOGIN = "REQ_LOGIN"
    REQ_CHAT  = "REQ_CHAT"
    REQ_BROADCAST = "REQ_BROADCAST"
    REQ_FILE_SEND = "REQ_FILE_SEND"

    # Type 2: Response
    RES_GENERAL = "RES_GENERAL"
    RES_FILE_ACCEPT = "RES_FILE_ACCEPT"
    RES_FILE_REJECT = "RES_FILE_REJECT"

    # Type 3: Server Feedback
    INCOMING_CHAT = "INCOMING_CHAT"
    INCOMING_FILE_REQ = "INCOMING_FILE_REQ"
    SIGNAL_FILE_READY = "SIGNAL_FILE_READY"


# =================
# ---- CLIENT -----
# =================


class LoginRequest(BaseModel):
    action: Literal[ActionType.REQ_LOGIN] = ActionType.REQ_LOGIN
    username: str

class ChatRequest(BaseModel):
    action: Literal[ActionType.REQ_CHAT] = ActionType.REQ_CHAT
    to_user: str
    msg: str

class BroadcastRequest(BaseModel):
    action: Literal[ActionType.REQ_BROADCAST] = ActionType.REQ_BROADCAST
    msg: str

class FileSendRequest(BaseModel):
    action: Literal[ActionType.REQ_FILE_SEND] = ActionType.REQ_FILE_SEND
    to_user: str
    filename: str
    filesize: int


class FileAcceptResponse(BaseModel):
    action: Literal[ActionType.RES_FILE_ACCEPT] = ActionType.RES_FILE_ACCEPT
    to_user: str 
    session_id: str

class FileRejectResponse(BaseModel):
    action: Literal[ActionType.RES_FILE_REJECT] = ActionType.RES_FILE_REJECT
    to_user: str
    reason: str = "Tự chối nhận file"


# Tổng hợp các loại tin nhắn Client có thể đẩy lên Server
ClientMessage = Annotated[
    Union[
        LoginRequest, ChatRequest, BroadcastRequest, 
        FileSendRequest, FileAcceptResponse, FileRejectResponse
    ], 
    Field(discriminator="action")
]


# ==================
# ---- Server ----
# =================


class GeneralResponse(BaseModel):
    action: Literal[ActionType.RES_GENERAL] = ActionType.RES_GENERAL
    status: Literal["ok", "error"]
    message: str

class IncomingChat(BaseModel):
    action: Literal[ActionType.INCOMING_CHAT] = ActionType.INCOMING_CHAT
    from_user: str
    is_broadcast: bool = False
    msg: str

class IncomingFileRequest(BaseModel):
    action: Literal[ActionType.INCOMING_FILE_REQ] = ActionType.INCOMING_FILE_REQ
    from_user: str
    filename: str
    filesize: int

class SignalFileReady(BaseModel):
    action: Literal[ActionType.SIGNAL_FILE_READY] = ActionType.SIGNAL_FILE_READY
    from_user: str
    session_id: str

# Tổng hợp các loại tin nhắn Server có thể đẩy xuống Client
ServerMessage = Annotated[
    Union[
        GeneralResponse, IncomingChat, 
        IncomingFileRequest, SignalFileReady
    ], 
    Field(discriminator="action")
]