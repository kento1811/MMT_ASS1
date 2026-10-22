class TransportError(Exception):
    """Base class cho mọi lỗi transport.

    Caller có thể bắt TransportError để xử lý chung mọi lỗi mạng,
    hoặc bắt từng subclass để xử lý riêng.
    """


class ProtocolError(TransportError):
    """Lỗi logic/cấu hình, không phải lỗi mạng.

    Nguyên nhân thường gặp:
    - Gọi sai thứ tự (send/receive trước connect, close khi chưa established).
    - Dùng connect() với shared_socket=True.
    - SlidingWindow thiếu buffered_bytes().
    - peer_address chưa được set khi gửi packet.

    Đây là lỗi lập trình, không nên retry — caller cần sửa code.
    """


class ConnectionTimeoutError(TransportError):
    """Timeout hoặc peer không phản hồi.

    Nguyên nhân thường gặp:
    - connect() không nhận được SYN-ACK trong max_retries.
    - send() không nhận được ACK cho packet sau max_retries retransmit.
    - receive() hết timeout mà không có data.
    - Idle timeout: không nhận gì từ peer trong IDLE_TIMEOUT giây.
    - close() không nhận được ACK cho FIN.

    Với file transfer: đây là dấu hiệu file bị cắt giữa chừng.
    Caller nên coi đây là lỗi nghiêm trọng, không phải EOF bình thường.
    """

class PeerResetError(ConnectionResetError):
    """Peer gửi RST flag (protocol-level reset).

    Khác với ConnectionResetError của OS (Windows ICMP unreachable).
    Caller có thể bắt riêng nếu cần phân biệt.
    """