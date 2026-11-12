import hashlib
import hmac
import os
import struct
import threading
import time

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey, X25519PublicKey
)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF


"""Security layer: mã hóa dữ liệu và bảo vệ thông tin xác thực.

Sử dụng thư viện cryptography để triển khai:
- Password hashing: PBKDF2-HMAC-SHA256.
- Datagram encryption: AES-256-GCM với nonce ngẫu nhiên.
- Stream encryption: X25519 ECDH kết hợp HKDF và AES-256-GCM.

SecureStream tích hợp với PacketService để mã hóa message
truyền qua kết nối UDP đáng tin cậy.
"""


KEY_SIZE = 32
NONCE_SIZE = 12
PBKDF2_ITERATIONS = 200_000
MAX_MESSAGE_SIZE = 16 * 1024 * 1024
_LEN = struct.Struct("!I")


class SecurityError(Exception):
    """Lỗi phát sinh trong quá trình mã hóa, giải mã hoặc thiết lập phiên."""


# ----------------------------------------------------------------
# Password hashing
# ----------------------------------------------------------------

def derive_key(password, salt: bytes, iterations: int = PBKDF2_ITERATIONS) -> bytes:
    """Dẫn xuất khóa từ mật khẩu bằng PBKDF2-HMAC-SHA256."""
    if isinstance(password, str):
        password = password.encode("utf-8")
    return hashlib.pbkdf2_hmac("sha256", password, salt, iterations, KEY_SIZE)


def hash_password(password: str, iterations: int = PBKDF2_ITERATIONS) -> str:
    """Tạo chuỗi lưu trữ gồm thuật toán, số vòng lặp, salt và hash."""
    salt = os.urandom(16)
    digest = derive_key(password, salt, iterations)
    return f"pbkdf2${iterations}${salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    """Kiểm tra mật khẩu bằng phép so sánh an toàn về thời gian."""
    try:
        scheme, iters, salt_hex, hash_hex = stored.split("$")
        if scheme != "pbkdf2":
            return False
        digest = derive_key(password, bytes.fromhex(salt_hex), int(iters))
        return hmac.compare_digest(digest, bytes.fromhex(hash_hex))
    except (ValueError, TypeError):
        return False


# ----------------------------------------------------------------
# Datagram encryption
# ----------------------------------------------------------------

class CryptoBox:
    """Mã hóa và giải mã datagram bằng AES-256-GCM.

    Mỗi gói tin chứa nonce ngẫu nhiên cùng ciphertext và authentication tag.
    """

    def __init__(self, key: bytes):
        if len(key) != KEY_SIZE:
            raise ValueError("key phải dài 32 byte")
        self._aes = AESGCM(key)

    def encrypt(self, plaintext: bytes, aad: bytes = b"") -> bytes:
        nonce = os.urandom(NONCE_SIZE)
        return nonce + self._aes.encrypt(nonce, plaintext, aad)

    def decrypt(self, blob: bytes, aad: bytes = b"") -> bytes:
        if len(blob) < NONCE_SIZE + 16:
            raise SecurityError("Blob quá ngắn")
        try:
            return self._aes.decrypt(blob[:NONCE_SIZE], blob[NONCE_SIZE:], aad)
        except InvalidTag as e:
            raise SecurityError("Giải mã thất bại (sai khóa hoặc dữ liệu bị sửa)") from e


# ----------------------------------------------------------------
# Secure stream session
# ----------------------------------------------------------------

def generate_keypair():
    """Tạo cặp khóa X25519 và trả về public key ở dạng bytes."""
    priv = X25519PrivateKey.generate()
    pub = priv.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )
    return priv, pub


class SecureSession:
    """Quản lý khóa mã hóa và bộ đếm nonce cho hai chiều truyền dữ liệu.

    Mỗi chiều sử dụng một khóa AES-GCM riêng. Nonce được tạo từ
    counter để đảm bảo không lặp lại trong cùng một phiên.
    """

    def __init__(
        self,
        private_key,
        peer_public: bytes,
        is_server: bool,
        psk: bytes = b""
    ):
        try:
            shared = private_key.exchange(
                X25519PublicKey.from_public_bytes(peer_public)
            )
        except ValueError as e:
            raise SecurityError("Public key của peer không hợp lệ") from e

        salt = hashlib.sha256(b"udp-collab-v1" + psk).digest()

        def key(info):
            return HKDF(
                algorithm=hashes.SHA256(),
                length=KEY_SIZE,
                salt=salt,
                info=info
            ).derive(shared)

        c2s, s2c = key(b"client->server"), key(b"server->client")
        send_key, recv_key = (s2c, c2s) if is_server else (c2s, s2c)

        self._send = AESGCM(send_key)
        self._recv = AESGCM(recv_key)
        self._send_ctr = 0
        self._recv_ctr = 0

    @staticmethod
    def _nonce(counter: int) -> bytes:
        """Chuyển counter thành nonce 96-bit."""
        return b"\x00" * 4 + counter.to_bytes(8, "big")

    def seal(self, plaintext: bytes) -> bytes:
        """Mã hóa plaintext bằng khóa gửi hiện tại."""
        blob = self._send.encrypt(
            self._nonce(self._send_ctr), plaintext, None
        )
        self._send_ctr += 1
        return blob

    def open(self, blob: bytes) -> bytes:
        """Giải mã ciphertext bằng khóa nhận hiện tại."""
        try:
            plain = self._recv.decrypt(
                self._nonce(self._recv_ctr), blob, None
            )
        except InvalidTag as e:
            raise SecurityError("Giải mã thất bại (sai khóa hoặc dữ liệu bị sửa)") from e

        self._recv_ctr += 1
        return plain


class SecureStream:
    """Lớp mã hóa message trên PacketService.

    Handshake sử dụng X25519 để thiết lập khóa phiên.
    Message được đóng khung bằng length-prefix và bảo vệ bằng AES-GCM.
    """

    def __init__(self, conn, is_server: bool, psk: bytes = b""):
        self.conn = conn
        self.is_server = is_server
        self.psk = psk
        self.session = None
        self._buf = bytearray()
        self._send_lock = threading.Lock()
        self._recv_lock = threading.Lock()
        self._eof = False

    def _fill(self, n: int, timeout=None) -> bool:
        """Đọc dữ liệu đến khi buffer đủ n byte hoặc gặp EOF.

        Dữ liệu chưa bị tiêu thụ cho đến khi gọi _take(), tránh
        làm lệch framing nếu thao tác đọc bị timeout.
        """
        deadline = None if timeout is None else time.monotonic() + timeout

        while len(self._buf) < n:
            if self._eof:
                break

            remaining = None
            if deadline is not None:
                remaining = max(0.0, deadline - time.monotonic())

            chunk = self.conn.receive(timeout=remaining)
            if chunk == b"":
                self._eof = True
                break

            self._buf += chunk

        if len(self._buf) >= n:
            return True

        if self._buf:
            raise SecurityError("Stream kết thúc giữa chừng một message!")

        return False

    def _take(self, n: int) -> bytes:
        """Lấy và xóa n byte đầu tiên khỏi buffer."""
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out

    def handshake(self, timeout=10):
        """Trao đổi public key và khởi tạo SecureSession."""
        priv, pub = generate_keypair()

        if self.is_server:
            ok = self._fill(32, timeout)
            self.conn.send(pub)
        else:
            self.conn.send(pub)
            ok = self._fill(32, timeout)

        if not ok:
            raise SecurityError("Peer đóng kết nối trong lúc handshake!")

        peer = self._take(32)
        self.session = SecureSession(priv, peer, self.is_server, self.psk)

    def send_message(self, plaintext: bytes):
        """Mã hóa và gửi một message theo định dạng length-prefix."""
        if self.session is None:
            raise SecurityError("Chưa handshake!")

        if len(plaintext) > MAX_MESSAGE_SIZE:
            raise ValueError("Message quá lớn")

        with self._send_lock:
            # Giữ thứ tự counter mã hóa đồng bộ với thứ tự gửi.
            blob = self.session.seal(plaintext)
            self.conn.send(_LEN.pack(len(blob)) + blob)

    def recv_message(self, timeout=None):
        """Nhận và giải mã message; trả về None nếu gặp EOF sạch."""
        if self.session is None:
            raise SecurityError("Chưa handshake!")

        with self._recv_lock:
            if not self._fill(_LEN.size, timeout):
                return None

            (length,) = _LEN.unpack(bytes(self._buf[:_LEN.size]))

            if length < 16 or length > MAX_MESSAGE_SIZE + 16:
                raise SecurityError("Độ dài frame không hợp lệ")

            if not self._fill(_LEN.size + length, timeout):
                raise SecurityError("Stream kết thúc giữa chừng một message!")

            self._take(_LEN.size)
            return self.session.open(self._take(length))

    def close(self, timeout=3):
        """Đóng kết nối PacketService."""
        return self.conn.close(timeout)