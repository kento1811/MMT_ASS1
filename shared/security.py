"""Security layer: mã hóa payload (AES-256-GCM) + băm mật khẩu cho authentication.

Thư viện: `cryptography` (pip install cryptography) — không tự cài đặt thuật toán.

Có 3 mức dùng:

1. Mật khẩu (auth, bước 3):
       hash_password() / verify_password()      PBKDF2-HMAC-SHA256 + salt

2. Datagram độc lập (UDP heartbeat / discovery broadcast, không có stream):
       box = CryptoBox(derive_key(passphrase, salt))
       blob = box.encrypt(b"...")  ->  box.decrypt(blob)
   Mỗi datagram mang nonce ngẫu nhiên riêng nên không cần trạng thái.

3. Stream đáng tin cậy (TCP tự cài trên UDP — PacketService):
       sec = SecureStream(packet_service, is_server=True/False, psk=b"...")
       sec.handshake()                 # X25519 ECDH -> 2 khóa AES (mỗi chiều 1 khóa)
       sec.send_message(b"...")        # length-prefix + AES-GCM
       msg = sec.recv_message(timeout) # None khi peer đóng (EOF)
   Có forward secrecy (khóa phiên tạm thời). Nếu hai bên đặt `psk` giống nhau,
   psk được trộn vào HKDF: sai psk => giải mã thất bại => chống kẻ lạ xen vào.
"""

import hashlib
import hmac
import os
import struct
import threading
import time

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey, X25519PublicKey)
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

KEY_SIZE = 32          # AES-256
NONCE_SIZE = 12
PBKDF2_ITERATIONS = 200_000
MAX_MESSAGE_SIZE = 16 * 1024 * 1024
_LEN = struct.Struct("!I")


class SecurityError(Exception):
    """Lỗi của security layer (giải mã sai, dữ liệu bị sửa, handshake hỏng)."""


# ---------------------------------------------------------------- mật khẩu --

def derive_key(password, salt: bytes, iterations: int = PBKDF2_ITERATIONS) -> bytes:
    """Mật khẩu/passphrase -> khóa 32 byte (PBKDF2-HMAC-SHA256)."""
    if isinstance(password, str):
        password = password.encode("utf-8")
    return hashlib.pbkdf2_hmac("sha256", password, salt, iterations, KEY_SIZE)


def hash_password(password: str, iterations: int = PBKDF2_ITERATIONS) -> str:
    """Trả chuỗi lưu được: 'pbkdf2$<iter>$<salt hex>$<hash hex>'."""
    salt = os.urandom(16)
    digest = derive_key(password, salt, iterations)
    return f"pbkdf2${iterations}${salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        scheme, iters, salt_hex, hash_hex = stored.split("$")
        if scheme != "pbkdf2":
            return False
        digest = derive_key(password, bytes.fromhex(salt_hex), int(iters))
        return hmac.compare_digest(digest, bytes.fromhex(hash_hex))
    except (ValueError, TypeError):
        return False


# --------------------------------------------------- datagram độc lập (UDP) --

class CryptoBox:
    """AES-256-GCM với nonce ngẫu nhiên: blob = nonce(12) || ciphertext || tag(16).

    Dùng cho gói không có thứ tự/không có kết nối (heartbeat, discovery).
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


# ------------------------------------------------- phiên cho stream tin cậy --

def generate_keypair():
    """-> (private_key, public_bytes[32])."""
    priv = X25519PrivateKey.generate()
    pub = priv.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return priv, pub


class SecureSession:
    """Hai khóa AES-GCM (mỗi chiều một khóa) + counter làm nonce.

    Stream bên dưới đã đảm bảo đúng thứ tự, nên nonce = counter 64-bit, không
    cần gửi kèm; mất/sửa/đảo frame đều làm giải mã thất bại.
    """

    def __init__(self, private_key, peer_public: bytes, is_server: bool,
                 psk: bytes = b""):
        try:
            shared = private_key.exchange(
                X25519PublicKey.from_public_bytes(peer_public))
        except ValueError as e:
            raise SecurityError("Public key của peer không hợp lệ") from e
        salt = hashlib.sha256(b"udp-collab-v1" + psk).digest()

        def key(info):
            return HKDF(algorithm=hashes.SHA256(), length=KEY_SIZE,
                        salt=salt, info=info).derive(shared)

        c2s, s2c = key(b"client->server"), key(b"server->client")
        send_key, recv_key = (s2c, c2s) if is_server else (c2s, s2c)
        self._send = AESGCM(send_key)
        self._recv = AESGCM(recv_key)
        self._send_ctr = 0
        self._recv_ctr = 0

    @staticmethod
    def _nonce(counter: int) -> bytes:
        return b"\x00" * 4 + counter.to_bytes(8, "big")

    def seal(self, plaintext: bytes) -> bytes:
        blob = self._send.encrypt(self._nonce(self._send_ctr), plaintext, None)
        self._send_ctr += 1
        return blob

    def open(self, blob: bytes) -> bytes:
        try:
            plain = self._recv.decrypt(self._nonce(self._recv_ctr), blob, None)
        except InvalidTag as e:
            raise SecurityError("Giải mã thất bại (sai khóa hoặc dữ liệu bị sửa)") from e
        self._recv_ctr += 1
        return plain


class SecureStream:
    """Bọc một PacketService: handshake ECDH + gửi/nhận message đã mã hóa.

    send_message()/recv_message() tách message rõ ràng (length-prefix) nên tầng
    trên không phải tự xử lý việc stream cắt/ghép byte tùy ý.
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
        """Đảm bảo buffer có >= n byte (không lấy ra). False nếu EOF sạch.

        Không tiêu thụ byte nên timeout giữa chừng không làm lệch framing."""
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
        out = bytes(self._buf[:n])
        del self._buf[:n]
        return out

    def handshake(self, timeout=10):
        """Trao đổi public key (32 byte mỗi bên) rồi dựng SecureSession."""
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
        if self.session is None:
            raise SecurityError("Chưa handshake!")
        if len(plaintext) > MAX_MESSAGE_SIZE:
            raise ValueError("Message quá lớn")
        with self._send_lock:        # seal + send phải cùng thứ tự (counter)
            blob = self.session.seal(plaintext)
            self.conn.send(_LEN.pack(len(blob)) + blob)

    def recv_message(self, timeout=None):
        """Trả bytes, hoặc None nếu peer đã đóng sạch (EOF)."""
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
        return self.conn.close(timeout)