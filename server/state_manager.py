import threading


class StateManager:
    """Quản lý trạng thái của các user.

    Thư viện mapping cho username <-> Port, ID
    Bảo vệ bằng lock khi change dict.

    Methods:
        login:                  Check and Add user in system
        logout_using_username:  Disconnectted username out system return address
        logout_using_address:   Disconnectted address out system return username
        get_username:           Return username with address
        get_address:            Return address with username
        get_all_users_online:   Return all username is online(or connected in system)
        is_online:              Return the client is connect or disconnect.
    """

    def __init__(self):
        self.users:  dict[str, tuple] = {}       # username -> address
        self.addresses: dict[tuple, str] = {}    # address -> username
        self.lock = threading.Lock()


    # Nhóm method tạo, xóa
    def login(self,username: str,address: tuple) -> bool:
        """ Thêm user vào hệ thống """
        with self.lock:
            # Check duplicated username
            if username in self.users:
                if self.users[username] == address:
                    return True
                return False

            # Check address is bind to other user -> Disconnected that
            if address in self.addresses:
                old_user = self.addresses[address]
                self.users.pop(old_user, None)

            # Add user to system (connected)
            self.users[username] = address
            self.addresses[address] = username
            return True

    def logout_by_username(self, username: str) -> tuple:
        """Xóa address theo username và return address tương ứng nếu có"""
        with self.lock:
            address = self.users.pop(username, None)
            if address is not None:
                self.addresses.pop(address, None)
                return address
            return None

    def logout_by_address(self, address: tuple) -> str:
        """Xóa username theo address và return username tương ứng nếu có"""
        with self.lock:
            username = self.addresses.pop(address, None)
            if username is not None:
                self.users.pop(username, None)
                return username
            return None


    # Nhóm method quản lý trạng thái
    def get_address(self, username: str) -> tuple:
        """Lấy address theo username"""
        with self.lock:
            return self.users.get(username)

    def get_username(self, address: tuple) -> str:
        """Lấy username theo address"""
        with self.lock:
            return self.addresses.get(address)

    def get_all_users_online(self) -> list:
        """Danh sách username đang online"""
        with self.lock:
            return list(self.users.keys())

    def is_online(self, username: str) -> bool:
        "Kiểm tra user có đang online không"
        with self.lock:
            return username in self.users