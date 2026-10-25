// ==========================================
// 1. API Giao tiếp với Python (ui_bridge.py)
// ==========================================
async function sendCommand(payload) {
    try {
        await fetch('/api/command', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
    } catch (e) {
        console.error("Lỗi gửi lệnh:", e);
    }
}

// Polling: Hỏi thăm server mỗi 500ms xem có tin nhắn mới không
setInterval(async () => {
    try {
        let res = await fetch('/api/messages');
        if (res.ok) {
            let msgs = await res.json();
            msgs.forEach(handleIncomingMessage);
        }
    } catch (e) {
        // Bỏ qua lỗi nếu máy chủ chưa sẵn sàng
    }
}, 500);


// ==========================================
// 2. Xử lý hiển thị tin nhắn lên Màn hình
// ==========================================
const chatMessages = document.getElementById('chat-messages');

function appendMessage(htmlContent, className = "") {
    const div = document.createElement('div');
    div.className = `message ${className}`;
    div.innerHTML = htmlContent;
    chatMessages.appendChild(div);
    chatMessages.scrollTop = chatMessages.scrollHeight; // Tự cuộn xuống đáy
}

function handleIncomingMessage(msg) {
    const action = msg.action;

    if (action === "SYSTEM") {
        appendMessage(msg.msg, "system");
    } 
    else if (action === "RES_GENERAL") {
        const icon = msg.status === "ok" ? "✅" : "❌";
        appendMessage(`${icon} ${msg.message}`, "system");
        
        // Nếu đăng nhập thành công thì chuyển màn hình
        if (msg.status === "ok" && msg.message.includes("Đăng nhập")) {
            document.getElementById('login-screen').classList.add('hidden');
            document.getElementById('chat-screen').classList.remove('hidden');
        }
    } 
    else if (action === "INCOMING_CHAT") {
        const prefix = msg.is_broadcast ? "Nhóm" : "Mật";
        appendMessage(`<span class="prefix">[${prefix}]</span> <b>${msg.from_user}:</b> ${msg.msg}`);
    } 
    else if (action === "INCOMING_FILE_REQ") {
        const html = `
            <b>📦 ${msg.from_user}</b> muốn gửi file '${msg.filename}' (${msg.filesize} bytes).
            <div class="file-req-box">
                <button class="btn-accept" onclick="acceptFile('${msg.from_user}', '${msg.filename}')">Đồng ý</button>
                <button class="btn-reject" onclick="rejectFile('${msg.from_user}')">Từ chối</button>
            </div>
        `;
        appendMessage(html);
    } 
    else if (action === "SIGNAL_FILE_READY") {
        // Bỏ qua (Đã có thông báo SYSTEM từ ui_bridge in ra)
    }
}


// ==========================================
// 3. Xử lý Tương tác Người dùng (Events)
// ==========================================

// --- Nút Đăng nhập ---
document.getElementById('login-btn').addEventListener('click', () => {
    const username = document.getElementById('username-input').value.trim();
    if (username) sendCommand({ cmd: "login", username: username });
});

// --- Chat Mode (Chuyển đổi Nhắn chung / Mật) ---
document.getElementById('chat-type').addEventListener('change', (e) => {
    const targetInput = document.getElementById('chat-target');
    if (e.target.value === "private") {
        targetInput.classList.remove('hidden');
    } else {
        targetInput.classList.add('hidden');
        targetInput.value = "";
    }
});

// --- Nút Gửi Chat ---
document.getElementById('send-chat-btn').addEventListener('click', () => {
    const type = document.getElementById('chat-type').value;
    const msg = document.getElementById('chat-input').value.trim();
    const target = document.getElementById('chat-target').value.trim();
    
    if (!msg) return;

    if (type === "all") {
        sendCommand({ cmd: "all", msg: msg });
        appendMessage(`<b>Tôi:</b> ${msg}`, "me");
    } else if (type === "private" && target) {
        sendCommand({ cmd: "chat", to_user: target, msg: msg });
        appendMessage(`<span class="prefix">[Mật tới ${target}]</span> <b>Tôi:</b> ${msg}`, "me");
    }
    
    document.getElementById('chat-input').value = ""; // Xóa trắng ô nhập
});

// Bắt phím Enter ở ô chat
document.getElementById('chat-input').addEventListener('keypress', (e) => {
    if (e.key === 'Enter') document.getElementById('send-chat-btn').click();
});

// --- Nút Gửi File ---
document.getElementById('send-file-btn').addEventListener('click', () => {
    const target = document.getElementById('file-target').value.trim();
    const filepath = document.getElementById('file-path').value.trim();
    if (target && filepath) {
        sendCommand({ cmd: "file", to_user: target, filepath: filepath });
    }
});

// --- Hàm gọi từ Giao diện (onclick) ---
window.acceptFile = function(sender, filename) {
    sendCommand({ cmd: "accept", to_user: sender, filename: filename });
    appendMessage(`Bạn đã đồng ý nhận file <b>${filename}</b>.`, "system");
};

window.rejectFile = function(sender) {
    sendCommand({ cmd: "reject", to_user: sender });
    appendMessage(`Bạn đã từ chối file từ <b>${sender}</b>.`, "system");
};