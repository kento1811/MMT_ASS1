import os
import subprocess
import sys


def control_windows_hostpot(turn_on=True, net_ssid=None, net_pass=None):
    """Using powershell with admin to active Mobile Hotspot in Window 10/11"""

    action = "StartTetheringAsync" if turn_on else "StopTetheringAsync"

    ps_command = f"""
    Add-Type -AssemblyName System.Runtime.WindowsRuntime
    $profile = [Windows.Networking.Connectivity.NetworkInformation, Windows.Networking.Connectivity, ContentType = WindowsRuntime]::GetInternetConnectionProfile()
    if ($profile -eq $null) {{
        Write-Error "Không tìm thấy kết nối mạng. Hãy đảm bảo máy đang có mạng (Wi-Fi/LAN) trước khi phát."
        exit 1
    }}
    $manager = [Windows.Networking.NetworkOperators.NetworkOperatorTetheringManager, Windows.Networking.NetworkOperators, ContentType = WindowsRuntime]::CreateFromConnectionProfile($profile)
    
    # Lệnh bật/tắt
    $asyncOp = $manager.{action}()
    
    # Chờ tiến trình chạy xong
    while ($asyncOp.Status -eq 0) {{ Start-Sleep -Milliseconds 100 }}
    
    # Kiểm tra trạng thái
    $state = $manager.TetheringOperationalState
    if ($state -eq 1) {{
        Write-Output "ON"
    }} else {{
        Write-Output "OFF"
    }}
    """

    try:
        result = subprocess.run(
            ["powershell", "-Command", ps_command],
            capture_output=True,
            text=True,
            check=True,
            encoding="utf-8",
            errors="replace",
        )

        output = result.stdout.strip()
        if output == "ON" and turn_on:
            print(
                "=> Đã BẬT Hotspot thành công! (SSID và Password sử dụng cấu hình mặc định trong Settings của Windows)"
            )
        elif output == "OFF" and not turn_on:
            print("=> Đã TẮT Hotspot thành công!")
        else:
            print(f"Trạng thái mạng hiện tại: {output}")

    except subprocess.CalledProcessError as e:
        print(f"Error when execute: {e.stderr.strip()}")


if __name__ == "__main__":
    command = sys.argv[1].lower() if len(sys.argv) > 1 else "start"

    if command == "stop":
        print("Đang thực hiện tắt Hotspot...")
        control_windows_hostpot(turn_on=False)
    elif command == "start":
        print("Đang thực hiện bật Hotspot...")
        control_windows_hostpot(turn_on=True)
    else:
        print("Lệnh không hợp lệ. Vui lòng dùng: python manage_hotspot.py [start|stop]")
