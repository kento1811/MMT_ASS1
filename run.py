import os
import sys
import subprocess

def run_module(module_name, *args):
    """Tiện ích gọi subprocess để chạy các module nội bộ mà không làm đứng Menu chính."""
    cmd = [sys.executable, "-m", module_name] + list(args)
    try:
        print(f"\n[Hệ thống] Đang khởi chạy: {module_name}...")
        subprocess.run(cmd)
    except KeyboardInterrupt:
        pass
    finally:
        print(f"\n[Hệ thống] Đã đóng {module_name}.")

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    # Đảm bảo Python nhận diện được thư mục gốc để import các package client, server, shared
    os.environ["PYTHONPATH"] = os.path.abspath(os.path.dirname(__file__))
    
    while True:
        clear_screen()
        print("="*60)
        print("      HUDP COLLABORATION PLATFORM - CONTROL PANEL")
        print("="*60)
        print(" 1. Khởi động Máy chủ (Server - Port UDP 9000)")
        print(" 2. Khởi động Máy khách (Client)")
        print("-" * 60)
        print(" 3. Chạy Demo: 1 Server + 3 Clients (Mở tự động)")
        print(" 4. Kịch bản: Chaos Network (Test rớt mạng, nuốt gói)")
        print(" 5. Kịch bản: P2P Stress (Test truyền nhiều file song song)")
        print(" 6. Kịch bản: Load Test (Test chịu tải 20 Bot spam)")
        print(" 7. Kịch bản: Advanced Security (Test chống MITM, Replay)")
        print("-" * 60)
        print(" 0. Thoát")
        print("="*60)
        
        choice = input("Vui lòng nhập lựa chọn (0-7): ").strip()
        
        if choice == '1':
            run_module("server.main")
            input("\nNhấn Enter để quay lại Menu chính...")
            
        elif choice == '2':
            port = input("Nhập cổng cho Web UI (Mặc định 8080, nhấn Enter để bỏ qua): ").strip()
            if not port:
                port = "8080"
            run_module("client.main", port)
            input("\nNhấn Enter để quay lại Menu chính...")
            
        elif choice == '3':
            run_module("simulations.simulate_ui")
            input("\nNhấn Enter để quay lại Menu chính...")
            
        elif choice == '4':
            run_module("simulations.chaos_network")
            input("\nNhấn Enter để quay lại Menu chính...")
            
        elif choice == '5':
            run_module("simulations.p2p_stress")
            input("\nNhấn Enter để quay lại Menu chính...")
            
        elif choice == '6':
            run_module("simulations.load_test")
            input("\nNhấn Enter để quay lại Menu chính...")
            
        elif choice == '7':
            run_module("simulations.demo_advanced_cases")
            input("\nNhấn Enter để quay lại Menu chính...")
            
        elif choice == '0':
            print("Đã thoát Control Panel. Tạm biệt!")
            break
            
        else:
            input("Lựa chọn không hợp lệ. Nhấn Enter để thử lại...")

if __name__ == "__main__":
    main()