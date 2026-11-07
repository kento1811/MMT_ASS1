import sys
import os

# Tự động thêm thư mục gốc (MMT_ASS1) vào PYTHONPATH để test có thể import 'shared'
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)