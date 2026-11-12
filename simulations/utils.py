import os
import glob
import shutil
import logging

def clear_screen():
    """Xóa trắng màn hình Terminal"""
    os.system('cls' if os.name == 'nt' else 'clear')

def cleanup_artifacts():
    """Dọn dẹp các file .bin và thư mục downloads_ rác từ các test trước"""
    for f in glob.glob("*.bin"):
        try:
            os.remove(f)
        except OSError:
            pass
            
    for d in glob.glob("downloads_*"):
        if os.path.isdir(d):
            try:
                shutil.rmtree(d)
            except OSError:
                pass

def setup_logger(test_name, log_file="simulations_result.log"):
    """Thiết lập Logger: In ra màn hình và ghi nối vào file log"""
    logger = logging.getLogger(test_name)
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        fh = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        fh_formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
        fh.setFormatter(fh_formatter)
        
        ch = logging.StreamHandler()
        ch_formatter = logging.Formatter('%(message)s')
        ch.setFormatter(ch_formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
    return logger