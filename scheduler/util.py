from datetime import datetime
import ctypes

SYS_GETID = 186
try:
    libc = ctypes.cdll.LoadLibrary('libc.so.6')
except Exception:
    libc = None


def get_pid():
    if libc:
        return libc.syscall(SYS_GETID)
    else:
        return -1


def get_now_str():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
