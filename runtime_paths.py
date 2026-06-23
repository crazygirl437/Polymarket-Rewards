"""
运行时路径工具

统一处理「源码运行」与「PyInstaller 打包后运行」两种情况下的基准目录：
- 源码运行时：基准目录为本文件所在目录（项目根目录）
- 打包运行时（sys.frozen）：基准目录为可执行文件所在目录

这样 .env、data/（SQLite 缓存）、logs/ 等都会落在可执行文件旁边，
而不是 PyInstaller 的临时解包目录（sys._MEIPASS），保证数据持久且多进程共享。
"""
import sys
from pathlib import Path


def is_frozen() -> bool:
    """是否运行在 PyInstaller 等打包环境中。"""
    return bool(getattr(sys, "frozen", False))


def app_base_dir() -> Path:
    """返回应用基准目录（打包后为可执行文件所在目录，否则为本文件所在目录）。"""
    if is_frozen():
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent
