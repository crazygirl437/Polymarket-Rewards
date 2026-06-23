"""
日志系统模块
"""
import logging
import os
import sys
from datetime import datetime
from pathlib import Path


# ANSI 颜色代码
class Colors:
    """ANSI 颜色代码"""
    # 基础颜色
    RESET = '\033[0m'
    BOLD = '\033[1m'
    
    # 前景色
    BLACK = '\033[30m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    WHITE = '\033[37m'
    
    # 亮色
    BRIGHT_BLACK = '\033[90m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_YELLOW = '\033[93m'
    BRIGHT_BLUE = '\033[94m'
    BRIGHT_MAGENTA = '\033[95m'
    BRIGHT_CYAN = '\033[96m'
    BRIGHT_WHITE = '\033[97m'


class ColoredFormatter(logging.Formatter):
    """带颜色的日志格式化器"""
    
    # 日志级别对应的颜色
    LEVEL_COLORS = {
        'DEBUG': Colors.BRIGHT_BLACK,      # 灰色
        'INFO': Colors.GREEN,              # 绿色
        'WARNING': Colors.YELLOW,          # 黄色
        'ERROR': Colors.RED,               # 红色
        'CRITICAL': Colors.BRIGHT_RED + Colors.BOLD,  # 亮红色加粗
    }
    
    def __init__(self, *args, use_color=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.use_color = use_color and self._is_tty()
    
    def _is_tty(self):
        """检查是否在终端中运行（支持颜色输出）"""
        return hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
    
    def format(self, record):
        """格式化日志记录，添加颜色"""
        if self.use_color:
            # 获取日志级别对应的颜色
            level_color = self.LEVEL_COLORS.get(record.levelname, Colors.RESET)
            
            # 为日志级别名称添加颜色
            record.levelname = f"{level_color}{record.levelname}{Colors.RESET}"
        
        return super().format(record)


class DailyDirectoryFileHandler(logging.FileHandler):
    """按日期目录存放日志文件的 Handler"""
    
    def __init__(self, base_dir, filename, encoding='utf-8', delay=False):
        """
        初始化按日期目录的日志处理器
        
        Args:
            base_dir: 日志基础目录
            filename: 日志文件名（不含路径）
            encoding: 文件编码
            delay: 是否延迟打开文件
        """
        # 转换为绝对路径，避免工作目录变化导致的问题
        self.base_dir = Path(base_dir).resolve()
        self.filename = filename
        self.current_date = None
        self.current_path = None
        
        # 确保基础目录存在
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        # 初始化当前日期和路径
        self._update_date_path()
        
        # 调用父类初始化，传入当前日期的完整绝对路径
        super().__init__(str(self.current_path), mode='a', encoding=encoding, delay=delay)
    
    def _update_date_path(self):
        """更新当前日期和日志文件路径"""
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 如果日期变化，更新路径
        if today != self.current_date:
            self.current_date = today
            # 创建日期目录
            date_dir = self.base_dir / self.current_date
            date_dir.mkdir(parents=True, exist_ok=True)
            # 更新日志文件路径（使用绝对路径）
            self.current_path = date_dir.resolve() / self.filename
    
    def emit(self, record):
        """发送日志记录"""
        try:
            # 检查日期是否变化
            self._update_date_path()
            
            # 获取当前路径的字符串表示（绝对路径）
            current_path_str = str(self.current_path)
            
            # 如果路径变化，需要重新打开文件
            if self.baseFilename != current_path_str:
                # 关闭旧文件
                if self.stream:
                    try:
                        self.stream.close()
                    except (OSError, IOError):
                        # 忽略关闭文件时的错误
                        pass
                    self.stream = None
                
                # 确保新目录存在（双重保险）
                try:
                    self.current_path.parent.mkdir(parents=True, exist_ok=True)
                except (OSError, IOError) as e:
                    # 如果创建目录失败，尝试输出到控制台
                    try:
                        print(f"[日志错误] 无法创建日志目录 {self.current_path.parent}: {e}", file=sys.stderr)
                    except:
                        pass
                    # 不抛出异常，让日志继续尝试写入
                
                # 更新基础文件名（使用绝对路径）
                self.baseFilename = current_path_str
                # 打开新文件
                if not self.delay:
                    try:
                        self.stream = self._open()
                    except (OSError, IOError) as e:
                        # 如果打开文件失败，尝试输出到控制台
                        try:
                            print(f"[日志错误] 无法打开日志文件 {current_path_str}: {e}", file=sys.stderr)
                        except:
                            pass
                        # 不抛出异常，让日志继续尝试写入（可能写入到控制台）
            
            # 调用父类的 emit 方法
            super().emit(record)
        except (OSError, IOError) as e:
            # 如果日志写入失败，尝试输出到控制台，但不抛出异常
            try:
                print(f"[日志错误] 写入日志失败: {e}", file=sys.stderr)
                # 尝试直接输出日志消息到 stderr
                try:
                    formatted_msg = self.format(record)
                    print(formatted_msg, file=sys.stderr)
                except:
                    pass
            except:
                # 如果连 stderr 都无法写入，静默忽略（防止程序卡死）
                pass
        except Exception as e:
            # 捕获所有其他异常，防止日志系统错误导致程序崩溃
            try:
                print(f"[日志错误] 日志系统发生未预期的错误: {e}", file=sys.stderr)
            except:
                pass


def setup_logger(
    name: str = "market_making",
    log_dir: str = "logs",
    level: int = logging.INFO,
    console: bool = True
) -> logging.Logger:
    """
    设置日志系统
    
    Args:
        name: 日志名称
        log_dir: 日志目录
        level: 日志级别
        console: 是否输出到控制台
        
    Returns:
        配置好的 Logger 对象
    """
    # 创建日志目录
    # 相对路径时，解析到应用基准目录（打包后为可执行文件所在目录），
    # 避免日志写入 PyInstaller 临时解包目录或随 cwd 漂移
    log_path = Path(log_dir)
    if not log_path.is_absolute():
        try:
            from runtime_paths import app_base_dir
            log_path = app_base_dir() / log_path
        except Exception:
            pass
    log_path.mkdir(parents=True, exist_ok=True)
    
    # 创建 logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 避免重复添加 handler
    if logger.handlers:
        return logger
    
    # 文件日志格式（不带颜色）
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 文件 handler（按日期目录存放）
    # 日志文件将存放在 logs/YYYY-MM-DD/name.log 格式的目录中
    log_filename = f"{name}.log"
    file_handler = DailyDirectoryFileHandler(
        base_dir=log_path,
        filename=log_filename,
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # 控制台 handler（带颜色）
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        # 使用带颜色的格式化器
        console_formatter = ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            use_color=True
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    return logger


# 创建默认 logger 实例
default_logger = setup_logger()
