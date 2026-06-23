#!/usr/bin/env python3
"""
订单簿数据服务启动脚本
支持前台和后台运行
"""
import sys
import signal
import argparse
import os
from orderbook_data_service import OrderbookDataService
from logger import setup_logger

logger = setup_logger("orderbook_service_starter")


def signal_handler(sig, frame):
    """信号处理器"""
    logger.info("收到停止信号，正在关闭服务...")
    sys.exit(0)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="订单簿数据服务")
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="后台运行（daemon 模式）"
    )
    args = parser.parse_args()
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if args.daemon:
        # 后台运行
        try:
            pid = os.fork()
            if pid > 0:
                # 父进程退出
                sys.exit(0)
        except OSError as e:
            logger.error(f"创建守护进程失败: {e}")
            sys.exit(1)
        
        # 子进程继续运行
        os.setsid()
        os.chdir("/")
        os.umask(0)
        
        # 重定向标准输入输出
        sys.stdin = open("/dev/null", "r")
        sys.stdout = open("/dev/null", "w")
        sys.stderr = open("/dev/null", "w")
        
        logger.info("订单簿数据服务以守护进程模式启动")
    else:
        logger.info("订单簿数据服务以前台模式启动")
    
    # 创建并启动服务
    service = OrderbookDataService()
    
    try:
        service.start()
        
        # 保持运行
        while True:
            import time
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("收到停止信号")
    except Exception as e:
        logger.error(f"服务运行出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        service.stop()
        logger.info("服务已关闭")


if __name__ == "__main__":
    main()

