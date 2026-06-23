#!/usr/bin/env python3
"""
订单簿数据客户端（SQLite 后端）

为了避免依赖额外的系统组件（Redis 服务），本模块使用 Python 标准库自带的
SQLite 作为底层 KV 存储。SQLite 是单文件数据库、跨平台（Windows/Linux/Mac）、
支持多进程并发访问（通过文件锁 + WAL 模式），因此可以在「订单簿数据服务」
进程与「主程序」进程之间共享数据，且无需启动任何额外的服务进程。

本类保持与原 Redis 版本完全一致的类名、构造函数参数与公开方法签名，
因此调用方代码无需任何改动。host/port/password 等参数仅为兼容保留，实际不使用。
"""
import os
import json
import time
import sqlite3
import threading
from typing import Dict, List, Optional, Any, Set

from logger import setup_logger
from runtime_paths import app_base_dir

logger = setup_logger("redis_orderbook_client")


def _default_db_path() -> str:
    """计算默认的 SQLite 数据库文件路径（可执行文件/项目目录下的 data/ 子目录）。

    打包成二进制后，基准目录为可执行文件所在目录，避免数据写入临时解包目录。
    """
    env_path = os.getenv("ORDERBOOK_DB_PATH")
    if env_path:
        return env_path
    data_dir = app_base_dir() / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return str(data_dir / "orderbook_cache.sqlite3")


class RedisOrderbookClient:
    """
    订单簿数据客户端（SQLite 实现）

    提供订单簿数据的存储和读取接口，支持批量操作。
    接口与原 Redis 版本保持一致，可作为其无缝替换。
    """

    # SQLite IN 查询单批最大参数数量（避免触发 SQLITE_MAX_VARIABLE_NUMBER 限制）
    _MAX_VARS = 500

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        orderbook_ttl: int = 60,
        db_path: Optional[str] = None,
    ):
        """
        初始化客户端

        Args:
            host: 兼容保留，未使用
            port: 兼容保留，未使用
            db: 兼容保留，未使用
            password: 兼容保留，未使用
            orderbook_ttl: 订单簿数据过期时间（秒）
            db_path: SQLite 数据库文件路径（默认使用模块同目录下的 data/orderbook_cache.sqlite3）
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.orderbook_ttl = orderbook_ttl

        # Key 前缀（与原 Redis 版本保持一致，保证业务逻辑不变）
        self.key_prefix = "orderbook:"
        self.market_tokens_prefix = "market_tokens:"
        self.markets_list_key = "markets:list"
        self.market_prefix = "market:"
        self.market_detail_prefix = "market_detail:"

        self.db_path = db_path or _default_db_path()
        # 同一连接在多线程间复用时需要加锁串行化
        self._lock = threading.Lock()

        try:
            self._conn = sqlite3.connect(
                self.db_path,
                timeout=30.0,
                check_same_thread=False,
            )
            self._init_db()
            logger.info(f"SQLite 存储连接成功: {self.db_path}")
        except Exception as e:
            logger.error(f"SQLite 连接失败: {e}")
            raise

    # ------------------------------------------------------------------
    # 底层通用 KV 操作
    # ------------------------------------------------------------------
    def _init_db(self) -> None:
        """初始化数据库结构与 PRAGMA 设置。"""
        with self._lock:
            # WAL 模式提升并发读写能力；busy_timeout 在锁竞争时等待而非立即失败
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA busy_timeout=30000")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    expire_at REAL
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_kv_expire_at ON kv(expire_at)"
            )
            self._conn.commit()

    @staticmethod
    def _now() -> float:
        return time.time()

    def _set(self, key: str, value: str, ttl: Optional[int]) -> None:
        expire_at = (self._now() + ttl) if ttl else None
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO kv (key, value, expire_at) VALUES (?, ?, ?)",
                (key, value, expire_at),
            )
            self._conn.commit()

    def _set_many(self, items: List[tuple], ttl: Optional[int]) -> int:
        """批量写入。items 为 [(key, value), ...]。"""
        expire_at = (self._now() + ttl) if ttl else None
        rows = [(k, v, expire_at) for (k, v) in items]
        with self._lock:
            self._conn.executemany(
                "INSERT OR REPLACE INTO kv (key, value, expire_at) VALUES (?, ?, ?)",
                rows,
            )
            self._conn.commit()
        return len(rows)

    def _get(self, key: str) -> Optional[str]:
        now = self._now()
        with self._lock:
            cur = self._conn.execute(
                "SELECT value, expire_at FROM kv WHERE key = ?", (key,)
            )
            row = cur.fetchone()
        if not row:
            return None
        value, expire_at = row
        if expire_at is not None and expire_at < now:
            # 已过期，惰性删除
            self._delete([key])
            return None
        return value

    def _mget(self, keys: List[str]) -> Dict[str, str]:
        """批量读取，返回 {key: value}（仅包含存在且未过期的键）。"""
        if not keys:
            return {}
        now = self._now()
        result: Dict[str, str] = {}
        expired: List[str] = []
        for i in range(0, len(keys), self._MAX_VARS):
            chunk = keys[i:i + self._MAX_VARS]
            placeholders = ",".join("?" * len(chunk))
            with self._lock:
                cur = self._conn.execute(
                    f"SELECT key, value, expire_at FROM kv WHERE key IN ({placeholders})",
                    chunk,
                )
                rows = cur.fetchall()
            for key, value, expire_at in rows:
                if expire_at is not None and expire_at < now:
                    expired.append(key)
                    continue
                result[key] = value
        if expired:
            self._delete(expired)
        return result

    def _delete(self, keys: List[str]) -> int:
        if not keys:
            return 0
        deleted = 0
        with self._lock:
            for i in range(0, len(keys), self._MAX_VARS):
                chunk = keys[i:i + self._MAX_VARS]
                placeholders = ",".join("?" * len(chunk))
                cur = self._conn.execute(
                    f"DELETE FROM kv WHERE key IN ({placeholders})", chunk
                )
                deleted += cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
            self._conn.commit()
        return deleted

    def _keys_with_prefix(self, prefix: str) -> List[str]:
        """返回所有以 prefix 开头且未过期的键。"""
        now = self._now()
        # LIKE 中转义 % 和 _，避免前缀含特殊字符（本项目前缀均为字母与冒号，安全起见仍处理）
        like = prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        with self._lock:
            cur = self._conn.execute(
                "SELECT key, expire_at FROM kv WHERE key LIKE ? ESCAPE '\\'", (like,)
            )
            rows = cur.fetchall()
        keys = []
        for key, expire_at in rows:
            if expire_at is not None and expire_at < now:
                continue
            keys.append(key)
        return keys

    def _purge_expired(self) -> None:
        """删除所有已过期的键（机会性清理，失败不影响主流程）。"""
        try:
            now = self._now()
            with self._lock:
                self._conn.execute(
                    "DELETE FROM kv WHERE expire_at IS NOT NULL AND expire_at < ?",
                    (now,),
                )
                self._conn.commit()
        except Exception as e:
            logger.debug(f"清理过期数据失败: {e}")

    def _get_orderbook_key(self, token_id: str) -> str:
        """获取订单簿 Key"""
        return f"{self.key_prefix}{token_id}"

    def _get_market_tokens_key(self, market_id: str) -> str:
        """获取市场 token 列表 Key"""
        return f"{self.market_tokens_prefix}{market_id}"

    # ------------------------------------------------------------------
    # 订单簿
    # ------------------------------------------------------------------
    def set_orderbook(self, token_id: str, orderbook: Dict[str, Any]) -> bool:
        """存储订单簿数据"""
        try:
            self._set(self._get_orderbook_key(token_id), json.dumps(orderbook), self.orderbook_ttl)
            return True
        except Exception as e:
            logger.error(f"存储订单簿失败 token_id={token_id[:20]}...: {e}")
            return False

    def get_orderbook(self, token_id: str) -> Optional[Dict[str, Any]]:
        """获取订单簿数据"""
        try:
            value = self._get(self._get_orderbook_key(token_id))
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"获取订单簿失败 token_id={token_id[:20]}...: {e}")
            return None

    def set_orderbooks_batch(self, orderbooks: Dict[str, Dict[str, Any]]) -> int:
        """批量存储订单簿数据"""
        if not orderbooks:
            return 0
        try:
            items = [
                (self._get_orderbook_key(token_id), json.dumps(orderbook))
                for token_id, orderbook in orderbooks.items()
            ]
            count = self._set_many(items, self.orderbook_ttl)
            # 借批量写入的时机顺带清理过期数据
            self._purge_expired()
            return count
        except Exception as e:
            logger.error(f"批量存储订单簿失败: {e}")
            return 0

    def get_orderbooks_batch(self, token_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """批量获取订单簿数据"""
        if not token_ids:
            return {}
        result: Dict[str, Dict[str, Any]] = {}
        try:
            keys = [self._get_orderbook_key(token_id) for token_id in token_ids]
            values = self._mget(keys)
            for token_id in token_ids:
                value = values.get(self._get_orderbook_key(token_id))
                if value:
                    try:
                        result[token_id] = json.loads(value)
                    except json.JSONDecodeError:
                        logger.warning(f"订单簿数据 JSON 解析失败: token_id={token_id[:20]}...")
        except Exception as e:
            logger.error(f"批量获取订单簿失败: {e}")
        return result

    def delete_orderbook(self, token_id: str) -> bool:
        """删除订单簿数据"""
        try:
            self._delete([self._get_orderbook_key(token_id)])
            return True
        except Exception as e:
            logger.error(f"删除订单簿失败 token_id={token_id[:20]}...: {e}")
            return False

    def delete_orderbooks_batch(self, token_ids: List[str]) -> int:
        """批量删除订单簿数据"""
        if not token_ids:
            return 0
        try:
            keys = [self._get_orderbook_key(token_id) for token_id in token_ids]
            return self._delete(keys)
        except Exception as e:
            logger.error(f"批量删除订单簿失败: {e}")
            return 0

    def get_all_token_ids(self) -> Set[str]:
        """获取所有存储的 token_id"""
        try:
            keys = self._keys_with_prefix(self.key_prefix)
            return {key[len(self.key_prefix):] for key in keys}
        except Exception as e:
            logger.error(f"获取所有 token_id 失败: {e}")
            return set()

    # ------------------------------------------------------------------
    # 市场 token 列表
    # ------------------------------------------------------------------
    def set_market_tokens(self, market_id: str, token_ids: List[str]) -> bool:
        """存储市场的 token 列表"""
        try:
            self._set(
                self._get_market_tokens_key(market_id),
                json.dumps(token_ids),
                self.orderbook_ttl * 2,
            )
            return True
        except Exception as e:
            logger.error(f"存储市场 token 列表失败 market_id={market_id[:20]}...: {e}")
            return False

    def get_market_tokens(self, market_id: str) -> List[str]:
        """获取市场的 token 列表"""
        try:
            value = self._get(self._get_market_tokens_key(market_id))
            if value:
                return json.loads(value)
            return []
        except Exception as e:
            logger.error(f"获取市场 token 列表失败 market_id={market_id[:20]}...: {e}")
            return []

    def delete_market_tokens(self, market_id: str) -> bool:
        """删除市场的 token 列表"""
        try:
            self._delete([self._get_market_tokens_key(market_id)])
            return True
        except Exception as e:
            logger.error(f"删除市场 token 列表失败 market_id={market_id[:20]}...: {e}")
            return False

    def get_all_market_ids(self) -> Set[str]:
        """获取所有存储的市场 ID"""
        try:
            keys = self._keys_with_prefix(self.market_tokens_prefix)
            return {key[len(self.market_tokens_prefix):] for key in keys}
        except Exception as e:
            logger.error(f"获取所有市场 ID 失败: {e}")
            return set()

    def ping(self) -> bool:
        """检查存储连接是否正常"""
        try:
            with self._lock:
                self._conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # 市场列表 / 单个市场
    # ------------------------------------------------------------------
    def set_markets(self, markets: List[Dict[str, Any]], ttl: Optional[int] = None) -> bool:
        """存储市场数据列表"""
        try:
            if ttl is None:
                ttl = self.orderbook_ttl * 30

            value = json.dumps(markets)
            data_size_mb = len(value.encode("utf-8")) / (1024 * 1024)
            logger.info(
                f"准备存储市场数据列表到 {self.markets_list_key}，包含 {len(markets)} 个市场，大小: {data_size_mb:.2f} MB"
            )

            self._set(self.markets_list_key, value, ttl)

            verify_value = self._get(self.markets_list_key)
            if verify_value:
                verify_markets = json.loads(verify_value)
                logger.info(
                    f"成功存储并验证市场数据列表到 {self.markets_list_key}，包含 {len(verify_markets)} 个市场，TTL: {ttl} 秒"
                )
                return True
            else:
                logger.error(f"存储验证失败: {self.markets_list_key} 存储后立即查询为空")
                return False
        except Exception as e:
            logger.error(f"存储市场数据列表失败: {e}")
            return False

    def get_markets(self) -> List[Dict[str, Any]]:
        """获取所有市场数据"""
        try:
            value = self._get(self.markets_list_key)
            if value:
                return json.loads(value)
            return []
        except Exception as e:
            logger.error(f"获取市场数据列表失败: {e}")
            return []

    def get_market(self, market_id: str) -> Optional[Dict[str, Any]]:
        """获取单个市场数据"""
        try:
            value = self._get(f"{self.market_prefix}{market_id}")
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"获取市场数据失败 market_id={market_id[:20]}...: {e}")
            return None

    def set_markets_indexed(self, markets: List[Dict[str, Any]], ttl: Optional[int] = None) -> int:
        """存储市场数据列表，同时为每个市场创建索引（用于快速查找）"""
        try:
            if ttl is None:
                ttl = self.orderbook_ttl * 30

            list_stored = self.set_markets(markets, ttl)
            if not list_stored:
                logger.error("存储市场数据列表（markets:list）失败，但仍继续创建索引")

            items = []
            for market in markets:
                market_id = market.get("market_id")
                if market_id:
                    items.append((f"{self.market_prefix}{market_id}", json.dumps(market)))

            if items:
                self._set_many(items, ttl)
            self._purge_expired()
            return len(items)
        except Exception as e:
            logger.error(f"存储市场数据索引失败: {e}")
            return 0

    # ------------------------------------------------------------------
    # 完整市场详情
    # ------------------------------------------------------------------
    def _get_market_detail_key(self, market_id: str) -> str:
        """获取完整市场详情的 Key"""
        return f"{self.market_detail_prefix}{market_id}"

    def set_market_detail(self, market_id: str, market_detail: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """存储完整市场详情"""
        try:
            if ttl is None:
                ttl = 604800  # 7 天
            self._set(self._get_market_detail_key(market_id), json.dumps(market_detail), ttl)
            return True
        except Exception as e:
            logger.error(f"存储完整市场详情失败 market_id={market_id}: {e}")
            return False

    def get_market_detail(self, market_id: str) -> Optional[Dict[str, Any]]:
        """获取完整市场详情"""
        try:
            value = self._get(self._get_market_detail_key(market_id))
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"获取完整市场详情失败 market_id={market_id}: {e}")
            return None

    def set_markets_detail_batch(self, markets_detail: Dict[str, Dict[str, Any]], ttl: Optional[int] = None) -> int:
        """批量存储完整市场详情"""
        if not markets_detail:
            return 0
        if ttl is None:
            ttl = 604800  # 7 天
        try:
            items = [
                (self._get_market_detail_key(market_id), json.dumps(market_detail))
                for market_id, market_detail in markets_detail.items()
            ]
            return self._set_many(items, ttl)
        except Exception as e:
            logger.error(f"批量存储完整市场详情失败: {e}")
            return 0

    def get_markets_detail_batch(self, market_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """批量获取完整市场详情"""
        if not market_ids:
            return {}
        result: Dict[str, Dict[str, Any]] = {}
        try:
            keys = [self._get_market_detail_key(market_id) for market_id in market_ids]
            values = self._mget(keys)
            for market_id in market_ids:
                value = values.get(self._get_market_detail_key(market_id))
                if value:
                    try:
                        result[market_id] = json.loads(value)
                    except json.JSONDecodeError:
                        logger.warning(f"完整市场详情数据 JSON 解析失败: market_id={market_id}")
        except Exception as e:
            logger.error(f"批量获取完整市场详情失败: {e}")
        return result

    def delete_market_detail(self, market_id: str) -> bool:
        """删除完整市场详情"""
        try:
            self._delete([self._get_market_detail_key(market_id)])
            return True
        except Exception as e:
            logger.error(f"删除完整市场详情失败 market_id={market_id}: {e}")
            return False

    def delete_markets_detail_batch(self, market_ids: List[str]) -> int:
        """批量删除完整市场详情"""
        if not market_ids:
            return 0
        try:
            keys = [self._get_market_detail_key(market_id) for market_id in market_ids]
            return self._delete(keys)
        except Exception as e:
            logger.error(f"批量删除完整市场详情失败: {e}")
            return 0

    def close(self):
        """关闭存储连接"""
        try:
            with self._lock:
                self._conn.close()
        except Exception:
            pass
