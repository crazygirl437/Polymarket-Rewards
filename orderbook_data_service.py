#!/usr/bin/env python3
"""
订单簿数据服务
定期扫描流动性奖励市场，分批获取订单簿数据并存入 Redis
"""
import time
import threading
from typing import Dict, List, Set, Any, Optional
from http_orderbook_client import HTTPOrderbookClient
from redis_orderbook_client import RedisOrderbookClient
from api_client import PolymarketAPIClient
from config import config
from logger import setup_logger

logger = setup_logger("orderbook_data_service")


class OrderbookDataService:
    """
    订单簿数据服务
    
    定期扫描市场，获取订单簿数据并存入 Redis
    """
    
    def __init__(self):
        """初始化服务"""
        # 获取配置
        service_config = config.orderbook_service
        storage_config = service_config.get("storage", {})
        
        # 初始化客户端
        self.api_client = PolymarketAPIClient()
        self.http_client = HTTPOrderbookClient()
        self.redis_client = RedisOrderbookClient(
            orderbook_ttl=storage_config.get("orderbook_ttl", 300),
            db_path=storage_config.get("db_path"),
        )
        
        # 配置参数
        self.market_scan_interval = service_config.get("market_scan_interval", 300)
        self.orderbook_update_interval = service_config.get("orderbook_update_interval", 30)
        self.batch_size = service_config.get("batch_size", 100)
        self.market_detail_ttl = service_config.get("market_detail_ttl", 604800)  # 默认 7 天
        self.market_detail_batch_size = service_config.get("market_detail_batch_size", 50)  # 批量获取市场详情的批次大小
        self.market_detail_fill_per_scan = service_config.get("market_detail_fill_per_scan", 100)  # 每次扫描最多补全的市场详情数量
        
        # 运行状态
        self._should_stop = False
        self._scan_thread: Optional[threading.Thread] = None
        self._update_thread: Optional[threading.Thread] = None
        
        # 当前市场列表（用于增量更新）
        self.current_markets: Dict[str, Dict[str, Any]] = {}
        self.current_token_ids: Set[str] = set()
    
    def start(self):
        """启动服务"""
        if self._scan_thread and self._scan_thread.is_alive():
            logger.warning("服务已经在运行")
            return
        
        self._should_stop = False
        
        # 启动市场扫描线程
        self._scan_thread = threading.Thread(target=self._market_scan_loop, daemon=True)
        self._scan_thread.start()
        logger.info("市场扫描线程已启动")
        
        # 启动订单簿更新线程
        self._update_thread = threading.Thread(target=self._orderbook_update_loop, daemon=True)
        self._update_thread.start()
        logger.info("订单簿更新线程已启动")
        
        logger.info("订单簿数据服务已启动")
    
    def stop(self):
        """停止服务"""
        logger.info("正在停止订单簿数据服务...")
        self._should_stop = True
        
        # 等待线程结束
        if self._scan_thread and self._scan_thread.is_alive():
            self._scan_thread.join(timeout=5.0)
        
        if self._update_thread and self._update_thread.is_alive():
            self._update_thread.join(timeout=5.0)
        
        # 关闭客户端连接
        self.http_client.close()
        self.redis_client.close()
        
        logger.info("订单簿数据服务已停止")
    
    def _market_scan_loop(self):
        """市场扫描循环"""
        # 启动时立即扫描一次
        self._scan_markets()
        
        # 定期扫描
        while not self._should_stop:
            try:
                time.sleep(self.market_scan_interval)
                if not self._should_stop:
                    self._scan_markets()
            except Exception as e:
                logger.error(f"市场扫描循环出错: {e}")
                time.sleep(60)  # 出错后等待1分钟再继续
    
    def _scan_markets(self):
        """扫描市场并更新订单簿数据"""
        logger.info("开始扫描流动性奖励市场...")
        
        try:
            # 获取所有流动性奖励市场（强制从 API 获取最新数据，不使用缓存）
            # 因为订单簿服务本身负责更新缓存，所以应该总是从 API 获取最新数据
            all_markets = self.api_client.get_all_rewards_markets(use_cache=False)
            logger.info(f"获取到 {len(all_markets)} 个流动性奖励市场")
            
            # 构建市场字典和 token 集合
            new_markets: Dict[str, Dict[str, Any]] = {}
            new_token_ids: Set[str] = set()
            market_tokens_map: Dict[str, List[str]] = {}
            
            for market in all_markets:
                market_id = market.get("market_id")
                if not market_id:
                    continue
                
                tokens = market.get("tokens", [])
                token_ids = []
                for token in tokens:
                    token_id = token.get("token_id")
                    if token_id:
                        token_ids.append(token_id)
                        new_token_ids.add(token_id)
                
                if token_ids:
                    new_markets[market_id] = market
                    market_tokens_map[market_id] = token_ids
            
            logger.info(f"提取到 {len(new_token_ids)} 个 token_id，涉及 {len(new_markets)} 个市场")
            
            # 找出需要删除的市场（不在新列表中的）
            old_market_ids = set(self.current_markets.keys())
            new_market_ids = set(new_markets.keys())
            markets_to_remove = old_market_ids - new_market_ids
            
            if markets_to_remove:
                logger.info(f"发现 {len(markets_to_remove)} 个市场已不在列表中，将删除相关数据")
                self._remove_markets(markets_to_remove)
                # 删除完整市场详情缓存
                deleted_detail_count = self.redis_client.delete_markets_detail_batch(list(markets_to_remove))
                logger.info(f"已删除 {deleted_detail_count} 个市场的完整详情缓存")
            
            # 找出新增的市场（需要获取完整详情的）
            markets_to_fetch_detail = new_market_ids - old_market_ids
            
            # 找出新增的 token（需要获取订单簿的）
            new_tokens = new_token_ids - self.current_token_ids
            if new_tokens:
                logger.info(f"发现 {len(new_tokens)} 个新 token，将获取订单簿数据")
            
            # 检查 Redis 中已过期的 token（从当前市场列表中查找）
            # 这些 token 可能在更新过程中过期了，需要重新获取
            expired_tokens = set()
            current_redis_token_ids = self.redis_client.get_all_token_ids()
            for token_id in new_token_ids:
                if token_id not in current_redis_token_ids:
                    # 这个 token 应该在 Redis 中，但不存在，说明可能过期了
                    expired_tokens.add(token_id)
            
            if expired_tokens:
                logger.info(f"发现 {len(expired_tokens)} 个 token 的数据已过期，将重新获取")
            
            # 分批获取订单簿数据（包括新增和已过期的 token）
            all_token_ids = list(new_token_ids)
            total_fetched = 0
            
            for i in range(0, len(all_token_ids), self.batch_size):
                batch = all_token_ids[i:i + self.batch_size]
                logger.info(f"获取订单簿批次 {i // self.batch_size + 1}/{(len(all_token_ids) + self.batch_size - 1) // self.batch_size}: {len(batch)} 个 token")
                
                orderbooks = self.http_client.get_orderbooks(batch)
                
                if orderbooks:
                    # 转换为字典格式，key 为 asset_id
                    orderbooks_dict = {}
                    for orderbook in orderbooks:
                        asset_id = orderbook.get("asset_id")
                        if asset_id:
                            orderbooks_dict[asset_id] = orderbook
                    
                    # 批量存入 Redis
                    saved_count = self.redis_client.set_orderbooks_batch(orderbooks_dict)
                    total_fetched += saved_count
                    logger.info(f"批次保存成功: {saved_count}/{len(batch)} 个订单簿")
                else:
                    logger.warning(f"批次获取失败: {len(batch)} 个 token")
            
            # 存储市场 token 映射
            for market_id, token_ids in market_tokens_map.items():
                self.redis_client.set_market_tokens(market_id, token_ids)
            
            # 存储市场数据到 Redis（使用索引方式，方便快速查找）
            markets_list = list(new_markets.values())
            markets_ttl = self.market_scan_interval + 60  # TTL 比扫描间隔稍长，避免数据过期
            
            if not markets_list:
                logger.warning("市场数据列表为空，跳过存储")
            else:
                indexed_count = self.redis_client.set_markets_indexed(markets_list, ttl=markets_ttl)
                logger.info(f"已存储 {indexed_count} 个市场数据索引到 Redis（TTL: {markets_ttl} 秒）")
                
                # 验证 markets:list 是否存储成功
                stored_markets = self.redis_client.get_markets()
                if stored_markets:
                    logger.info(f"验证成功: markets:list 包含 {len(stored_markets)} 个市场")
                else:
                    logger.error("验证失败: markets:list 为空，可能存储失败或数据过大")
            
            # 处理新增市场的完整详情获取
            if markets_to_fetch_detail:
                logger.info(f"发现 {len(markets_to_fetch_detail)} 个新增市场，将获取完整详情")
                self._fetch_markets_detail(list(markets_to_fetch_detail))
            
            # 检查并补全缺失的完整市场详情（包括已存在的市场）
            # 对于已存在的市场，如果缺失完整详情，也会进行补全
            # 为了避免一次性补全太多市场（可能有上千个），每次扫描只补全一部分
            all_market_ids_to_check = list(new_market_ids)
            if all_market_ids_to_check:
                # 检查哪些市场缺失完整详情（分批检查，避免一次性查询太多）
                markets_without_detail = []
                check_batch_size = 100  # 每次检查100个市场
                for i in range(0, len(all_market_ids_to_check), check_batch_size):
                    batch = all_market_ids_to_check[i:i + check_batch_size]
                    existing_details = self.redis_client.get_markets_detail_batch(batch)
                    markets_without_detail.extend([mid for mid in batch if mid not in existing_details])
                
                if markets_without_detail:
                    # 排除新增市场（已经在上一步处理过了）
                    markets_to_fill = [mid for mid in markets_without_detail if mid not in markets_to_fetch_detail]
                    if markets_to_fill:
                        # 限制每次扫描最多补全的数量，避免一次性请求过多
                        markets_to_fill_this_scan = markets_to_fill[:self.market_detail_fill_per_scan]
                        logger.info(
                            f"发现 {len(markets_to_fill)} 个已存在市场缺失完整详情，"
                            f"本次扫描将补全 {len(markets_to_fill_this_scan)} 个"
                            f"（剩余 {len(markets_to_fill) - len(markets_to_fill_this_scan)} 个将在后续扫描中补全）"
                        )
                        self._fetch_markets_detail(markets_to_fill_this_scan)
            
            # 更新当前状态
            self.current_markets = new_markets
            self.current_token_ids = new_token_ids
            
            logger.info(f"市场扫描完成: 当前 {len(new_markets)} 个市场，{len(new_token_ids)} 个 token，成功获取 {total_fetched} 个订单簿")
            
        except Exception as e:
            logger.error(f"扫描市场失败: {e}")
            import traceback
            traceback.print_exc()
    
    def _fetch_markets_detail(self, market_ids: List[str]):
        """
        获取新增市场的完整详情
        
        Args:
            market_ids: 市场 ID 列表
        """
        if not market_ids:
            return
        
        try:
            # 检查 Redis 中已存在的完整详情，避免重复获取
            existing_details = self.redis_client.get_markets_detail_batch(market_ids)
            existing_market_ids = set(existing_details.keys())
            markets_to_fetch = [mid for mid in market_ids if mid not in existing_market_ids]
            
            if existing_market_ids:
                logger.info(f"Redis 中已有 {len(existing_market_ids)} 个市场的完整详情，跳过获取")
            
            if not markets_to_fetch:
                logger.info("所有新增市场都已存在完整详情，无需获取")
                return
            
            logger.info(f"需要获取 {len(markets_to_fetch)} 个市场的完整详情")
            
            # 分批获取市场详情
            total_fetched = 0
            markets_detail_dict = {}
            
            for i in range(0, len(markets_to_fetch), self.market_detail_batch_size):
                batch = markets_to_fetch[i:i + self.market_detail_batch_size]
                logger.info(f"获取完整市场详情批次 {i // self.market_detail_batch_size + 1}/{(len(markets_to_fetch) + self.market_detail_batch_size - 1) // self.market_detail_batch_size}: {len(batch)} 个市场")
                
                try:
                    # 调用 API 获取完整市场详情
                    market_details = self.api_client.get_markets_detail(batch)
                    
                    if market_details:
                        # 构建市场详情字典，key 为 market_id（注意：API 返回的字段可能是 "id" 而不是 "market_id"）
                        for detail in market_details:
                            # API 返回的 market_id 可能在 "id" 字段中
                            market_id = detail.get("id") or detail.get("market_id")
                            if market_id:
                                markets_detail_dict[market_id] = detail
                                total_fetched += 1
                        
                        logger.info(f"批次获取成功: {len(market_details)} 个市场详情")
                    else:
                        logger.warning(f"批次获取失败: {len(batch)} 个市场")
                        
                except Exception as e:
                    logger.error(f"获取市场详情批次失败: {e}")
                    continue
                
                # 避免请求过快
                time.sleep(0.2)
            
            # 批量存储到 Redis
            if markets_detail_dict:
                saved_count = self.redis_client.set_markets_detail_batch(markets_detail_dict, ttl=self.market_detail_ttl)
                logger.info(f"已存储 {saved_count} 个完整市场详情到 Redis（TTL: {self.market_detail_ttl} 秒）")
            
        except Exception as e:
            logger.error(f"获取市场详情失败: {e}")
            import traceback
            traceback.print_exc()
    
    def _remove_markets(self, market_ids: Set[str]):
        """删除指定市场的数据"""
        try:
            token_ids_to_remove = set()
            
            # 收集要删除的 token_id
            for market_id in market_ids:
                token_ids = self.redis_client.get_market_tokens(market_id)
                token_ids_to_remove.update(token_ids)
                # 删除市场 token 映射
                self.redis_client.delete_market_tokens(market_id)
            
            if token_ids_to_remove:
                # 批量删除订单簿数据
                deleted_count = self.redis_client.delete_orderbooks_batch(list(token_ids_to_remove))
                logger.info(f"删除市场数据: {len(market_ids)} 个市场，{deleted_count} 个订单簿")
                
        except Exception as e:
            logger.error(f"删除市场数据失败: {e}")
    
    def _orderbook_update_loop(self):
        """订单簿更新循环"""
        # 等待一段时间，让市场扫描先完成
        time.sleep(10)
        
        while not self._should_stop:
            try:
                time.sleep(self.orderbook_update_interval)
                if not self._should_stop:
                    self._update_orderbooks()
            except Exception as e:
                logger.error(f"订单簿更新循环出错: {e}")
                time.sleep(60)  # 出错后等待1分钟再继续
    
    def _update_orderbooks(self):
        """更新订单簿数据"""
        try:
            # 从 Redis 获取所有 token_id
            token_ids = self.redis_client.get_all_token_ids()
            
            if not token_ids:
                logger.debug("没有需要更新的订单簿数据")
                return
            
            token_ids_list = list(token_ids)
            logger.info(f"开始更新 {len(token_ids_list)} 个订单簿数据...")
            
            total_updated = 0
            total_missing = 0
            total_expired = 0
            
            # 分批更新
            for i in range(0, len(token_ids_list), self.batch_size):
                batch = token_ids_list[i:i + self.batch_size]
                logger.debug(f"更新订单簿批次 {i // self.batch_size + 1}/{(len(token_ids_list) + self.batch_size - 1) // self.batch_size}: {len(batch)} 个 token")
                
                orderbooks = self.http_client.get_orderbooks(batch)
                
                if orderbooks:
                    # 转换为字典格式，key 为 asset_id
                    orderbooks_dict = {}
                    returned_asset_ids = set()
                    for orderbook in orderbooks:
                        asset_id = orderbook.get("asset_id")
                        if asset_id:
                            orderbooks_dict[asset_id] = orderbook
                            returned_asset_ids.add(asset_id)
                    
                    # 批量更新 Redis
                    updated_count = self.redis_client.set_orderbooks_batch(orderbooks_dict)
                    total_updated += updated_count
                    
                    # 对于 API 没有返回数据的 token，延长其 TTL（保留旧数据）
                    missing_token_ids = set(batch) - returned_asset_ids
                    if missing_token_ids:
                        total_missing += len(missing_token_ids)
                        # 延长这些 token 的 TTL，避免因为未更新而过期
                        for token_id in missing_token_ids:
                            # 获取旧数据并重新设置（延长 TTL）
                            old_orderbook = self.redis_client.get_orderbook(token_id)
                            if old_orderbook:
                                # 重新设置，延长 TTL
                                self.redis_client.set_orderbook(token_id, old_orderbook)
                            else:
                                # 旧数据已过期，无法延长
                                total_expired += 1
                                logger.debug(f"Token {token_id[:20]}... 的数据已过期，无法延长 TTL")
                else:
                    logger.warning(f"批次更新失败: {len(batch)} 个 token")
                    # 批次完全失败时，延长所有 token 的 TTL
                    for token_id in batch:
                        old_orderbook = self.redis_client.get_orderbook(token_id)
                        if old_orderbook:
                            self.redis_client.set_orderbook(token_id, old_orderbook)
                        else:
                            total_expired += 1
                    total_missing += len(batch)
            
            # 如果有数据过期，需要从市场扫描中补充
            if total_expired > 0:
                logger.warning(f"发现 {total_expired} 个订单簿数据已过期，将在下次市场扫描时补充")
            
            if total_missing > 0:
                logger.info(f"订单簿更新完成: 成功更新 {total_updated}/{len(token_ids_list)} 个订单簿，"
                          f"{total_missing} 个 token 未返回数据（已延长 TTL 保留旧数据），"
                          f"{total_expired} 个 token 数据已过期")
            else:
                logger.info(f"订单簿更新完成: 成功更新 {total_updated}/{len(token_ids_list)} 个订单簿")
            
        except Exception as e:
            logger.error(f"更新订单簿失败: {e}")
            import traceback
            traceback.print_exc()
    
    def get_status(self) -> Dict[str, Any]:
        """
        获取服务状态
        
        Returns:
            服务状态字典
        """
        try:
            token_count = len(self.redis_client.get_all_token_ids())
            market_count = len(self.redis_client.get_all_market_ids())
            redis_connected = self.redis_client.ping()
            
            return {
                "running": not self._should_stop,
                "redis_connected": redis_connected,
                "token_count": token_count,
                "market_count": market_count,
                "current_markets": len(self.current_markets),
                "current_tokens": len(self.current_token_ids),
                "scan_interval": self.market_scan_interval,
                "update_interval": self.orderbook_update_interval
            }
        except Exception as e:
            logger.error(f"获取服务状态失败: {e}")
            return {
                "running": False,
                "error": str(e)
            }


def main():
    """主函数"""
    service = OrderbookDataService()
    
    try:
        service.start()
        
        # 保持运行
        while True:
            time.sleep(1)
            # 可以在这里添加健康检查等逻辑
            
    except KeyboardInterrupt:
        logger.info("收到停止信号")
    except Exception as e:
        logger.error(f"服务运行出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        service.stop()


if __name__ == "__main__":
    main()

