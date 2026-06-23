"""
做市策略模块 - 基于流动性奖励的优化策略
"""
import math
from typing import Dict, Any, Optional, Tuple
from config import config
from logger import setup_logger

logger = setup_logger("market_making_strategy")


class MarketMakingStrategy:
    """做市策略"""
    
    def __init__(self):
        """初始化做市策略"""
        pass
    
    @staticmethod
    def get_order_price_min_tick_size(market: Optional[Dict[str, Any]] = None) -> float:
        """
        从市场数据中获取 orderPriceMinTickSize
        
        Args:
            market: 市场数据字典，如果为 None 则返回默认值 0.01
            
        Returns:
            orderPriceMinTickSize，如果无法获取则返回默认值 0.01
        """
        if market is None:
            return 0.01
        
        # 尝试从市场数据中获取 orderPriceMinTickSize
        tick_size = market.get("orderPriceMinTickSize")
        if tick_size is not None:
            return float(tick_size)
        
        # 如果市场数据中没有，尝试从 Redis 获取
        try:
            from redis_orderbook_client import RedisOrderbookClient
            from config import config
            
            market_id = market.get("id")
            if market_id:
                storage_config = config.orderbook_service.get("storage", {})
                redis_client = RedisOrderbookClient(
                    orderbook_ttl=storage_config.get("orderbook_ttl", 300),
                    db_path=storage_config.get("db_path"),
                )
                try:
                    cached_market = redis_client.get_market(market_id)
                    if cached_market:
                        tick_size = cached_market.get("orderPriceMinTickSize")
                        if tick_size is not None:
                            return float(tick_size)
                finally:
                    redis_client.close()
        except Exception as e:
            logger.debug(f"从 Redis 获取 orderPriceMinTickSize 失败: {e}")
        
        # 默认返回 0.01（两位小数）
        return 0.01
    
    def infer_tick_size_from_orderbook(self, orderbook: Dict[str, Any]) -> float:
        """
        从订单簿中的价格推断最小价格步长（tick_size）
        
        通过检查订单簿中的价格小数位数，推断出市场的 tick_size
        例如：如果价格是 0.331（三位小数），推断 tick_size = 0.001
        如果价格是 0.33（两位小数），推断 tick_size = 0.01
        
        Args:
            orderbook: 订单簿数据，包含 bids 和 asks
            
        Returns:
            推断出的 tick_size，如果无法推断则返回 0.01（默认值）
        """
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        
        # 收集所有价格，检查小数位数
        all_prices = []
        for bid in bids[:10]:  # 只检查前10个，避免性能问题
            price = bid.get("price")
            if price:
                try:
                    all_prices.append(float(price))
                except (ValueError, TypeError):
                    continue
        
        for ask in asks[:10]:  # 只检查前10个
            price = ask.get("price")
            if price:
                try:
                    all_prices.append(float(price))
                except (ValueError, TypeError):
                    continue
        
        if not all_prices:
            return 0.01  # 默认值
        
        # 检查所有价格的小数位数，取最大值
        max_decimal_places = 0
        for price in all_prices:
            # 将价格转换为字符串，检查小数位数
            price_str = f"{price:.10f}".rstrip('0').rstrip('.')
            if '.' in price_str:
                decimal_places = len(price_str.split('.')[1])
                max_decimal_places = max(max_decimal_places, decimal_places)
        
        # 根据最大小数位数推断 tick_size
        if max_decimal_places >= 3:
            return 0.001
        elif max_decimal_places == 2:
            return 0.01
        elif max_decimal_places == 1:
            return 0.1
        else:
            return 0.01  # 默认值
    
    def calculate_mid_price(self, orderbook: Dict[str, Any]) -> Optional[float]:
        """
        计算中间价格：使用 ask 一价和 bid 一价除以2
        
        Args:
            orderbook: 订单簿数据，包含 bids 和 asks
            
        Returns:
            中间价格，如果无法计算则返回 None
        """
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])
        
        if not bids or not asks:
            return None
        
        # best_bid = 最高买价（bids[-1]，按价格降序排列）
        # best_ask = 最低卖价（asks[-1]，按价格升序排列）
        best_bid = float(bids[-1].get("price", 0))
        best_ask = float(asks[-1].get("price", 0))
        
        if best_bid <= 0 or best_ask <= 0:
            return None
        
        mid_price = (best_bid + best_ask) / 2
        return mid_price
    
    def normalize_price(self, price: float, order_price_min_tick_size: Optional[float] = None) -> float:
        """
        规范化价格：根据市场的最小价格步长（orderPriceMinTickSize）向下取整，并限制在有效范围内 [0.01, 1.0]
        
        根据 Polymarket 的限制：
        - 价格必须符合市场的最小价格步长（orderPriceMinTickSize）
        - 如果 orderPriceMinTickSize 是 0.01，则保留两位小数
        - 如果 orderPriceMinTickSize 是 0.001，则保留三位小数
        - 价格范围是 0.01 到 1.0 之间
        - 使用向下取整（floor）而不是四舍五入
        
        Args:
            price: 原始价格
            order_price_min_tick_size: 市场的最小价格步长（从市场数据的 orderPriceMinTickSize 字段获取）
                                      如果为 None，默认使用 0.01（两位小数）
            
        Returns:
            规范化后的价格（根据 orderPriceMinTickSize 决定小数位数，范围 [0.01, 1.0]）
        """
        # 确定最小价格步长和小数位数
        if order_price_min_tick_size is None:
            # 默认使用 0.01（两位小数）
            tick_size = 0.01
            decimal_places = 2
        else:
            tick_size = float(order_price_min_tick_size)
            # 根据 orderPriceMinTickSize 确定小数位数
            if tick_size == 0.001:
                decimal_places = 3
            elif tick_size == 0.01:
                decimal_places = 2
            else:
                # 对于其他值，尝试自动计算小数位数
                # 例如：0.0001 -> 4位小数，0.1 -> 1位小数
                tick_size_str = f"{tick_size:.10f}".rstrip('0').rstrip('.')
                if '.' in tick_size_str:
                    decimal_places = len(tick_size_str.split('.')[1])
                else:
                    decimal_places = 0
        
        # 向下取整到最小价格步长的倍数
        # 例如：如果 tick_size = 0.01，价格 0.156 会变成 0.15
        # 例如：如果 tick_size = 0.001，价格 0.1567 会变成 0.156
        if tick_size > 0:
            normalized = math.floor(float(price) / tick_size) * tick_size
        else:
            normalized = float(price)
        
        # 四舍五入到指定小数位数（避免浮点数精度问题）
        normalized = round(normalized, decimal_places)
        
        # 限制在有效范围内 [0.01, 1.0]
        normalized = max(0.01, min(1.0, normalized))
        
        return normalized
    
    def round_price(self, price: float) -> float:
        """
        将价格四舍五入到小数点后两位（0.01 的倍数）
        
        注意：此方法已被 normalize_price() 替代，保留用于向后兼容
        建议使用 normalize_price() 以确保价格在有效范围内
        
        Args:
            price: 原始价格
            
        Returns:
            四舍五入后的价格（两位小数）
        """
        return round(price, 2)
    
    def calculate_reward_range(
        self, 
        mid_price: float, 
        rewards_max_spread: float,
        market: Optional[Dict[str, Any]] = None
    ) -> Tuple[float, float]:
        """
        计算奖励区间
        
        Args:
            mid_price: 中间价格
            rewards_max_spread: 奖励最大价差（美分，例如 3.5 表示 0.035）
            market: 市场数据字典（可选，用于获取 orderPriceMinTickSize）
            
        Returns:
            (buy_price, sell_price) 奖励区间边界价格
            买单价格 = 中间价 - rewards_max_spread
            卖单价格 = 中间价 + rewards_max_spread
        """
        # rewards_max_spread 是美分，需要转换为小数
        # 例如 3.5 美分 = 0.035
        spread = (rewards_max_spread - 1) / 100
        
        # 买单价格 = 中间价 - rewards_max_spread
        buy_price = mid_price - spread
        # 卖单价格 = 中间价 + rewards_max_spread
        sell_price = mid_price + spread
        
        # 获取市场的最小价格步长
        tick_size = self.get_order_price_min_tick_size(market)
        
        # 规范化价格（根据市场的最小价格步长四舍五入，并限制在有效范围内 [0.01, 1.0]）
        buy_price = self.normalize_price(buy_price, tick_size)
        sell_price = self.normalize_price(sell_price, tick_size)
        
        return buy_price, sell_price
    
    def calculate_order_prices(
        self,
        orderbook: Dict[str, Any],
        rewards_max_spread: float,
        use_conservative_price: bool = False,
        market: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, float]]:
        """
        计算挂单价格（奖励区间边界，离中心价最远）
        
        Args:
            orderbook: 订单簿数据
            rewards_max_spread: 奖励最大价差（美分）
            use_conservative_price: 是否使用保守价格（数据过期时使用，使用极低/极高价格避免成交）
            market: 市场数据字典（可选，用于获取 orderPriceMinTickSize）
            
        Returns:
            包含 buy_price 和 sell_price 的字典，如果无法计算则返回 None
        """
        # 获取市场的最小价格步长
        tick_size = self.get_order_price_min_tick_size(market)
        
        # 如果使用保守价格（数据过期时），使用极低/极高价格，几乎不会被成交
        if use_conservative_price:
            buy_price = 0.01  # 极低价格，几乎不会被成交
            sell_price = 0.99  # 极高价格，几乎不会被成交
            logger.debug(
                f"使用保守价格（数据过期）: 买入价={buy_price:.2f}, 卖出价={sell_price:.2f}"
            )
            return {
                "mid_price": 0.50,  # 占位值，实际不使用
                "buy_price": buy_price,
                "sell_price": sell_price,
                "reward_min_price": buy_price,
                "reward_max_price": sell_price
            }
        
        mid_price = self.calculate_mid_price(orderbook)
        if mid_price is None:
            logger.warning("无法计算中间价格")
            return None
        
        buy_price, sell_price = self.calculate_reward_range(mid_price, rewards_max_spread, market)
        
        logger.debug(
            f"价格计算: 中间价={mid_price:.2f}, "
            f"rewards_max_spread={rewards_max_spread}美分, "
            f"买入价={buy_price:.2f}, 卖出价={sell_price:.2f}"
        )
        
        return {
            "mid_price": self.normalize_price(mid_price, tick_size),
            "buy_price": buy_price,
            "sell_price": sell_price,
            "reward_min_price": buy_price,
            "reward_max_price": sell_price
        }
    
    def calculate_order_size(
        self,
        market: Dict[str, Any],
        multiplier: Optional[float] = None
    ) -> int:
        """
        计算实际下单份额（基于市场最小奖励份额的倍数）
        
        Args:
            market: 市场数据
            multiplier: 倍数（如果为None，使用配置值）
            
        Returns:
            实际下单份额（至少为市场最小份额）
        """
        if multiplier is None:
            multiplier = config.order_size_multiplier
        
        rewards_min_size = market.get("rewards_min_size", 0)
        
        if rewards_min_size <= 0:
            logger.warning(f"市场最小份额无效: {rewards_min_size}，使用默认值 50")
            rewards_min_size = 50
        
        # 实际下单份额 = 市场最小份额 × 倍数（至少为市场最小份额）
        actual_size = int(rewards_min_size * multiplier)
        actual_size = max(actual_size, rewards_min_size)  # 确保至少满足最小份额要求
        
        logger.debug(
            f"份额计算: 市场最小份额={rewards_min_size}, "
            f"倍数={multiplier}, "
            f"实际下单份额={actual_size}"
        )
        
        return actual_size
    
    def calculate_hedge_sell_price(
        self,
        buy_price: float,
        current_market_price: Optional[float] = None,
        min_profit_margin_bps: Optional[int] = None,
        market: Optional[Dict[str, Any]] = None,
        best_bid_price: Optional[float] = None,
        max_bid_gap: Optional[float] = None
    ) -> float:
        """
        计算对冲卖出价格（确保不低于买入价）
        
        Args:
            buy_price: 买入价格
            current_market_price: 当前市场价格（如果为None，使用买入价）
            min_profit_margin_bps: 最小利润（基点，如果为None，使用配置值）
            market: 市场数据字典（可选，用于获取 orderPriceMinTickSize）
            
        Returns:
            对冲卖出价格
        """
        if min_profit_margin_bps is None:
            min_profit_margin_bps = config.min_profit_margin_bps
        
        if current_market_price is None:
            current_market_price = buy_price
        
        if max_bid_gap is None:
            max_bid_gap = config.hedge_sell_max_bid_gap
        
        tick_size = self.get_order_price_min_tick_size(market)
        
        sell_price = None
        if best_bid_price is not None and max_bid_gap is not None:
            price_gap = abs(buy_price - best_bid_price)
            if price_gap <= max_bid_gap:
                sell_price = best_bid_price
        
        if sell_price is None:
            # 卖出价 = max(买入价, 当前市场价) + 最小利润
            base_price = max(buy_price, current_market_price)
            profit = min_profit_margin_bps / 10000  # 基点转小数
            sell_price = base_price + profit
        
        # 规范化价格（根据市场的最小价格步长四舍五入，并限制在有效范围内 [0.01, 1.0]）
        sell_price = self.normalize_price(sell_price, tick_size)
        
        logger.debug(
            f"对冲卖出价计算: 买入价={buy_price:.2f}, "
            f"当前市场价={current_market_price:.2f}, "
            f"最小利润={min_profit_margin_bps}bps, "
            f"最优买价={best_bid_price if best_bid_price is not None else 'N/A'}, "
            f"使用买一价阈值={max_bid_gap:.2f}, "
            f"卖出价={sell_price:.2f}"
        )
        
        return sell_price
    
    def is_price_in_reward_range(
        self,
        price: float,
        mid_price: float,
        rewards_max_spread: float
    ) -> bool:
        """
        检查价格是否在奖励区间内
        
        Args:
            price: 要检查的价格
            mid_price: 中间价格
            rewards_max_spread: 奖励最大价差（美分）
            
        Returns:
            如果价格在奖励区间内返回 True，否则返回 False
        """
        buy_price, sell_price = self.calculate_reward_range(mid_price, rewards_max_spread)
        return buy_price <= price <= sell_price
    
    def calculate_actual_buy_price(
        self,
        orderbook: Dict[str, Any],
        buy_price: float
    ) -> Optional[float]:
        """
        根据订单簿和奖励下边界计算实际挂单价格
        
        逻辑：
        1. 如果订单簿只有买一价（没有买二价），返回 None（跳过）
        2. 如果订单簿有买一价和买二价：
           - 如果买二价 >= buy_price（奖励下边界），返回买二价
           - 如果买二价 < buy_price（奖励下边界），返回 buy_price（奖励下边界）
        
        Args:
            orderbook: 订单簿数据，包含 bids 和 asks
            buy_price: 奖励区间下边界
            
        Returns:
            实际挂单价格，如果无法计算（只有买一价）则返回 None
        """
        bids = orderbook.get("bids", [])
        
        if not bids:
            return None
        
        # 获取买一价（最高买价）
        best_bid_price = float(bids[-1].get("price", 0))
        
        # 找买二价（第二高的买价，价格低于买一价）
        second_bid_price = None
        if len(bids) >= 2:
            # 从后往前找，找到第一个价格低于买一价的买单
            for i in range(len(bids) - 2, -1, -1):
                bid_price = float(bids[i].get("price", 0))
                if bid_price < best_bid_price:
                    second_bid_price = bid_price
                    break
        
        # 如果订单簿只有买一价（没有买二价），返回 None（跳过）
        if second_bid_price is None:
            return None
        
        # 如果订单簿有买一价和买二价
        # 如果买二价 >= buy_price（奖励下边界），返回买二价
        if second_bid_price >= buy_price:
            return second_bid_price
        
        # 如果买二价 < buy_price（奖励下边界），返回 buy_price（奖励下边界）
        # 因为使用奖励下边界挂单后，买二价在我们后面，我们就是买二价
        return buy_price
    
    def can_place_buy_order_safely(
        self,
        orderbook: Dict[str, Any],
        buy_price: float,
        sell_price: float,
        order_size: float,
        actual_buy_price: float
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        检查是否可以安全挂买单（挂单后成为买二价，并检测价格断层）
        
        核心逻辑：
        1. 如果 actual_buy_price == 买二价：挂单后我们就是买二价，不需要位置检查，只需检查价格断层和保护份额
        2. 如果 actual_buy_price == buy_price（奖励下边界）：检查挂单后是否成为买二价，如果不是则跳过；如果是买二价，继续检查价格断层和保护份额
        
        Args:
            orderbook: 订单簿数据，包含 bids 和 asks
            buy_price: 奖励区间下边界
            sell_price: 奖励区间上边界
            order_size: 我们计划挂单的份额
            actual_buy_price: 实际挂单价格（买二价或奖励下边界）
            
        Returns:
            (can_place, info_dict)
            can_place: True 表示可以安全挂单，False 表示不能挂单
            info_dict: 包含详细信息，用于日志输出
        """
        bids = orderbook.get("bids", [])
        
        # 统计价格高于、等于、低于 actual_buy_price 的买单
        bids_above_actual_price = []  # 价格 > actual_buy_price 的买单（排在我们前面）
        bids_at_actual_price = []     # 价格 = actual_buy_price 的买单（和我们同一位置）
        bids_below_actual_price = []  # 价格 < actual_buy_price 的买单（排在我们后面）
        bids_in_reward_range = []  # 奖励区间内的买单（用于信息展示）
        
        for bid in bids:
            bid_price = float(bid.get("price", 0))
            
            # 统计奖励区间内的买单（用于信息展示）
            if buy_price <= bid_price <= sell_price:
                bids_in_reward_range.append(bid_price)
            
            # 按价格分类（基于 actual_buy_price）
            if bid_price > actual_buy_price:
                bids_above_actual_price.append(bid_price)
            elif abs(bid_price - actual_buy_price) < 0.0001:  # 考虑浮点数精度
                bids_at_actual_price.append(bid_price)
            else:  # bid_price < actual_buy_price
                bids_below_actual_price.append(bid_price)
        
        # 按价格降序排序（最高价在前）
        bids_above_actual_price.sort(reverse=True)
        bids_at_actual_price.sort(reverse=True)
        bids_below_actual_price.sort(reverse=True)
        bids_in_reward_range.sort(reverse=True)
        
        # 计算买一价和买二价
        best_bid_price = None
        best_bid_size = 0.0
        second_bid_price = None
        second_bid_size = 0.0
        
        if bids:
            # bids 按价格降序排列，bids[-1] 是最高买价（买一价）
            best_bid_price = float(bids[-1].get("price", 0))
            # 买一价的份额 = 所有价格为买一价的买单份额之和
            for bid in bids:
                if abs(float(bid.get("price", 0)) - best_bid_price) < 0.0001:  # 考虑浮点数精度
                    best_bid_size += float(bid.get("size", 0))
            
            # 找买二价（第二高的买价，价格低于买一价）
            if len(bids) >= 2:
                # 从后往前找，找到第一个价格低于买一价的买单
                for i in range(len(bids) - 2, -1, -1):
                    bid_price = float(bids[i].get("price", 0))
                    if bid_price < best_bid_price:
                        second_bid_price = bid_price
                        # 买二价的份额 = 所有价格为买二价的买单份额之和
                        for bid in bids:
                            if abs(float(bid.get("price", 0)) - second_bid_price) < 0.0001:  # 考虑浮点数精度
                                second_bid_size += float(bid.get("size", 0))
                        break
        
        # 判断 actual_buy_price 是否等于买二价
        is_second_bid_price = (second_bid_price is not None and 
                              abs(actual_buy_price - second_bid_price) < 0.0001)
        
        # 计算我们挂单后的位置（基于 actual_buy_price）
        unique_prices_above = len(set(bids_above_actual_price))  # 不同价格的数量
        count_at = len(bids_at_actual_price)  # 不包括我们，因为我们还没挂单
        
        # 我们挂单后的位置 = unique_prices_above + 1（因为价格 = actual_buy_price 的买单在下一个价格位置）
        our_position_after_place = unique_prices_above + 1
        
        # 构建信息字典
        info = {
            "count_in_range": len(bids_in_reward_range),
            "bids_in_range": bids_in_reward_range,
            "count_above": len(bids_above_actual_price),
            "unique_prices_above": unique_prices_above,
            "count_at": count_at,
            "count_below": len(bids_below_actual_price),
            "bids_above": bids_above_actual_price,
            "bids_at": bids_at_actual_price,
            "bids_below": bids_below_actual_price,
            "best_bid": best_bid_price,
            "best_bid_size": best_bid_size,
            "second_bid": second_bid_price,
            "second_bid_size": second_bid_size,
            "our_position": our_position_after_place,
            "actual_buy_price": actual_buy_price,
            "is_second_bid_price": is_second_bid_price,
            "price_cliff_detected": False,
            "price_cliff_reason": ""
        }
        
        # 检查逻辑
        if is_second_bid_price:
            # 如果使用买二价挂单，挂单后我们就是买二价，不需要位置检查
            # 只需要检查价格断层和保护份额
            pass  # 跳过位置检查
        else:
            # 如果使用奖励下边界挂单，需要检查挂单后是否成为买二价
            # 如果买二价 < 奖励下边界，使用奖励下边界挂单后，买二价在我们后面，我们就是买二价
            if our_position_after_place != 2:
                return False, {**info, "reason": f"使用奖励下边界挂单后将成为买{our_position_after_place}价（不是买二价），跳过挂单"}
        
        # 价格断层检查
        # 检查后1价、后2价
        # 我们的价格是 actual_buy_price
        # 后1价是所有价格 < actual_buy_price 中最高的那个
        # 后2价是所有价格 < actual_buy_price 中第二高的那个
        
        # 提取所有低于我们挂单价的不同价格，并按降序排列
        unique_prices_below_actual_price = sorted(list(set(bids_below_actual_price)), reverse=True)
        
        price_cliff_threshold = config.price_cliff_threshold  # 绝对差值
        min_protection_size = order_size * config.min_protection_size_multiplier  # 最小保护份额
        
        # 检查后1价
        if not unique_prices_below_actual_price:
            info["price_cliff_detected"] = True
            info["price_cliff_reason"] = "订单簿中没有低于我们挂单价的买单，存在价格断层风险"
            return False, {**info, "reason": info["price_cliff_reason"]}
        
        # 检查后1价、后2价
        next_prices_info = []
        cumulative_size = 0.0
        
        for i in range(2):  # 检查后1价、后2价
            if i >= len(unique_prices_below_actual_price):
                info["price_cliff_detected"] = True
                info["price_cliff_reason"] = f"订单簿中缺少后{i+1}价，存在价格断层风险"
                return False, {**info, "reason": info["price_cliff_reason"]}
            
            next_price = unique_prices_below_actual_price[i]
            price_diff = actual_buy_price - next_price
            
            # 计算该价格层的总份额
            price_size = 0.0
            for bid in bids:
                bid_price = float(bid.get("price", 0))
                if abs(bid_price - next_price) < 0.0001:  # 考虑浮点数精度
                    price_size += float(bid.get("size", 0))
            
            cumulative_size += price_size
            next_prices_info.append({
                "position": i + 1,
                "price": next_price,
                "price_diff": price_diff,
                "size": price_size,
                "cumulative_size": cumulative_size
            })
            
            if price_diff > price_cliff_threshold:
                info["price_cliff_detected"] = True
                info["price_cliff_reason"] = f"后{i+1}价 ({next_price:.4f}) 与我们挂单价 ({actual_buy_price:.4f}) 的差值 ({price_diff:.4f}) 超过阈值 ({price_cliff_threshold:.4f})，存在价格断层风险"
                return False, {**info, "reason": info["price_cliff_reason"]}
        
        # 3. 检查保护份额是否足够
        # 计算买一价和买二价的总份额
        total_protection_size = best_bid_size + second_bid_size
        
        if total_protection_size < min_protection_size:
            info["price_cliff_detected"] = True
            info["price_cliff_reason"] = f"买一价和买二价的总份额 ({total_protection_size:.2f}) 小于最小保护份额要求 ({min_protection_size:.2f})，容易被吃掉"
            return False, {**info, "reason": info["price_cliff_reason"]}
        
        # 如果通过所有检查
        info["next_prices"] = next_prices_info
        info["total_protection_size"] = total_protection_size
        info["min_protection_size"] = min_protection_size
        position_desc = "买二价" if is_second_bid_price else f"买{our_position_after_place}价"
        return True, {**info, "reason": f"挂单后将成为{position_desc}，且未检测到价格断层，安全"}