from __future__ import annotations


class CommissionCalculator:
    def __init__(self, maker_fee: float = 0.0002, taker_fee: float = 0.00055) -> None:
        self.maker_fee = float(maker_fee)
        self.taker_fee = float(taker_fee)
        self.total_cost = self.maker_fee + self.taker_fee

    def calculate_min_profitable_price(self, entry_price: float, direction: str, buffer: float = 0.001) -> float:
        total_cost_with_buffer = float(self.total_cost) + float(buffer)
        if direction.lower() in {"long", "buy"}:
            return float(entry_price) * (1.0 + total_cost_with_buffer)
        else:
            return float(entry_price) * (1.0 - total_cost_with_buffer)

    def validate_trade_profitability(self, entry_price: float, exit_price: float, direction: str) -> tuple[bool, float]:
        if direction.lower() in {"long", "buy"}:
            gross_profit_pct = (float(exit_price) - float(entry_price)) / float(entry_price)
        else:
            gross_profit_pct = (float(entry_price) - float(exit_price)) / float(entry_price)
        net_profit_pct = gross_profit_pct - self.total_cost
        return (net_profit_pct > 0.0), float(net_profit_pct)

    def calculate_targets_with_fees(self, entry_price: float, direction: str = "long") -> dict:
        if direction.lower() in {"long", "buy"}:
            tp1 = float(entry_price) * (1.0 + 0.008)  # 0.8%
            tp2 = float(entry_price) * (1.0 + 0.015)  # 1.5%
            sl = float(entry_price) * (1.0 - 0.005)   # -0.5%
        else:
            tp1 = float(entry_price) * (1.0 - 0.008)  # -0.8%
            tp2 = float(entry_price) * (1.0 - 0.015)  # -1.5%
            sl = float(entry_price) * (1.0 + 0.005)   # +0.5%
        return {
            "tp1": round(tp1, 6),
            "tp2": round(tp2, 6),
            "sl": round(sl, 6),
            "expected_net_profit_tp1": 0.008 - self.total_cost,
            "expected_net_profit_tp2": 0.015 - self.total_cost,
        }


