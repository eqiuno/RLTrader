import pandas as pd
import numpy as np
from typing import Callable, List

from lib.env.reward.BaseRewardStrategy import BaseRewardStrategy


class SortinoRatio(BaseRewardStrategy):
    def __init__(self, rf=1.03):
        self.rf = rf

    def reset_reward(self):
        pass

    def get_reward(self,
                   current_step: int,
                   current_price: Callable[[str], float],
                   observations: pd.DataFrame,
                   account_history: pd.DataFrame,
                   net_worths: List[float]) -> float:
        curr_balance = account_history['balance'].values[-1]
        prev_balance = account_history['balance'].values[-2] if len(account_history['balance']) > 1 else curr_balance
        price = current_price()
        curr_net_worth = net_worths[-1]

        initial_balance =  account_history['balance'][0]
        downside_returns = [self.rf - x for x in net_worths if x / initial_balance < self.rf]
        downside = pow(np.sum(np.square(downside_returns)) / len(downside_returns), 0.5) if len(downside_returns) > 0 else 1E-9
        reward = (net_worths[-1] / initial_balance - self.rf) / downside
        log_str = 'step:{:>5d} balance:{:>10.2f} prev_balance:{:>10.2f} price:{:>8.2f} worth:{:>10.2f} reward:{:>8.4f}'
        print(log_str.format(current_step, curr_balance, prev_balance, price, curr_net_worth, reward))
        return reward
