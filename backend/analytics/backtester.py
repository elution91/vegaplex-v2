"""
Backtester Module
Backtests volatility skew arbitrage strategies
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
import json

logger = logging.getLogger(__name__)


class StrategyBacktester:
    """Backtests volatility skew arbitrage strategies"""
    
    def __init__(self, config):
        self.config = config
        self.backtest_results = []
    
    def run_backtest(self, start_date: str, end_date: str, symbols: List[str]) -> Dict:
        """Run comprehensive backtest"""
        logger.info(f"Starting backtest from {start_date} to {end_date}")
        
        results = {
            'period': {'start': start_date, 'end': end_date},
            'symbols': symbols,
            'summary': {},
            'trades': [],
            'performance_metrics': {},
            'risk_metrics': {}
        }
        
        # For each symbol, run historical analysis
        for symbol in symbols:
            try:
                symbol_results = self._backtest_symbol(symbol, start_date, end_date)
                results['trades'].extend(symbol_results['trades'])
                
            except Exception as e:
                logger.warning(f"Failed to backtest {symbol}: {e}")
                continue
        
        # Calculate aggregate performance metrics
        results['performance_metrics'] = self._calculate_performance_metrics(results['trades'])
        results['risk_metrics'] = self._calculate_risk_metrics(results['trades'])
        results['summary'] = self._generate_summary(results)
        
        logger.info(f"Backtest complete. {len(results['trades'])} trades generated.")
        
        return results
    
    def _backtest_symbol(self, symbol: str, start_date: str, end_date: str) -> Dict:
        """Backtest a single symbol"""
        trades = []
        
        # Generate date range for backtest
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        dates = pd.date_range(start, end, freq='D')
        
        # Simulate daily scanning
        for date in dates[::5]:  # Scan every 5 days (weekly)
            try:
                # Simulate finding opportunities (would use historical data in practice)
                opportunity = self._simulate_historical_opportunity(symbol, date)
                
                if opportunity and opportunity['confidence'] > 0.5:
                    # Simulate trade execution and outcome
                    trade_result = self._simulate_trade_execution(opportunity, date)
                    if trade_result:
                        trades.append(trade_result)
                        
            except Exception as e:
                continue
        
        return {'symbol': symbol, 'trades': trades}
    
    def _simulate_historical_opportunity(self, symbol: str, date: datetime) -> Optional[Dict]:
        """Simulate finding historical opportunity"""
        # This would use actual historical options data
        # For now, return simulated opportunities
        
        # Simulate random opportunities with some pattern
        np.random.seed(hash(str(symbol + str(date))) % 2**32)
        
        if np.random.random() < 0.3:  # 30% chance of opportunity
            opportunity_types = ['skew_arbitrage', 'calendar_spread', 'vertical_spread']
            opp_type = np.random.choice(opportunity_types)
            
            return {
                'type': opp_type,
                'subtype': f'{opp_type}_simulated',
                'symbol': symbol,
                'date': date.strftime('%Y-%m-%d'),
                'confidence': np.random.uniform(0.5, 0.9),
                'expected_pnl': np.random.uniform(0.02, 0.15),
                'risk_level': np.random.choice(['low', 'medium', 'high']),
                'entry_signal': 'simulated_signal',
                'rationale': f'Simulated {opp_type} opportunity'
            }
        
        return None
    
    def _simulate_trade_execution(self, opportunity: Dict, entry_date: datetime) -> Optional[Dict]:
        """Simulate trade execution and outcome"""
        try:
            # Simulate trade parameters
            entry_price = np.random.uniform(1.0, 5.0)
            contracts = np.random.randint(1, 10)
            max_loss = entry_price * contracts * 100 * np.random.uniform(0.5, 2.0)
            
            # Simulate holding period
            holding_days = np.random.randint(7, 60)
            exit_date = entry_date + timedelta(days=holding_days)
            
            # Simulate P&L based on opportunity type and confidence
            base_pnl = opportunity['expected_pnl'] * max_loss
            confidence_factor = opportunity['confidence']
            
            # Add randomness
            pnl = base_pnl * confidence_factor * np.random.uniform(0.5, 1.5)
            
            # Some trades lose money
            if np.random.random() < 0.3:  # 30% loss rate
                pnl = -abs(pnl) * np.random.uniform(0.2, 1.0)
            
            # Calculate return
            return_pct = pnl / max_loss if max_loss > 0 else 0
            
            trade = {
                'symbol': opportunity['symbol'],
                'type': opportunity['type'],
                'subtype': opportunity['subtype'],
                'entry_date': entry_date.strftime('%Y-%m-%d'),
                'exit_date': exit_date.strftime('%Y-%m-%d'),
                'holding_days': holding_days,
                'contracts': contracts,
                'entry_price': entry_price,
                'max_loss': max_loss,
                'pnl': pnl,
                'return_pct': return_pct,
                'confidence': opportunity['confidence'],
                'risk_level': opportunity['risk_level'],
                'win': pnl > 0
            }
            
            return trade
            
        except Exception as e:
            logger.error(f"Error simulating trade execution: {e}")
            return None
    
    def _calculate_performance_metrics(self, trades: List[Dict]) -> Dict:
        """Calculate performance metrics"""
        if not trades:
            return {}
        
        df = pd.DataFrame(trades)
        
        # Basic metrics
        total_trades = len(df)
        winning_trades = len(df[df['win'] == True])
        losing_trades = len(df[df['win'] == False])
        
        win_rate = winning_trades / total_trades if total_trades > 0 else 0
        
        # P&L metrics
        total_pnl = df['pnl'].sum()
        avg_pnl = df['pnl'].mean()
        avg_win = df[df['win'] == True]['pnl'].mean() if winning_trades > 0 else 0
        avg_loss = df[df['win'] == False]['pnl'].mean() if losing_trades > 0 else 0
        
        # Return metrics
        avg_return = df['return_pct'].mean()
        total_return = total_pnl / df['max_loss'].sum() if df['max_loss'].sum() > 0 else 0
        
        # Holding period metrics
        avg_holding_days = df['holding_days'].mean()
        
        # Risk-adjusted metrics
        profit_factor = abs(avg_win / avg_loss) if avg_loss != 0 else float('inf')
        
        # Monthly metrics (approximate)
        months = (pd.to_datetime(df['exit_date']).max() - pd.to_datetime(df['entry_date']).min()).days / 30.44
        monthly_pnl = total_pnl / months if months > 0 else 0
        monthly_return = monthly_pnl / df['max_loss'].sum() if df['max_loss'].sum() > 0 else 0
        
        return {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'avg_return': avg_return,
            'total_return': total_return,
            'avg_holding_days': avg_holding_days,
            'profit_factor': profit_factor,
            'monthly_pnl': monthly_pnl,
            'monthly_return': monthly_return
        }
    
    def _calculate_risk_metrics(self, trades: List[Dict]) -> Dict:
        """Calculate risk metrics"""
        if not trades:
            return {}
        
        df = pd.DataFrame(trades)
        
        # Drawdown calculations
        cumulative_pnl = df['pnl'].cumsum()
        running_max = cumulative_pnl.expanding().max()
        drawdown = (cumulative_pnl - running_max) / running_max
        
        max_drawdown = drawdown.min()
        avg_drawdown = drawdown[drawdown < 0].mean() if (drawdown < 0).any() else 0
        
        # Volatility of returns
        returns_vol = df['return_pct'].std()
        
        # Sharpe ratio (simplified, assuming 0% risk-free rate)
        sharpe_ratio = df['return_pct'].mean() / returns_vol if returns_vol > 0 else 0
        
        # Maximum consecutive losses
        losses = df['win'] == False
        max_consecutive_losses = 0
        current_streak = 0
        
        for loss in losses:
            if loss:
                current_streak += 1
                max_consecutive_losses = max(max_consecutive_losses, current_streak)
            else:
                current_streak = 0
        
        # Risk by trade type
        risk_by_type = {}
        for trade_type in df['type'].unique():
            type_df = df[df['type'] == trade_type]
            risk_by_type[trade_type] = {
                'win_rate': type_df['win'].mean(),
                'avg_return': type_df['return_pct'].mean(),
                'return_vol': type_df['return_pct'].std(),
                'max_loss': type_df['pnl'].min(),
                'count': len(type_df)
            }
        
        return {
            'max_drawdown': max_drawdown,
            'avg_drawdown': avg_drawdown,
            'returns_volatility': returns_vol,
            'sharpe_ratio': sharpe_ratio,
            'max_consecutive_losses': max_consecutive_losses,
            'risk_by_type': risk_by_type,
            'largest_win': df['pnl'].max(),
            'largest_loss': df['pnl'].min(),
            'avg_risk_per_trade': df['max_loss'].mean()
        }
    
    def _generate_summary(self, results: Dict) -> Dict:
        """Generate backtest summary"""
        perf = results['performance_metrics']
        risk = results['risk_metrics']
        
        # Overall assessment
        if perf.get('win_rate', 0) > 0.6 and perf.get('total_return', 0) > 0:
            assessment = "Strong performance"
        elif perf.get('win_rate', 0) > 0.5 and perf.get('total_return', 0) > -0.1:
            assessment = "Moderate performance"
        else:
            assessment = "Poor performance"
        
        return {
            'assessment': assessment,
            'key_metrics': {
                'win_rate': f"{perf.get('win_rate', 0):.1%}",
                'total_return': f"{perf.get('total_return', 0):.1%}",
                'sharpe_ratio': f"{risk.get('sharpe_ratio', 0):.2f}",
                'max_drawdown': f"{risk.get('max_drawdown', 0):.1%}",
                'profit_factor': f"{perf.get('profit_factor', 0):.2f}"
            },
            'recommendations': self._generate_recommendations(perf, risk)
        }
    
    def _generate_recommendations(self, perf: Dict, risk: Dict) -> List[str]:
        """Generate recommendations based on backtest results"""
        recommendations = []
        
        # Win rate recommendations
        win_rate = perf.get('win_rate', 0)
        if win_rate < 0.4:
            recommendations.append("Consider tightening entry criteria - win rate too low")
        elif win_rate > 0.8:
            recommendations.append("Win rate very high - verify realistic assumptions")
        
        # Return recommendations
        total_return = perf.get('total_return', 0)
        if total_return < -0.2:
            recommendations.append("Strategy losing money - reconsider approach")
        elif total_return > 0.5:
            recommendations.append("High returns - verify risk assumptions")
        
        # Risk recommendations
        max_dd = risk.get('max_drawdown', 0)
        if max_dd < -0.3:
            recommendations.append("High drawdown - consider position sizing")
        
        # Profit factor recommendations
        profit_factor = perf.get('profit_factor', 0)
        if profit_factor < 1.2:
            recommendations.append("Low profit factor - improve risk/reward")
        
        # Sharpe ratio recommendations
        sharpe = risk.get('sharpe_ratio', 0)
        if sharpe < 0.5:
            recommendations.append("Low Sharpe ratio - improve risk-adjusted returns")
        
        if not recommendations:
            recommendations.append("Strategy metrics look reasonable")
        
        return recommendations
    
    def save_backtest_results(self, results: Dict, filename: str):
        """Save backtest results to file"""
        try:
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            logger.info(f"Backtest results saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save results: {e}")
    
    def load_backtest_results(self, filename: str) -> Optional[Dict]:
        """Load backtest results from file"""
        try:
            with open(filename, 'r') as f:
                results = json.load(f)
            logger.info(f"Backtest results loaded from {filename}")
            return results
        except Exception as e:
            logger.error(f"Failed to load results: {e}")
            return None
    
    def compare_strategies(self, results_list: List[Dict]) -> Dict:
        """Compare multiple backtest results"""
        if not results_list:
            return {}
        
        comparison = {
            'strategies': [],
            'ranking': [],
            'summary': {}
        }
        
        # Extract key metrics for each strategy
        for i, results in enumerate(results_list):
            perf = results.get('performance_metrics', {})
            risk = results.get('risk_metrics', {})
            
            strategy_metrics = {
                'name': results.get('name', f'Strategy_{i+1}'),
                'win_rate': perf.get('win_rate', 0),
                'total_return': perf.get('total_return', 0),
                'sharpe_ratio': risk.get('sharpe_ratio', 0),
                'max_drawdown': risk.get('max_drawdown', 0),
                'profit_factor': perf.get('profit_factor', 0),
                'total_trades': perf.get('total_trades', 0)
            }
            
            comparison['strategies'].append(strategy_metrics)
        
        # Rank strategies (simple scoring)
        for strategy in comparison['strategies']:
            score = 0
            score += strategy['win_rate'] * 25
            score += strategy['total_return'] * 25
            score += strategy['sharpe_ratio'] * 20
            score += (1 + strategy['max_drawdown']) * 15  # Lower drawdown better
            score += min(strategy['profit_factor'], 3) * 15
            
            strategy['score'] = score
        
        # Sort by score
        comparison['ranking'] = sorted(comparison['strategies'], key=lambda x: x['score'], reverse=True)
        
        return comparison