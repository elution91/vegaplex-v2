"""
Visualizer Module
Creates visualizations for volatility analysis and opportunities
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend — must be set before pyplot import
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.gridspec import GridSpec

try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

logger = logging.getLogger(__name__)


class VolatilityVisualizer:
    """Creates visualizations for volatility analysis"""
    
    def __init__(self):
        self.style = 'seaborn-v0_8'
        self.color_palette = 'husl'
        
        # Set up matplotlib
        plt.style.use(self.style)
        sns.set_palette(self.color_palette)
    
    def create_opportunity_dashboard(self, results: List[Dict]):
        """Create comprehensive dashboard of opportunities"""
        if not results:
            logger.warning("No results to visualize")
            return
        
        if PLOTLY_AVAILABLE:
            self._create_plotly_dashboard(results)
        else:
            self._create_matplotlib_dashboard(results)
    
    def create_plotly_dashboard_figure(self, results: List[Dict]):
        """Create interactive dashboard figure with Plotly and return it."""
        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=('Opportunities by Type', 'Confidence Scores',
                          'Expected PnL Distribution', 'Risk Levels',
                          'Top Opportunities', 'Opportunity Timeline'),
            specs=[[{"type": "bar"}, {"type": "histogram"}],
                   [{"type": "bar"}, {"type": "pie"}],
                   [{"type": "table"}, {"type": "scatter"}]]
        )

        # Convert results to DataFrame
        df = pd.DataFrame([{
            'symbol': r.symbol,
            'type': r.opportunity_type,
            'confidence': r.confidence_score,
            'expected_pnl': r.expected_pnl or 0,
            'risk_level': r.risk_metrics.get('risk_level', 'medium'),
            'date': r.timestamp
        } for r in results])

        # 1. Opportunities by type
        type_counts = df['type'].value_counts()
        fig.add_trace(
            go.Bar(x=type_counts.index, y=type_counts.values, name='Count'),
            row=1, col=1
        )

        # 2. Confidence scores
        fig.add_trace(
            go.Histogram(x=df['confidence'], name='Confidence', nbinsx=20),
            row=1, col=2
        )

        # 3. Expected PnL by type
        pnl_by_type = df.groupby('type')['expected_pnl'].mean()
        fig.add_trace(
            go.Bar(x=pnl_by_type.index, y=pnl_by_type.values, name='Avg PnL'),
            row=2, col=1
        )

        # 4. Risk levels
        risk_counts = df['risk_level'].value_counts()
        fig.add_trace(
            go.Pie(labels=risk_counts.index, values=risk_counts.values, name='Risk'),
            row=2, col=2
        )

        # 5. Top opportunities table
        top_opp = df.nlargest(10, 'confidence')[['symbol', 'type', 'confidence', 'expected_pnl']]
        fig.add_trace(
            go.Table(
                header=dict(values=['Symbol', 'Type', 'Confidence', 'Exp PnL']),
                cells=dict(values=[top_opp['symbol'], top_opp['type'],
                                 top_opp['confidence'], top_opp['expected_pnl']])
            ),
            row=3, col=1
        )

        # 6. Timeline
        df['date'] = pd.to_datetime(df['date'])
        fig.add_trace(
            go.Scatter(x=df['date'], y=df['confidence'], mode='markers',
                      text=df['symbol'], name='Opportunities'),
            row=3, col=2
        )

        fig.update_layout(
            title='Volatility Skew Arbitrage Scanner Dashboard',
            height=1200,
            showlegend=False
        )

        return fig

    def _create_plotly_dashboard(self, results: List[Dict]):
        """Create interactive dashboard with Plotly"""
        fig = self.create_plotly_dashboard_figure(results)
        fig.show()
    
    def _create_matplotlib_dashboard(self, results: List[Dict]):
        """Create dashboard with matplotlib"""
        # Convert results to DataFrame
        df = pd.DataFrame([{
            'symbol': r.symbol,
            'type': r.opportunity_type,
            'confidence': r.confidence_score,
            'expected_pnl': r.expected_pnl or 0,
            'risk_level': r.risk_metrics.get('risk_level', 'medium'),
            'date': r.timestamp
        } for r in results])
        
        # Create figure with subplots
        fig, axes = plt.subplots(3, 2, figsize=(15, 12))
        fig.suptitle('Volatility Skew Arbitrage Scanner Dashboard', fontsize=16)
        
        # 1. Opportunities by type
        type_counts = df['type'].value_counts()
        axes[0, 0].bar(type_counts.index, type_counts.values)
        axes[0, 0].set_title('Opportunities by Type')
        axes[0, 0].tick_params(axis='x', rotation=45)
        
        # 2. Confidence scores
        axes[0, 1].hist(df['confidence'], bins=20, alpha=0.7)
        axes[0, 1].set_title('Confidence Score Distribution')
        axes[0, 1].set_xlabel('Confidence')
        axes[0, 1].set_ylabel('Frequency')
        
        # 3. Expected PnL by type
        pnl_by_type = df.groupby('type')['expected_pnl'].mean()
        axes[1, 0].bar(pnl_by_type.index, pnl_by_type.values)
        axes[1, 0].set_title('Average Expected PnL by Type')
        axes[1, 0].tick_params(axis='x', rotation=45)
        
        # 4. Risk levels
        risk_counts = df['risk_level'].value_counts()
        axes[1, 1].pie(risk_counts.values, labels=risk_counts.index, autopct='%1.1f%%')
        axes[1, 1].set_title('Risk Level Distribution')
        
        # 5. Top opportunities table
        top_opp = df.nlargest(10, 'confidence')[['symbol', 'type', 'confidence', 'expected_pnl']]
        axes[2, 0].axis('tight')
        axes[2, 0].axis('off')
        table = axes[2, 0].table(cellText=top_opp.values, 
                                 colLabels=top_opp.columns,
                                 cellLoc='center',
                                 loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        axes[2, 0].set_title('Top 10 Opportunities')
        
        # 6. Timeline
        df['date'] = pd.to_datetime(df['date'])
        axes[2, 1].scatter(df['date'], df['confidence'], alpha=0.7)
        axes[2, 1].set_title('Opportunity Timeline')
        axes[2, 1].set_xlabel('Date')
        axes[2, 1].set_ylabel('Confidence')
        
        plt.tight_layout()
        plt.show()
    
    def create_volatility_surface_figure(self, vol_surface: Dict, symbol: str):
        """Create 3D volatility surface figure and return it."""
        if not PLOTLY_AVAILABLE:
            logger.warning("Plotly not available for 3D surface")
            return None

        surface_2d = vol_surface.get('surface_2d')
        if not surface_2d:
            logger.warning("No 2D surface data available")
            return None

        strike_grid = surface_2d['strike_grid']
        tte_grid = surface_2d['tte_grid']
        surface = surface_2d['surface']

        fig = go.Figure(data=[go.Surface(
            x=strike_grid,
            y=tte_grid,
            z=surface,
            colorscale='Viridis',
            hovertemplate='Strike: %{x:.1f}<br>TTE: %{y:.3f}<br>IV: %{z:.4f}<extra></extra>'
        )])

        fig.update_layout(
            title=f'Volatility Surface - {symbol}',
            scene=dict(
                xaxis_title='Strike',
                yaxis_title='Time to Expiry',
                zaxis_title='Implied Volatility'
            )
        )

        return fig

    def plot_volatility_surface(self, vol_surface: Dict, symbol: str):
        """Plot 3D volatility surface"""
        fig = self.create_volatility_surface_figure(vol_surface, symbol)
        if fig:
            fig.show()
    
    def plot_skew_analysis(self, skew_metrics: Dict, symbol: str):
        """Plot skew analysis"""
        call_skew = skew_metrics.get('call_skew', {})
        put_skew = skew_metrics.get('put_skew', {})
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle(f'Skew Analysis - {symbol}', fontsize=16)
        
        # Call skew by expiry
        call_by_expiry = call_skew.get('by_expiry', [])
        if call_by_expiry:
            call_df = pd.DataFrame(call_by_expiry)
            axes[0, 0].plot(call_df['tte'], call_df['slope'], 'o-', label='Call Skew')
            axes[0, 0].set_title('Call Skew by Time to Expiry')
            axes[0, 0].set_xlabel('Time to Expiry')
            axes[0, 0].set_ylabel('Skew Slope')
            axes[0, 0].grid(True)
        
        # Put skew by expiry
        put_by_expiry = put_skew.get('by_expiry', [])
        if put_by_expiry:
            put_df = pd.DataFrame(put_by_expiry)
            axes[0, 1].plot(put_df['tte'], put_df['slope'], 'o-', label='Put Skew', color='orange')
            axes[0, 1].set_title('Put Skew by Time to Expiry')
            axes[0, 1].set_xlabel('Time to Expiry')
            axes[0, 1].set_ylabel('Skew Slope')
            axes[0, 1].grid(True)
        
        # Skew comparison
        if call_by_expiry and put_by_expiry:
            call_df = pd.DataFrame(call_by_expiry)
            put_df = pd.DataFrame(put_by_expiry)
            axes[1, 0].plot(call_df['tte'], call_df['slope'], 'o-', label='Call')
            axes[1, 0].plot(put_df['tte'], put_df['slope'], 'o-', label='Put')
            axes[1, 0].set_title('Call vs Put Skew')
            axes[1, 0].set_xlabel('Time to Expiry')
            axes[1, 0].set_ylabel('Skew Slope')
            axes[1, 0].legend()
            axes[1, 0].grid(True)
        
        # Skew curvature
        call_curvatures = [c.get('curvature', 0) for c in call_by_expiry]
        put_curvatures = [p.get('curvature', 0) for p in put_by_expiry]
        
        if call_curvatures and put_curvatures:
            call_df = pd.DataFrame(call_by_expiry)
            put_df = pd.DataFrame(put_by_expiry)
            axes[1, 1].plot(call_df['tte'], call_curvatures, 'o-', label='Call')
            axes[1, 1].plot(put_df['tte'], put_curvatures, 'o-', label='Put')
            axes[1, 1].set_title('Skew Curvature')
            axes[1, 1].set_xlabel('Time to Expiry')
            axes[1, 1].set_ylabel('Curvature')
            axes[1, 1].legend()
            axes[1, 1].grid(True)
        
        plt.tight_layout()
        plt.show()
    
    def create_trade_payoff_figure(self, trade_structure: Dict, symbol: str, current_price: float = 100):
        """Create trade payoff diagram as a Plotly figure and return it."""
        if not PLOTLY_AVAILABLE or not trade_structure or not trade_structure.get('legs'):
            return None

        legs = trade_structure['legs']
        price_range = np.linspace(current_price * 0.7, current_price * 1.3, 100)
        total_payoff = np.zeros_like(price_range)

        fig = go.Figure()

        for leg in legs:
            strike = leg['strike']
            contracts = leg.get('contracts', 1)

            if leg['type'] == 'call':
                if leg['action'] == 'buy':
                    payoff = np.maximum(price_range - strike, 0) - leg.get('price', 0) * 100
                else:
                    payoff = leg.get('price', 0) * 100 - np.maximum(price_range - strike, 0)
            else:
                if leg['action'] == 'buy':
                    payoff = np.maximum(strike - price_range, 0) - leg.get('price', 0) * 100
                else:
                    payoff = leg.get('price', 0) * 100 - np.maximum(strike - price_range, 0)

            payoff *= contracts
            total_payoff += payoff

            fig.add_trace(go.Scatter(
                x=price_range, y=payoff, mode='lines',
                name=f"{leg['action']} {leg['type']} {strike}",
                opacity=0.7
            ))

        fig.add_trace(go.Scatter(
            x=price_range, y=total_payoff, mode='lines',
            name='Total Payoff', line=dict(color='white', width=3)
        ))
        fig.add_hline(y=0, line_dash='dash', line_color='red', opacity=0.5)
        fig.add_vline(x=current_price, line_dash='dash', line_color='cyan', opacity=0.5)

        fig.update_layout(
            title=f'Payoff Diagram - {symbol}',
            xaxis_title='Underlying Price',
            yaxis_title='Profit/Loss ($)',
            template='plotly_dark'
        )
        return fig

    def create_greeks_bar_figure(self, trade_structure: Dict):
        """Create Greeks bar chart as a Plotly figure and return it."""
        if not PLOTLY_AVAILABLE or not trade_structure:
            return None

        metrics = trade_structure.get('metrics', {})
        greek_names = ['Delta', 'Gamma', 'Vega', 'Theta']
        greek_values = [
            metrics.get('total_delta', 0),
            metrics.get('total_gamma', 0),
            metrics.get('total_vega', 0),
            metrics.get('total_theta', 0)
        ]
        colors = ['green' if v >= 0 else 'red' for v in greek_values]

        fig = go.Figure(data=[go.Bar(
            x=greek_names, y=greek_values,
            marker_color=colors
        )])
        fig.add_hline(y=0, line_color='white', opacity=0.3)
        fig.update_layout(
            title='Greeks Profile',
            yaxis_title='Greek Value',
            template='plotly_dark'
        )
        return fig

    def plot_trade_structure(self, trade_structure: Dict, symbol: str):
        """Plot trade structure payoff diagram"""
        if not trade_structure or not trade_structure.get('legs'):
            logger.warning("No trade structure to plot")
            return

        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        fig.suptitle(f'Trade Structure - {symbol}', fontsize=16)

        current_price = 100
        price_range = np.linspace(current_price * 0.7, current_price * 1.3, 100)

        legs = trade_structure['legs']
        total_payoff = np.zeros_like(price_range)
        colors = plt.cm.Set3(np.linspace(0, 1, len(legs)))

        for i, leg in enumerate(legs):
            strike = leg['strike']
            contracts = leg.get('contracts', 1)

            if leg['type'] == 'call':
                if leg['action'] == 'buy':
                    payoff = np.maximum(price_range - strike, 0) - leg.get('price', 0) * 100
                else:
                    payoff = leg.get('price', 0) * 100 - np.maximum(price_range - strike, 0)
            else:
                if leg['action'] == 'buy':
                    payoff = np.maximum(strike - price_range, 0) - leg.get('price', 0) * 100
                else:
                    payoff = leg.get('price', 0) * 100 - np.maximum(strike - price_range, 0)

            payoff *= contracts
            total_payoff += payoff
            axes[0].plot(price_range, payoff, color=colors[i], alpha=0.7,
                        label=f"{leg['action']} {leg['type']} {strike}")

        axes[0].plot(price_range, total_payoff, 'k-', linewidth=2, label='Total Payoff')
        axes[0].axhline(y=0, color='r', linestyle='--', alpha=0.5)
        axes[0].axvline(x=current_price, color='b', linestyle='--', alpha=0.5, label='Current Price')
        axes[0].set_title('Payoff Diagram')
        axes[0].set_xlabel('Underlying Price')
        axes[0].set_ylabel('Profit/Loss ($)')
        axes[0].legend()
        axes[0].grid(True)

        if trade_structure.get('metrics'):
            metrics = trade_structure['metrics']
            greek_names = ['Delta', 'Gamma', 'Vega', 'Theta']
            greek_values = [
                metrics.get('total_delta', 0),
                metrics.get('total_gamma', 0),
                metrics.get('total_vega', 0),
                metrics.get('total_theta', 0)
            ]
            bars = axes[1].bar(greek_names, greek_values)
            axes[1].set_title('Greeks Profile')
            axes[1].set_ylabel('Greek Value')
            axes[1].axhline(y=0, color='r', linestyle='-', alpha=0.5)
            axes[1].grid(True, axis='y')
            for bar, value in zip(bars, greek_values):
                if value < 0:
                    bar.set_color('red')
                else:
                    bar.set_color('green')

        plt.tight_layout()
        plt.show()
    
    def plot_backtest_results(self, backtest_results: Dict):
        """Plot backtest results"""
        trades = backtest_results.get('trades', [])
        if not trades:
            logger.warning("No trades to plot")
            return
        
        df = pd.DataFrame(trades)
        df['entry_date'] = pd.to_datetime(df['entry_date'])
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 10))
        fig.suptitle('Backtest Results', fontsize=16)
        
        # Cumulative PnL
        df['cumulative_pnl'] = df['pnl'].cumsum()
        axes[0, 0].plot(df['entry_date'], df['cumulative_pnl'])
        axes[0, 0].set_title('Cumulative P&L')
        axes[0, 0].set_xlabel('Date')
        axes[0, 0].set_ylabel('P&L ($)')
        axes[0, 0].grid(True)
        
        # Return distribution
        axes[0, 1].hist(df['return_pct'], bins=20, alpha=0.7)
        axes[0, 1].set_title('Return Distribution')
        axes[0, 1].set_xlabel('Return (%)')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].grid(True)
        
        # Win rate by type
        if 'type' in df.columns:
            win_rate_by_type = df.groupby('type')['win'].mean()
            axes[1, 0].bar(win_rate_by_type.index, win_rate_by_type.values)
            axes[1, 0].set_title('Win Rate by Trade Type')
            axes[1, 0].set_ylabel('Win Rate')
            axes[1, 0].tick_params(axis='x', rotation=45)
            axes[1, 0].grid(True)
        
        # Monthly returns
        df['month'] = df['entry_date'].dt.to_period('M')
        monthly_returns = df.groupby('month')['pnl'].sum()
        axes[1, 1].bar(range(len(monthly_returns)), monthly_returns.values)
        axes[1, 1].set_title('Monthly Returns')
        axes[1, 1].set_ylabel('P&L ($)')
        axes[1, 1].set_xticks(range(len(monthly_returns)))
        axes[1, 1].set_xticklabels([str(m) for m in monthly_returns.index], rotation=45)
        axes[1, 1].grid(True)
        
        plt.tight_layout()
        plt.show()
    
    def create_summary_report(self, results: List[Dict], backtest_results: Optional[Dict] = None):
        """Create summary report with visualizations"""
        if not results:
            logger.warning("No results for summary report")
            return
        
        # Create dashboard
        self.create_opportunity_dashboard(results)
        
        # If we have detailed results, plot individual analyses
        for result in results[:3]:  # Top 3 results
            if hasattr(result, 'skew_metrics'):
                self.plot_skew_analysis(result.skew_metrics, result.symbol)
            
            if hasattr(result, 'trade_structure'):
                self.plot_trade_structure(result.trade_structure, result.symbol)
        
        # Plot backtest results if available
        if backtest_results:
            self.plot_backtest_results(backtest_results)
    
    def save_plots(self, results: List[Dict], output_dir: str = 'plots'):
        """Save plots to files"""
        import os
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # This would save each plot as a file
        # Implementation depends on specific requirements
        logger.info(f"Plots would be saved to {output_dir}")