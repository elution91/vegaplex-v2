# Volatility Skew Arbitrage Scanner
# Inspired by the "Monk Pattern" - systematic skew/convexity arbitrage

import argparse
import sys
import json
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import Optional, Dict, List, Tuple, Any
from enum import Enum
import math
import logging

import pandas as pd
import numpy as np

# Import our modules
from data_fetcher import DataFetcher
from volatility_surface import VolatilitySurface
from skew_analyzer import SkewAnalyzer
from opportunity_scanner import OpportunityScanner
from trade_structures import TradeStructureBuilder
from backtester import StrategyBacktester
from visualizer import VolatilityVisualizer
from regime_classifier import RegimeClassifier
from skew_history import SkewHistory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataSource(str, Enum):
    IBKR = "ibkr"
    YFINANCE = "yfinance"


@dataclass
class ScannerConfig:
    """Configuration for the volatility scanner"""
    
    # Data source settings
    data_source: DataSource = DataSource.YFINANCE
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 7497
    ibkr_client_id: int = 1
    
    # Universe settings
    scan_sp500: bool = True
    scan_leveraged_etfs: bool = True
    scan_volatility_etfs: bool = True
    custom_symbols: List[str] = None
    
    # Skew detection thresholds
    min_skew_slope: float = 0.15  # Minimum skew slope to consider
    max_skew_slope: float = 2.0   # Maximum skew slope (avoid extremes)
    min_term_skew_diff: float = 0.10  # Minimum term structure difference
    min_convexity_score: float = 0.05  # Minimum convexity opportunity score
    
    # Trade structure settings
    max_delta_exposure: float = 0.30  # Maximum net delta per trade
    min_theta_ratio: float = -0.02   # Minimum theta/vega ratio (prefer positive theta)
    max_vega_exposure: float = 0.50  # Maximum vega exposure
    min_risk_reward: float = 2.0      # Minimum risk/reward ratio
    
    # Risk management
    max_positions: int = 5            # Maximum concurrent positions
    max_capital_per_trade: float = 0.20  # Maximum capital per trade
    stop_loss_vol: float = 0.30      # Stop loss on volatility move
    
    # Timing settings
    scan_frequency_minutes: int = 15  # How often to scan
    min_days_to_expiry: int = 7      # Minimum DTE for trades
    max_days_to_expiry: int = 60     # Maximum DTE for trades

    # Combo optimizer settings
    combo_max_legs: int = 6              # Maximum legs per combo structure
    combo_min_iv_edge: float = 0.005     # Minimum IV edge to qualify
    combo_min_richness: float = 0.3      # Minimum |richness_score| for candidates
    combo_liquidity_filter: bool = True  # Filter by OI and bid-ask spread
    combo_min_open_interest: int = 10    # Minimum OI per option
    combo_max_bid_ask_spread_pct: float = 0.40  # Max bid-ask as % of mid
    risk_free_rate: float = 0.05         # Risk-free rate for BS pricing


@dataclass
class ScanResult:
    """Result of a volatility scan"""

    timestamp: str
    symbol: str
    opportunity_type: str
    confidence_score: float
    skew_metrics: Dict[str, float]
    trade_structure: Dict[str, Any]
    risk_metrics: Dict[str, float]
    expected_pnl: Optional[float]
    max_loss: Optional[float]
    rationale: str
    regime_data: Optional[Dict[str, Any]] = None
    all_opportunities: Optional[List[Dict[str, Any]]] = None


class VolatilityScanner:
    """Main scanner class - orchestrates the entire process"""
    
    def __init__(self, config: ScannerConfig):
        self.config = config
        self.data_fetcher = DataFetcher(config)
        self.vol_surface = VolatilitySurface()
        self.skew_analyzer = SkewAnalyzer(config)
        self.opportunity_scanner = OpportunityScanner(config)
        self.trade_builder = TradeStructureBuilder(config)
        self.backtester = StrategyBacktester(config)
        self.visualizer = VolatilityVisualizer()
        self.regime_classifier = RegimeClassifier()
        self.skew_history = SkewHistory()

        # Results storage
        self.current_opportunities: List[ScanResult] = []
        self.historical_results: List[ScanResult] = []
        
    def initialize(self):
        """Initialize data connections and load required data"""
        logger.info("Initializing Volatility Scanner...")
        
        try:
            if not self.data_fetcher.connect():
                logger.error("Data fetcher failed to connect")
                return False
            logger.info("Data fetcher connected successfully")
            
            # Load universe
            symbols = self._get_scan_universe()
            logger.info(f"Loaded {len(symbols)} symbols for scanning")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize scanner: {e}")
            return False
    
    def _get_scan_universe(self) -> List[str]:
        """Get the list of symbols to scan"""
        symbols = []
        
        if self.config.scan_sp500:
            # Add major indices
            symbols.extend(['SPX', 'NDX', 'RUT'])
            
        if self.config.scan_leveraged_etfs:
            # Add leveraged ETFs (common skew distortions)
            symbols.extend([
                'TQQQ', 'SQQQ',  # 3x NASDAQ
                'UPRO', 'SPDN',  # 3x S&P
                'KOLD', 'BOIL',  # Inverse/regular Natural Gas
                'LABU', 'LABD',  # 3x Biotech
            ])
            
        if self.config.scan_volatility_etfs:
            # Add volatility products
            symbols.extend(['UVXY', 'SVXY', 'VXX'])
            
        if self.config.custom_symbols:
            symbols.extend(self.config.custom_symbols)
            
        return list(set(symbols))  # Remove duplicates
    
    def scan_market(self) -> List[ScanResult]:
        """Perform a full market scan"""
        logger.info("Starting market scan...")
        
        opportunities = []
        symbols = self._get_scan_universe()
        
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"Scanning {symbol} ({i}/{len(symbols)})...")
                result = self._scan_symbol(symbol)
                if result and result.confidence_score > 0.05:
                    opportunities.append(result)
                    logger.info(f"Found opportunity: {symbol} - {result.opportunity_type}")
                    
            except Exception as e:
                logger.warning(f"Failed to scan {symbol}: {e}")
                continue
        
        # Sort by confidence score
        opportunities.sort(key=lambda x: x.confidence_score, reverse=True)
        
        # Store results
        self.current_opportunities = opportunities
        self.historical_results.extend(opportunities)
        
        logger.info(f"Scan complete. Found {len(opportunities)} opportunities")
        return opportunities
    
    def _scan_symbol(self, symbol: str) -> Optional[ScanResult]:
        """Scan a single symbol for opportunities"""

        # Fetch options data
        options_data = self.data_fetcher.get_options_chain(symbol)
        if not options_data:
            return None

        # Build volatility surface
        vol_surface = self.vol_surface.build_surface(options_data)

        # Analyze skew
        skew_metrics = self.skew_analyzer.analyze_skew(vol_surface)

        # Classify regime (uses yfinance history internally)
        try:
            regime_data = self.regime_classifier.classify(symbol)
        except Exception as e:
            logger.warning(f"Regime classification failed for {symbol}: {e}")
            regime_data = None

        # Record skew snapshot for historical analysis
        try:
            self.skew_history.record(symbol, skew_metrics, vol_surface, regime_data)
        except Exception as e:
            logger.debug(f"Skew history record failed for {symbol}: {e}")

        # Fetch skew context (percentile data) for regime-aware opportunity detection
        try:
            skew_context = self.skew_history.get_context(symbol)
        except Exception:
            skew_context = None

        # Check for opportunities (regime-aware scoring) — returns full list
        opportunities = self.opportunity_scanner.find_opportunities(
            symbol, vol_surface, skew_metrics,
            regime_data=regime_data, skew_context=skew_context
        )

        if not opportunities:
            return None

        best = opportunities[0]

        # Build trade structure for best opportunity
        trade_structure = self.trade_builder.build_structure(
            best, vol_surface, options_data
        )

        # Calculate risk metrics
        risk_metrics = self._calculate_risk_metrics(trade_structure, vol_surface)

        return ScanResult(
            timestamp=datetime.now().isoformat(),
            symbol=symbol,
            opportunity_type=best['type'],
            confidence_score=best['confidence'],
            skew_metrics=skew_metrics,
            trade_structure=trade_structure,
            risk_metrics=risk_metrics,
            expected_pnl=best.get('expected_pnl'),
            max_loss=risk_metrics.get('max_loss'),
            rationale=best['rationale'],
            regime_data=regime_data,
            all_opportunities=opportunities,
        )
    
    def scan_symbol_full(self, symbol: str) -> Optional[Dict]:
        """Scan a symbol and return all intermediate results for dashboard use."""
        options_data = self.data_fetcher.get_options_chain(symbol)
        if not options_data:
            return None

        vol_surface = self.vol_surface.build_surface(options_data)
        skew_metrics = self.skew_analyzer.analyze_skew(vol_surface)

        # Classify regime
        try:
            regime_data = self.regime_classifier.classify(symbol)
        except Exception as e:
            logger.warning(f"Regime classification failed for {symbol}: {e}")
            regime_data = None

        # Record skew snapshot for historical analysis
        try:
            self.skew_history.record(symbol, skew_metrics, vol_surface, regime_data)
        except Exception as e:
            logger.debug(f"Skew history record failed for {symbol}: {e}")

        # Fetch skew context (percentile data) for regime-aware opportunity detection
        try:
            skew_context = self.skew_history.get_context(symbol)
        except Exception:
            skew_context = None

        all_opportunities = self.opportunity_scanner.find_opportunities(
            symbol, vol_surface, skew_metrics,
            regime_data=regime_data, skew_context=skew_context
        )

        best = all_opportunities[0] if all_opportunities else None

        trade_structure = None
        risk_metrics = None
        if best:
            trade_structure = self.trade_builder.build_structure(
                best, vol_surface, options_data
            )
            risk_metrics = self._calculate_risk_metrics(trade_structure, vol_surface)

        return {
            'options_data': options_data,
            'vol_surface': vol_surface,
            'skew_metrics': skew_metrics,
            'opportunity': best,
            'all_opportunities': all_opportunities,
            'trade_structure': trade_structure,
            'risk_metrics': risk_metrics,
            'regime_data': regime_data,
        }

    def _calculate_risk_metrics(self, trade_structure: Dict, vol_surface: Dict) -> Dict:
        """Calculate risk metrics for a trade structure"""
        
        # Extract Greeks
        total_delta = sum(leg.get('delta', 0) * leg.get('contracts', 0) 
                        for leg in trade_structure.get('legs', []))
        total_vega = sum(leg.get('vega', 0) * leg.get('contracts', 0) 
                        for leg in trade_structure.get('legs', []))
        total_theta = sum(leg.get('theta', 0) * leg.get('contracts', 0) 
                         for leg in trade_structure.get('legs', []))
        total_gamma = sum(leg.get('gamma', 0) * leg.get('contracts', 0) 
                         for leg in trade_structure.get('legs', []))
        
        # Calculate max loss (simplified - would need more sophisticated analysis)
        max_loss = abs(sum(leg.get('max_loss', 0) for leg in trade_structure.get('legs', [])))
        
        return {
            'total_delta': total_delta,
            'total_vega': total_vega,
            'total_theta': total_theta,
            'total_gamma': total_gamma,
            'max_loss': max_loss,
            'theta_vega_ratio': total_theta / total_vega if total_vega != 0 else 0,
            'delta_exposure': abs(total_delta),
        }
    
    def run_backtest(self, start_date: str, end_date: str) -> Dict:
        """Run backtest on historical data"""
        logger.info(f"Running backtest from {start_date} to {end_date}")
        
        return self.backtester.run_backtest(
            start_date=start_date,
            end_date=end_date,
            symbols=self._get_scan_universe()
        )
    
    def generate_report(self, results: List[ScanResult] = None) -> str:
        """Generate a comprehensive report of scan results"""
        if results is None:
            results = self.current_opportunities
        
        report = []
        report.append("=" * 80)
        report.append("VOLATILITY SKEW ARBITRAGE SCANNER REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Total Opportunities: {len(results)}")
        report.append("")
        
        for i, result in enumerate(results[:10], 1):  # Top 10
            report.append(f"{i}. {result.symbol} - {result.opportunity_type}")
            report.append(f"   Confidence: {result.confidence_score:.2f}")
            report.append(f"   Expected PnL: {result.expected_pnl:.2f}" if result.expected_pnl else "   Expected PnL: N/A")
            report.append(f"   Max Loss: {result.max_loss:.2f}" if result.max_loss else "   Max Loss: N/A")
            report.append(f"   Rationale: {result.rationale}")
            report.append("")
        
        return "\n".join(report)
    
    def visualize_opportunities(self, results: List[ScanResult] = None):
        """Create visualizations of current opportunities"""
        if results is None:
            results = self.current_opportunities
        
        self.visualizer.create_opportunity_dashboard(results)
    
    def continuous_scan(self):
        """Run continuous scanning"""
        logger.info("Starting continuous scanning mode...")
        
        while True:
            try:
                # Perform scan
                opportunities = self.scan_market()
                
                # Generate and display report
                report = self.generate_report(opportunities)
                print(report)
                
                # Create visualizations
                if opportunities:
                    self.visualize_opportunities(opportunities)
                
                # Wait for next scan
                logger.info(f"Waiting {self.config.scan_frequency_minutes} minutes for next scan...")
                import time
                time.sleep(self.config.scan_frequency_minutes * 60)
                
            except KeyboardInterrupt:
                logger.info("Stopping continuous scan...")
                break
            except Exception as e:
                logger.error(f"Error in continuous scan: {e}")
                import time
                time.sleep(60)  # Wait 1 minute before retrying


def main():
    parser = argparse.ArgumentParser(
        description='Volatility Skew Arbitrage Scanner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python vol_skew_scanner.py --scan                    # Single scan
  python vol_skew_scanner.py --continuous              # Continuous scanning
  python vol_skew_scanner.py --backtest 2023-01-01 2023-12-31  # Backtest
  python vol_skew_scanner.py --symbols SPX NDX VIX    # Custom symbols
        """
    )
    
    parser.add_argument('--scan', action='store_true',
                        help='Run single market scan')
    parser.add_argument('--continuous', action='store_true',
                        help='Run continuous scanning')
    parser.add_argument('--backtest', nargs=2, metavar=('START', 'END'),
                        help='Run backtest on historical data')
    parser.add_argument('--symbols', nargs='+',
                        help='Custom symbols to scan')
    parser.add_argument('--data-source', choices=['ibkr', 'yfinance'], default='yfinance',
                        help='Data source (default: yfinance)')
    parser.add_argument('--config', type=str,
                        help='Path to configuration file')
    parser.add_argument('--output', type=str,
                        help='Output file for results')
    parser.add_argument('--visualize', action='store_true',
                        help='Create visualizations')
    parser.add_argument('--dashboard', action='store_true',
                        help='Launch interactive Dash dashboard')
    parser.add_argument('--port', type=int, default=8050,
                        help='Dashboard port (default: 8050)')
    
    args = parser.parse_args()
    
    # Load configuration
    config = ScannerConfig()
    if args.config:
        with open(args.config, 'r') as f:
            config_dict = json.load(f)
            config = ScannerConfig(**config_dict)
    
    if args.symbols:
        config.custom_symbols = args.symbols
    
    config.data_source = DataSource(args.data_source)

    # Launch dashboard if requested (before scanner init so dashboard handles it)
    if args.dashboard:
        from dashboard import create_app
        app = create_app(config)
        logger.info(f"Starting dashboard on http://localhost:{args.port}")
        app.run(debug=False, port=args.port)
        return

    # Initialize scanner
    scanner = VolatilityScanner(config)

    if not scanner.initialize():
        logger.error("Failed to initialize scanner")
        sys.exit(1)

    try:
        if args.backtest:
            # Run backtest
            results = scanner.run_backtest(args.backtest[0], args.backtest[1])
            print(json.dumps(results, indent=2))
            
        elif args.continuous:
            # Continuous scanning
            scanner.continuous_scan()
            
        elif args.scan:
            # Single scan
            opportunities = scanner.scan_market()
            report = scanner.generate_report(opportunities)
            print(report)
            
            if args.visualize and opportunities:
                scanner.visualize_opportunities(opportunities)
            
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump([asdict(r) for r in opportunities], f, indent=2)
                logger.info(f"Results saved to {args.output}")
        
        else:
            # Default: single scan
            opportunities = scanner.scan_market()
            report = scanner.generate_report(opportunities)
            print(report)
            
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()