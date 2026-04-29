import { useSettingsStore } from '../store/useSettingsStore'
import { NavLink } from 'react-router-dom'

export default function ResourcesView() {
  const t = useSettingsStore((s) => s.thresholds)
  return (
    <div className="space-y-4 max-w-5xl">
      <h2>Resources &amp; Documentation</h2>

      <div className="grid grid-cols-2 gap-4">
        {/* Left: Capabilities */}
        <div className="card p-4 space-y-4">
          <div className="section-title" style={{ borderBottom: '1px solid #21262d', paddingBottom: 8 }}>
            Capabilities
          </div>

          {([
            { icon: '◈', title: 'Regime Classification',
              text: 'Per-ticker vol regime detection (Sticky Strike, Sticky Delta, Local Vol, Jumpy Vol) grounded in Bennett\'s volatility framework. Warm-starts from 2-year historical context once seeded.' },
            { icon: '◈', title: 'Skew Surface Analysis',
              text: 'Full options surface decomposition: slope, 25Δ skew, curvature, term steepness, and forward skew extraction (Bennett pp.210–225). √T scaling to separate structural from noise.' },
            { icon: '◈', title: 'Cross-Ticker Skew Arb',
              text: 'SVI-normalised skew comparison across structurally related pairs (TQQQ/QQQ, UVXY/VIX, UPRO/SPY). Flags dislocations with historical reversion context.' },
            { icon: '◈', title: 'LETF Complex Radar',
              text: 'Universe-wide heatmap of IV percentile, IV–RV spread, skew persistence, and forward vol across the leveraged ETF complex and broader universe.' },
            { icon: '◈', title: 'VIX Term Structure',
              text: 'Live futures strip (IBKR → vixcentral fallback), carry ratio, roll cost, historical contango/backwardation regimes, and VRP outcomes analysis.' },
            { icon: '◈', title: 'Earnings Vol Patterns',
              text: 'Per-ticker historical IV expansion and crush across prior earnings cycles. Expected move vs implied move comparison with skew context.' },
          ] as { icon: string; title: string; text: string }[]).map((c) => (
            <div key={c.title}>
              <div className="flex items-center gap-2 mb-1">
                <span style={{ color: '#2DD4BF', fontSize: 13 }}>{c.icon}</span>
                <span style={{ color: '#2DD4BF', fontWeight: 600, fontSize: 12, letterSpacing: '0.05em', textTransform: 'uppercase' }}>{c.title}</span>
              </div>
              <p style={{ color: '#6e7681', fontSize: 12, lineHeight: 1.6, margin: 0, paddingLeft: 22 }}>{c.text}</p>
            </div>
          ))}
        </div>

        {/* Right: Methodology + Sources + Limitations */}
        <div className="space-y-4">
          <div className="card p-4 space-y-3">
            <div className="section-title" style={{ borderBottom: '1px solid #21262d', paddingBottom: 8 }}>
              Methodology
            </div>
            <p style={{ color: '#6e7681', fontSize: 12, lineHeight: 1.7, margin: 0 }}>
              Theoretical framework: Colin Bennett — <em>Trading Volatility</em> (2014).
              Regime classification, forward skew extraction, √T scaling, and stickiness
              measurement follow the analytical framework in chapters 4–7.
            </p>
          </div>

          <div className="card p-4 space-y-3">
            <div className="section-title" style={{ borderBottom: '1px solid #21262d', paddingBottom: 8 }}>
              Data Sources
            </div>
            {([
              { label: 'Options chains',     detail: 'yfinance (live, 15-min delayed)',           accent: false },
              { label: 'Price history',      detail: 'yfinance (daily OHLCV)',                    accent: false },
              { label: 'VIX futures strip',  detail: 'IBKR live → vixcentral fallback',           accent: true  },
              { label: 'Historical skew',    detail: 'Polygon.io flat files (2-year seed)',        accent: true  },
              { label: 'Earnings dates',     detail: 'yfinance calendar',                         accent: false },
            ] as { label: string; detail: string; accent: boolean }[]).map((s) => (
              <div key={s.label} className="flex items-baseline gap-2">
                <span style={{ color: '#e6edf3', fontSize: 12, fontWeight: 600, minWidth: 140 }}>{s.label}</span>
                <span style={{ color: s.accent ? '#2DD4BF' : '#6e7681', fontSize: 12 }}>{s.detail}</span>
              </div>
            ))}
          </div>

          <div className="card p-4 space-y-2">
            <div className="section-title" style={{ borderBottom: '1px solid #21262d', paddingBottom: 8 }}>
              Limitations
            </div>
            {[
              'Options data delayed ~15 minutes outside market hours',
              'Regime signals are informational, not trade recommendations',
              'Historical percentiles require seeded DB for full accuracy',
              'Cross-ticker arb signals most reliable within LETF complex',
            ].map((l) => (
              <div key={l} style={{ color: '#484f58', fontSize: 11 }}>· {l}</div>
            ))}
          </div>
        </div>
      </div>

      {/* Signal reference table */}
      <div className="card p-4">
        <div className="flex items-center justify-between mb-3">
          <div className="section-title">Signal Reference</div>
          <NavLink to="/settings" style={{ fontSize: 11, color: '#58a6ff' }}>Edit thresholds →</NavLink>
        </div>
        <table className="vp-table">
          <thead>
            <tr>
              <th>Signal</th>
              <th>Threshold</th>
              <th>Interpretation</th>
              <th>Source</th>
            </tr>
          </thead>
          <tbody>
            {([
              { signal: 'Confidence',   thresh: `≥ ${(t.confidence_high * 100).toFixed(0)}%`, interp: 'High conviction — green highlight',       source: 'Opportunity scanner' },
              { signal: 'Confidence',   thresh: `≥ ${(t.confidence_med  * 100).toFixed(0)}%`, interp: 'Medium conviction — yellow highlight',    source: 'Opportunity scanner' },
              { signal: 'R/R',          thresh: `≥ ${t.rr_excellent}`,   interp: 'Excellent risk/reward — green',           source: 'Trade structure' },
              { signal: 'R/R',          thresh: `≥ ${t.rr_acceptable}`,  interp: 'Acceptable risk/reward — yellow',         source: 'Trade structure' },
              { signal: 'IV/RV',        thresh: `≥ ${t.iv_rv_pass}`,     interp: 'Earnings pass gate — options expensive',  source: 'Earnings scanner' },
              { signal: 'IV/RV',        thresh: `≥ ${t.iv_rv_near_miss}`,interp: 'Near-miss — monitor',                    source: 'Earnings scanner' },
              { signal: 'RICH',         thresh: `> ${t.rich_threshold}`,  interp: 'Straddle > Bennett move — sell premium', source: 'Earnings scanner' },
              { signal: 'RICH',         thresh: `< ${t.cheap_threshold}`, interp: 'Straddle < Bennett move — buy premium',  source: 'Earnings scanner' },
              { signal: 'VIX / VIX3M', thresh: `< ${t.vix_ratio_carry_on}`, interp: 'Deep contango — carry-on zone',       source: 'VIX engine' },
              { signal: 'VIX / VIX3M', thresh: '> 1.0',                  interp: 'Backwardation — carry off, reduce risk', source: 'VIX engine' },
              { signal: 'Carry Ratio',  thresh: `≥ ${t.carry_ratio_min}`,interp: 'Sufficient carry — filter passes',        source: 'VIX engine' },
              { signal: 'VRP',          thresh: `> ${t.vrp_good} pts`,   interp: 'Options expensive vs realised — edge',   source: 'VIX engine' },
              { signal: 'VVIX / VIX',  thresh: `> ${t.vvix_vix_danger}`,interp: 'Tail-risk pressure — mean-reversion risk', source: 'VIX engine' },
            ] as { signal: string; thresh: string; interp: string; source: string }[]).map((r, i) => (
              <tr key={i}>
                <td style={{ fontWeight: 600, color: '#2DD4BF' }}>{r.signal}</td>
                <td style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 12 }}>{r.thresh}</td>
                <td style={{ color: '#8b949e' }}>{r.interp}</td>
                <td style={{ color: '#484f58', fontSize: 11 }}>{r.source}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <p style={{ fontSize: 11, color: '#30363d', textAlign: 'center' }}>
        νegaPlex — for informational purposes only. Not financial advice.
      </p>
    </div>
  )
}
