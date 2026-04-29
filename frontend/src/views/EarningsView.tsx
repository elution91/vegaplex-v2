import { useState } from 'react'
import { useLocation } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { useEarnings, EarningsRow } from '../hooks/useEarnings'
import ProgressStream from '../components/shared/ProgressStream'
import ChartCard from '../components/charts/ChartCard'
import MetricCard from '../components/shared/MetricCard'
import EmptyState from '../components/shared/EmptyState'
import Tooltip from '../components/shared/Tooltip'
import clsx from 'clsx'
import { getMacroEvents, type MacroEventRow } from '../api/macroEventsApi'

// ── Conditional formatting ──────────────────────────────────────────────────

function ivRvStyle(v: number) {
  if (v >= 1.25) return { color: '#3fb950', background: 'rgba(63,185,80,0.12)', fontWeight: 600 }
  if (v >= 1.0)  return { color: '#FACC15', background: 'rgba(250,204,21,0.10)' }
  return { color: '#6e7681' }
}

function richStyle(v: number | null | undefined) {
  if (v == null) return {}
  if (v > 1.10)  return { color: '#3fb950' }
  if (v < 0.90)  return { color: '#f85149' }
  return {}
}

function pctStyle(v: number, highGood = true) {
  if (v > 80) return highGood
    ? { color: '#3fb950', background: 'rgba(63,185,80,0.10)' }
    : { color: '#f85149', background: 'rgba(248,81,73,0.10)' }
  if (v < 20) return highGood
    ? { color: '#f85149' }
    : { color: '#3fb950' }
  return {}
}

function fmt(v: unknown, decimals = 2): string {
  if (v == null) return '—'
  const n = Number(v)
  return isNaN(n) ? String(v) : n.toFixed(decimals)
}

// ── Column definitions ──────────────────────────────────────────────────────

const COLUMNS = [
  { key: 'ticker',        label: 'Ticker' },
  { key: 'days',          label: 'Days',    title: 'Days until earnings' },
  { key: 'tier',          label: 'Tier',    title: 'Earnings importance tier' },
  { key: 'date',          label: 'Date' },
  { key: 'price',         label: 'Price' },
  { key: 'iv_rv_ratio',   label: 'IV/RV',   title: 'Implied vol / Realized vol ratio. Pass ≥ 1.25, near-miss ≥ 1.0' },
  { key: 'slope',         label: 'Slope',   title: 'Earnings-date skew slope' },
  { key: 'win_rate',      label: 'Win%',    title: 'Historical win rate for this structure' },
  { key: 'bennett_move',  label: 'Exp Δ',   title: 'Expected move delta (straddle-implied)' },
  { key: 'rich',          label: 'RICH',    title: 'Richness score (> 1.10 = rich, < 0.90 = cheap)' },
  { key: 'structure',     label: 'REC',     title: 'Recommended structure' },
  { key: 'spread_signal', label: 'Spread',  title: 'Spread signal indicator' },
]

// ── Iron fly drill-down ─────────────────────────────────────────────────────

function IronFlyDrillDown({ row, onClose }: { row: EarningsRow; onClose: () => void }) {
  const ironFly = row.iron_fly as Record<string, unknown> | undefined
  const payoff  = row.payoff_chart as Record<string, unknown> | undefined

  return (
    <div className="card p-3 space-y-3">
      <div className="flex items-center justify-between">
        <span className="text-sm font-semibold" style={{ color: '#2DD4BF' }}>
          {row.ticker} — Iron Fly
        </span>
        <button onClick={onClose} className="text-xs text-text-muted hover:text-text-primary">✕ Close</button>
      </div>

      <div className="flex gap-4">
        {/* Iron fly metrics table */}
        {ironFly && (
          <div className="flex-1 card overflow-auto">
            <table className="vp-table">
              <thead><tr><th>Metric</th><th>Value</th></tr></thead>
              <tbody>
                {Object.entries(ironFly).map(([k, v]) => (
                  <tr key={k}>
                    <td className="text-text-muted">{k.replace(/_/g, ' ')}</td>
                    <td>{String(v ?? '—')}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Payoff + gate thresholds */}
        <div className="flex-1 space-y-2">
          {payoff && <ChartCard option={payoff} height={200} />}
          <div className="card p-2 text-xs text-text-muted space-y-1">
            <div className="font-semibold text-text-primary text-xs">Gate Thresholds</div>
            <div>IV/RV pass: 1.25 | IV/RV near-miss: 1.0</div>
            {row.iv_rv_ratio != null && (
              <div>Current IV/RV: {fmt(row.iv_rv_ratio)}</div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

// ── Macro event vol table ──────────────────────────────────────────────────

const IMPACT_STARS: Record<number, string> = { 1: '●○○', 2: '●●○', 3: '●●●' }

function MacroEventTable() {
  const [daysAhead, setDaysAhead] = useState(60)
  const { data, isLoading } = useQuery({
    queryKey: ['macro-events', daysAhead],
    queryFn: () => getMacroEvents(daysAhead),
    staleTime: 15 * 60_000,
  })

  const rows = (data ?? []) as MacroEventRow[]

  return (
    <div className="card" style={{ padding: '12px 16px' }}>
      <div className="flex items-center gap-3 mb-3">
        <Tooltip text="Implied move extracted from SPX straddle prices bracketing each event date. Event vol isolates what the market prices for that specific day vs background vol. Hist Avg and RICH/CHEAP signal are normalised to the current VIX percentile regime — not unconditional averages." icon>
          <span className="section-title">SPX Macro Event Vol</span>
        </Tooltip>
        <select
          value={daysAhead}
          onChange={(e) => setDaysAhead(Number(e.target.value))}
          style={{ fontSize: 11 }}
          className="px-2 py-0.5 bg-bg-elevated rounded text-text-primary focus:outline-none"
        >
          <option value={30}>30d</option>
          <option value={60}>60d</option>
          <option value={90}>90d</option>
        </select>
        {isLoading && <span className="caption animate-pulse">Fetching SPX chains…</span>}
        {rows.length > 0 && rows[0].vix_regime && (
          <span style={{
            fontSize: 11, color: '#2DD4BF',
            background: 'rgba(45,212,191,0.10)',
            padding: '2px 8px', borderRadius: 4,
          }}>
            VIX regime: {rows[0].vix_regime}
          </span>
        )}
      </div>

      <table className="vp-table">
        <thead>
          <tr>
            <th>Event</th>
            <th>Date</th>
            <th>Days</th>
            <th>Impact</th>
            <th>
              <Tooltip text="Near expiry ATM IV — the expiry just after the event, capturing event risk." icon>Near IV</Tooltip>
            </th>
            <th>
              <Tooltip text="Far expiry ATM IV — post-event expiry, represents background vol." icon>Background IV</Tooltip>
            </th>
            <th>
              <Tooltip text="Isolated vol priced for the event day alone (annualised). Extracted by subtracting background variance from near-expiry total variance." icon>Event Vol</Tooltip>
            </th>
            <th>
              <Tooltip text="ATM straddle price ÷ spot. The market's priced move for the event day." icon>Impl. Move</Tooltip>
            </th>
            <th>
              <Tooltip text="Historical average SPX move on this event type (abs %), conditioned on the current VIX percentile regime. Higher VIX = higher expected move baseline." icon>Hist Avg</Tooltip>
            </th>
            <th>
              <Tooltip text="Implied move ÷ historical average − 1. RICH = implied > 20% above hist avg (sell vol). CHEAP = more than 20% below (buy vol)." icon>Signal</Tooltip>
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 && !isLoading && (
            <tr>
              <td colSpan={10} style={{ color: '#484f58', textAlign: 'center', padding: '16px 0' }}>
                No upcoming macro events in window
              </td>
            </tr>
          )}
          {rows.map((row, i) => (
            <tr key={i}>
              <td style={{ fontWeight: 700, color: '#e6edf3' }}>{row.event}</td>
              <td style={{ fontFamily: 'monospace', color: '#8b949e' }}>{row.date}</td>
              <td style={{ fontFamily: 'monospace' }}>{row.days}d</td>
              <td style={{ color: row.impact === 3 ? '#f85149' : row.impact === 2 ? '#FACC15' : '#8b949e', letterSpacing: 1 }}>
                {IMPACT_STARS[row.impact] ?? '—'}
              </td>
              <td style={{ fontFamily: 'monospace' }}>
                {row.error ? <span style={{ color: '#484f58' }}>—</span> : `${((row.near_iv ?? 0) * 100).toFixed(1)}%`}
              </td>
              <td style={{ fontFamily: 'monospace', color: '#8b949e' }}>
                {row.error ? '—' : `${((row.background_vol ?? 0) * 100).toFixed(1)}%`}
              </td>
              <td style={{ fontFamily: 'monospace', color: '#2DD4BF' }}>
                {row.error ? '—' : row.event_vol ? `${(row.event_vol * 100).toFixed(1)}%` : '—'}
              </td>
              <td style={{ fontFamily: 'monospace', fontWeight: 600, color: '#e6edf3' }}>
                {row.error ? <span style={{ color: '#484f58', fontSize: 10 }}>{row.error}</span>
                  : row.implied_move_pct != null ? `±${row.implied_move_pct.toFixed(2)}%` : '—'}
              </td>
              <td style={{ fontFamily: 'monospace', color: '#484f58' }}>
                {row.hist_avg_move != null ? `±${row.hist_avg_move.toFixed(2)}%` : '—'}
              </td>
              <td>
                {row.signal && row.signal !== '—' ? (
                  <span style={{
                    color: row.signal_color,
                    background: row.signal_color + '18',
                    padding: '1px 7px', borderRadius: 3,
                    fontWeight: 600, fontSize: 11,
                  }}>
                    {row.signal}
                  </span>
                ) : '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Main view ───────────────────────────────────────────────────────────────

type EventsTab = 'earnings' | 'macro'

export default function EarningsView() {
  const location = useLocation()
  const evTab: EventsTab = location.pathname.endsWith('macro') ? 'macro' : 'earnings'
  const [daysAhead, setDaysAhead]     = useState(14)
  const [minIvRv,   setMinIvRv]       = useState(0.8)
  const [dataSource, setDataSource]   = useState<'yfinance' | 'ibkr'>('yfinance')
  const [selectedRow, setSelectedRow] = useState<EarningsRow | null>(null)
  const { rows, status, progress, errors, scan } = useEarnings()

  const handleScan = () => {
    setSelectedRow(null)
    scan({ days_ahead: daysAhead, min_iv_rv_ratio: minIvRv, data_source: dataSource })
  }

  const richCount = rows.filter((r) => Number(r.rich) > 1.10).length
  const avgIvRv   = rows.length
    ? (rows.reduce((s, r) => s + (Number(r.iv_rv_ratio) || 0), 0) / rows.length).toFixed(2)
    : '—'

  return (
    <div className="space-y-2">

      {/* ── MACRO EVENT VOL TAB ── */}
      {evTab === 'macro' && <MacroEventTable />}

      {/* ── EARNINGS TAB ── */}
      {evTab === 'earnings' && <div className="space-y-2">
      {/* Controls */}
      <div className="flex items-center gap-3 flex-wrap">

        {/* Data source */}
        <div className="flex items-center gap-1 text-xs text-text-muted">
          {(['yfinance', 'ibkr'] as const).map((ds) => (
            <label key={ds} className="flex items-center gap-1 cursor-pointer">
              <input
                type="radio"
                name="data-source"
                value={ds}
                checked={dataSource === ds}
                onChange={() => setDataSource(ds)}
                className="accent-accent"
              />
              {ds === 'yfinance' ? 'Auto (yfinance)' : 'IBKR TWS'}
            </label>
          ))}
        </div>

        <label className="flex items-center gap-1 text-xs text-text-muted">
          Window
          <select
            value={daysAhead}
            onChange={(e) => setDaysAhead(Number(e.target.value))}
            className="ml-1 px-2 py-1 bg-bg-elevated border border-border rounded text-text-primary focus:outline-none focus:border-accent"
          >
            {[3, 5, 7, 10, 14, 21, 30].map((d) => <option key={d} value={d}>{d}d</option>)}
          </select>
        </label>

        <label className="flex items-center gap-1 text-xs text-text-muted">
          Min IV/RV
          <input
            type="number"
            value={minIvRv}
            step={0.05}
            min={0}
            max={5}
            onChange={(e) => setMinIvRv(Number(e.target.value))}
            className="w-16 ml-1 px-2 py-1 bg-bg-elevated border border-border rounded
                       text-text-primary focus:outline-none focus:border-accent"
          />
        </label>

        <button
          onClick={handleScan}
          disabled={status === 'running'}
          className="nav-tab-btn disabled:opacity-40"
        >
          {status === 'running' ? 'Scanning…' : 'Scan Earnings'}
        </button>
      </div>

      <ProgressStream
        done={progress.done}
        total={progress.total}
        currentTicker={progress.currentTicker}
        errors={errors}
        visible={status === 'running'}
      />

      {/* Summary cards */}
      {rows.length > 0 && (
        <div className="grid grid-cols-4 gap-2">
          <MetricCard label="Events found" value={rows.length} />
          <MetricCard label="RICH" value={richCount} />
          <MetricCard label="Avg IV/RV" value={avgIvRv} />
          <MetricCard label="Status" value={status} />
        </div>
      )}

      {rows.length === 0 && status === 'idle' && (
        <EmptyState message="Set a window and click Scan Earnings" />
      )}
      {rows.length === 0 && status === 'complete' && (
        <EmptyState message={`No earnings setups found in the next ${daysAhead} days. The scanner targets companies reporting at the end of the window — try a wider window (21d or 30d) or check back closer to earnings season.`} />
      )}

      {/* Iron fly drill-down */}
      {selectedRow && (
        <IronFlyDrillDown row={selectedRow} onClose={() => setSelectedRow(null)} />
      )}

      {/* Main table */}
      {rows.length > 0 && (
        <div className="card overflow-auto max-h-[calc(100vh-300px)]">
          <table className="vp-table">
            <thead>
              <tr>
                {COLUMNS.map((c) => (
                  <th key={c.key}>
                    {c.title
                      ? <Tooltip text={c.title} icon>{c.label}</Tooltip>
                      : c.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => {
                const ivRv    = Number(row.iv_rv_ratio) || 0
                const richVal = row.rich != null ? Number(row.rich) : null
                const winRate = Number(row.win_rate) || 0
                const isSelected = selectedRow === row
                return (
                  <tr
                    key={i}
                    onClick={() => setSelectedRow(isSelected ? null : row)}
                    className="cursor-pointer"
                    style={isSelected ? { background: 'rgba(88,166,255,0.08)' } : undefined}
                  >
                    <td style={{ color: '#2DD4BF', fontWeight: 600 }}>{row.ticker}</td>
                    <td>{row.days != null ? String(row.days) : '—'}</td>
                    <td>{row.tier != null ? String(row.tier) : '—'}</td>
                    <td>{String(row.date ?? '—')}</td>
                    <td>{row.price != null ? `$${fmt(row.price)}` : '—'}</td>
                    <td>
                      <span style={{ ...ivRvStyle(ivRv), padding: '1px 5px', borderRadius: 3 }}>
                        {fmt(row.iv_rv_ratio)}
                      </span>
                    </td>
                    <td>{fmt(row.slope, 3)}</td>
                    <td style={pctStyle(winRate)}>{winRate > 0 ? winRate.toFixed(0) + '%' : '—'}</td>
                    <td>{fmt(row.bennett_move, 1)}%</td>
                    <td style={richStyle(richVal)}>{fmt(row.rich, 3)}</td>
                    <td>{row.structure != null ? String(row.structure) : '—'}</td>
                    <td className={clsx(
                      row.spread_signal === 'WIDE' ? 'text-negative' :
                      row.spread_signal === 'TIGHT' ? 'text-positive' : ''
                    )}>
                      {row.spread_signal != null ? String(row.spread_signal) : '—'}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      </div>}
    </div>
  )
}
