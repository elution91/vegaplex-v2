import { useState } from 'react'
import { useEarnings } from '../hooks/useEarnings'
import ProgressStream from '../components/shared/ProgressStream'
import MetricCard from '../components/shared/MetricCard'
import EmptyState from '../components/shared/EmptyState'

const COLUMNS = [
  { key: 'ticker',         label: 'Ticker' },
  { key: 'date',           label: 'Date' },
  { key: 'direction',      label: 'Dir' },
  { key: 'bennett_move',   label: 'Δ Move%' },
  { key: 'iv_rv_ratio',    label: 'IV/RV' },
  { key: 'spread_signal',  label: 'Spread' },
  { key: 'structure',      label: 'Rec' },
  { key: 'confidence',     label: 'Conf' },
]

export default function EarningsView() {
  const [daysAhead, setDaysAhead] = useState(7)
  const [minIvRv, setMinIvRv] = useState(0.8)
  const { rows, status, progress, errors, scan } = useEarnings()

  const handleScan = () => {
    scan({ days_ahead: daysAhead, min_iv_rv_ratio: minIvRv })
  }

  const richCount  = rows.filter((r) => String(r.richness ?? '').includes('RICH')).length
  const avgIvRv    = rows.length
    ? (rows.reduce((s, r) => s + (Number(r.iv_rv_ratio) || 0), 0) / rows.length).toFixed(2)
    : '—'

  return (
    <div className="space-y-4">
      {/* Controls */}
      <div className="flex items-center gap-3 flex-wrap">
        <h2 className="text-sm font-semibold text-text-primary">Earnings Scanner</h2>
        <label className="flex items-center gap-1 text-xs text-text-muted">
          Window
          <select
            value={daysAhead}
            onChange={(e) => setDaysAhead(Number(e.target.value))}
            className="ml-1 px-2 py-1 bg-bg-elevated border border-border rounded text-text-primary focus:outline-none focus:border-accent"
          >
            {[3, 5, 7, 10, 14].map((d) => (
              <option key={d} value={d}>{d}d</option>
            ))}
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

      {/* Progress */}
      <ProgressStream
        done={progress.done}
        total={progress.total}
        currentTicker={progress.currentTicker}
        errors={errors}
        visible={status === 'running'}
      />

      {/* Summary */}
      {rows.length > 0 && (
        <div className="grid grid-cols-4 gap-2">
          <MetricCard label="Events found" value={rows.length} />
          <MetricCard label="RICH" value={richCount} />
          <MetricCard label="Avg IV/RV" value={avgIvRv} />
          <MetricCard label="Status" value={status} />
        </div>
      )}

      {rows.length === 0 && status !== 'running' && (
        <EmptyState message="Set a window and click Scan Earnings" />
      )}

      {rows.length > 0 && (
        <div className="card overflow-auto max-h-[60vh]">
          <table className="vp-table">
            <thead>
              <tr>
                {COLUMNS.map((c) => <th key={c.key}>{c.label}</th>)}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr key={i}>
                  {COLUMNS.map((c) => (
                    <td key={c.key}>
                      {row[c.key] != null ? String(row[c.key]) : '—'}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
