import { useNavigate } from 'react-router-dom'
import { useScanStore, Opportunity } from '../store/useScanStore'
import { useSettingsStore } from '../store/useSettingsStore'
import Tooltip from '../components/shared/Tooltip'
import ProgressStream from '../components/shared/ProgressStream'
import ChartCard from '../components/charts/ChartCard'
import MetricCard from '../components/shared/MetricCard'

// ── Conditional formatting — thresholds injected at render time ────────────

function confStyle(conf: number, high: number, med: number) {
  if (conf >= high) return { color: '#3fb950', background: 'rgba(63,185,80,0.12)', fontWeight: 600 }
  if (conf >= med)  return { color: '#FACC15', background: 'rgba(250,204,21,0.10)' }
  return { color: '#6e7681' }
}

function rrStyle(rr: number, excellent: number, acceptable: number) {
  if (rr >= excellent)  return { color: '#3fb950', background: 'rgba(63,185,80,0.12)', fontWeight: 600 }
  if (rr >= acceptable) return { color: '#FACC15', background: 'rgba(250,204,21,0.10)' }
  return { color: '#6e7681' }
}

function greekStyle(bias: string | undefined, aligned: string | undefined) {
  if (aligned === 'False') return { color: '#d29922', fontWeight: 600 }
  if (bias === 'short')    return { color: '#00d4aa' }
  if (bias === 'long')     return { color: '#58a6ff' }
  return { color: '#8b949e' }
}

function legStyle(action: string) {
  return action === 'BUY'
    ? { color: '#3fb950' }
    : { color: '#f85149' }
}

function richStyle(rich: number | undefined) {
  if (rich == null) return {}
  if (rich > 0.05)  return { color: '#3fb950' }
  if (rich < -0.05) return { color: '#f85149' }
  return {}
}

function fmt(v: unknown, decimals = 2): string {
  if (v == null) return '—'
  const n = Number(v)
  return isNaN(n) ? String(v) : n.toFixed(decimals)
}

// ── Column definitions ─────────────────────────────────────────────────────

const COLS = [
  { key: 'symbol',     label: 'Symbol',     tip: '' },
  { key: 'type',       label: 'Type',       tip: '' },
  { key: 'subtype',    label: 'Subtype',    tip: '' },
  { key: 'confidence', label: 'Confidence', tip: 'Model confidence score (0–100%). ≥70% = high (green), ≥50% = medium (yellow).' },
  { key: 'max_gain',   label: 'Max Gain',   tip: 'Maximum profit at expiry.' },
  { key: 'max_loss',   label: 'Max Loss',   tip: 'Maximum loss at expiry (worst case).' },
  { key: 'rr',         label: 'R/R',        tip: 'Risk/reward ratio: Max Gain ÷ Max Loss. ≥3 = excellent, ≥1.5 = acceptable.' },
  { key: 'greeks',     label: 'Greeks',     tip: 'Net Greek exposure. Teal = short vega, blue = long vega, amber = misaligned.' },
  { key: 'regime',     label: 'Regime',     tip: 'Vol regime: Sticky Delta, Sticky Strike, Local Vol, or Jumpy.' },
  { key: 'rationale',  label: 'Rationale',  tip: '' },
]

const LEG_COLS = [
  { key: 'action', label: 'Action' },
  { key: 'type',   label: 'Type' },
  { key: 'strike', label: 'Strike' },
  { key: 'expiry', label: 'Expiry' },
  { key: 'qty',    label: 'Qty' },
  { key: 'price',  label: 'Price' },
  { key: 'delta',  label: 'Delta' },
  { key: 'vega',   label: 'Vega' },
  { key: 'theta',  label: 'Theta' },
  { key: 'iv',     label: 'IV' },
  { key: 'rich',   label: 'Rich' },
]

// ── Main view ──────────────────────────────────────────────────────────────

export default function ResultsView() {
  const navigate = useNavigate()
  const { status, progress, results, errors, selectedOpportunity, selectOpportunity } = useScanStore()
  const t = useSettingsStore((s) => s.thresholds)
  const allOpps: Opportunity[] = results.flatMap((r) => r.opportunities ?? [])
  const scannedSymbols = results.map((r) => r.symbol).filter(Boolean)

  const totalOpps = allOpps.length
  const avgConf   = totalOpps ? (allOpps.reduce((s, o) => s + (o.confidence ?? 0), 0) / totalOpps).toFixed(2) : '—'
  const avgRR     = totalOpps ? (allOpps.reduce((s, o) => s + (o.rr ?? 0), 0) / totalOpps).toFixed(2) : '—'
  const highConf  = allOpps.filter((o) => (o.confidence ?? 0) >= t.confidence_high).length

  return (
    <div className="space-y-2">
      <ProgressStream
        done={progress.done}
        total={progress.total}
        currentTicker={progress.currentTicker}
        errors={errors}
        visible={status === 'running'}
      />

      {/* Summary metric cards */}
      <div className="grid grid-cols-4 gap-2">
        <MetricCard label="Opportunities"   value={totalOpps || '—'} />
        <MetricCard label="High Confidence" value={highConf  || '—'} />
        <MetricCard label="Avg Confidence"  value={avgConf} />
        <MetricCard label="Avg R/R"         value={avgRR} />
      </div>

      {totalOpps === 0 && status !== 'running' && (
        status === 'complete' && scannedSymbols.length > 0 ? (
          // Scan ran but found no qualifying opportunities — explain why.
          <div className="card" style={{ padding: 20, lineHeight: 1.55 }}>
            <div style={{ fontSize: 14, fontWeight: 600, color: '#e6edf3', marginBottom: 6 }}>
              No qualifying opportunities found for {scannedSymbols.slice(0, 5).join(', ')}
              {scannedSymbols.length > 5 ? ` + ${scannedSymbols.length - 5} more` : ''}
            </div>
            <div style={{ fontSize: 12, color: '#8b949e', marginBottom: 12 }}>
              The scanner is conservative by design. To clear the gate, an opportunity
              must satisfy <em>all</em> of these:
            </div>
            <ul style={{ fontSize: 12, color: '#8b949e', paddingLeft: 18, marginBottom: 14, listStyle: 'disc' }}>
              <li>Risk/reward ratio ≥ <strong style={{ color: '#e6edf3' }}>2.0</strong> — typical edge cases sit at 1.2–1.8</li>
              <li>Strategy must match the active <strong style={{ color: '#e6edf3' }}>regime</strong> (e.g. long skew only emits in Jumpy Vol)</li>
              <li>Positive expected P&amp;L and finite max loss</li>
            </ul>
            <div style={{ fontSize: 12, color: '#6e7681', marginBottom: 12 }}>
              In quiet markets (low VIX, calm regimes) most tickers naturally produce zero hits.
              This isn't a bug — it's the scanner saying "nothing edgy is priced today."
            </div>
            <button
              onClick={() => navigate('/settings')}
              style={{
                fontSize: 12, padding: '5px 12px',
                background: '#1f6feb', color: '#e6edf3',
                border: 'none', borderRadius: 4, cursor: 'pointer',
              }}
            >
              Adjust thresholds
            </button>
          </div>
        ) : (
          // Idle / no scan yet
          <div className="card" style={{ padding: 20, color: '#8b949e', fontSize: 13 }}>
            Scan a symbol via the navbar search, or click <strong>Scan Universe</strong> on the Radar page.
          </div>
        )
      )}

      {totalOpps > 0 && (
        <div className="space-y-2">
          {/* Full-width opportunities table */}
          <div className="card overflow-auto" style={{ maxHeight: selectedOpportunity ? 'calc(50vh - 60px)' : 'calc(100vh - 200px)' }}>
            <table className="vp-table">
              <thead>
                <tr>
                  {COLS.map((c) => (
                    <th key={c.key}>
                      {c.tip ? <Tooltip text={c.tip} icon>{c.label}</Tooltip> : c.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {allOpps.map((opp, i) => {
                  const conf = opp.confidence ?? 0
                  const rr   = opp.rr ?? 0
                  const isSelected = selectedOpportunity === opp
                  return (
                    <tr
                      key={i}
                      onClick={() => selectOpportunity(isSelected ? null : opp)}
                      className="cursor-pointer"
                      style={isSelected ? { background: 'rgba(88,166,255,0.07)' } : undefined}
                    >
                      <td style={{ color: '#2DD4BF', fontWeight: 600 }}>{opp.symbol}</td>
                      <td>{opp.type ?? '—'}</td>
                      <td>{(opp.subtype as string) ?? '—'}</td>
                      <td>
                        <span style={{ ...confStyle(conf, t.confidence_high, t.confidence_med), padding: '1px 6px', borderRadius: 3 }}>
                          {(conf * 100).toFixed(0)}%
                        </span>
                      </td>
                      <td style={{ color: '#3fb950' }}>{fmt(opp.max_gain)}</td>
                      <td style={{ color: '#f85149' }}>{fmt(opp.max_loss)}</td>
                      <td>
                        <span style={{ ...rrStyle(rr, t.rr_excellent, t.rr_acceptable), padding: '1px 6px', borderRadius: 3 }}>
                          {rr.toFixed(2)}
                        </span>
                      </td>
                      <td style={greekStyle(opp._vega_bias as string, opp._greek_aligned as string)}>
                        {(opp.greeks as string) ?? '—'}
                      </td>
                      <td>{(opp.regime as string) ?? '—'}</td>
                      <td style={{ color: '#8b949e', maxWidth: 280 }} className="truncate">
                        {(opp.rationale as string) ?? '—'}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          {/* Drill-down — below the table, full width */}
          {selectedOpportunity && (
            <DrillDown opp={selectedOpportunity} confHigh={t.confidence_high} confMed={t.confidence_med} rrExcellent={t.rr_excellent} rrAcceptable={t.rr_acceptable} />
          )}
        </div>
      )}
    </div>
  )
}

// ── Drill-down panel (full width, below table) ─────────────────────────────

function DrillDown({ opp, confHigh, confMed, rrExcellent, rrAcceptable }: {
  opp: Opportunity
  confHigh: number
  confMed: number
  rrExcellent: number
  rrAcceptable: number
}) {
  const payoff = opp.payoff_chart as Record<string, unknown> | undefined
  const m      = opp.metrics     as Record<string, number>  | undefined

  const conf = (opp.confidence ?? 0) as number

  return (
    <div className="card" style={{ padding: '14px 16px' }}>
      {/* ── Header row ──────────────────────────────────────────────── */}
      <div className="flex items-center gap-3 mb-3" style={{ borderBottom: '1px solid #21262d', paddingBottom: 10 }}>
        <span style={{ color: '#2DD4BF', fontWeight: 700, fontSize: 14 }}>{opp.symbol}</span>
        <span className="caption">
          {opp.type}{opp.subtype ? ` / ${String(opp.subtype)}` : ''}
        </span>
        <span style={{ ...confStyle(conf, confHigh, confMed), padding: '1px 7px', borderRadius: 3, fontSize: 12 }}>
          {(conf * 100).toFixed(0)}% conf
        </span>
        <span style={{ ...rrStyle((opp.rr ?? 0) as number, rrExcellent, rrAcceptable), padding: '1px 7px', borderRadius: 3, fontSize: 12 }}>
          R/R {((opp.rr ?? 0) as number).toFixed(2)}
        </span>
        <span style={{ color: '#3fb950', fontSize: 13, fontWeight: 600, marginLeft: 8 }}>
          Max: {fmt(opp.max_gain as number)}
        </span>
        <span style={{ color: '#f85149', fontSize: 13, fontWeight: 600 }}>
          Loss: {fmt(opp.max_loss as number)}
        </span>
      </div>

      {/* ── Content: 3-column grid ───────────────────────────────────── */}
      <div className="grid grid-cols-3 gap-4">

        {/* Col 1: Greeks + rationale */}
        <div className="space-y-2">
          {m && (
            <div>
              <div className="section-title mb-2">Greeks</div>
              <div className="grid grid-cols-2 gap-1.5">
                {[
                  { label: 'Δ Delta', val: m.total_delta, dec: 3 },
                  { label: 'ν Vega',  val: m.total_vega,  dec: 3 },
                  { label: 'θ Theta', val: m.total_theta, dec: 3 },
                  { label: 'γ Gamma', val: m.total_gamma, dec: 4 },
                ].map(({ label, val, dec }) => (
                  <div key={label} style={{ background: '#1c2128', borderRadius: 5, padding: '6px 10px' }}>
                    <div className="caption">{label}</div>
                    <div className="metric-value" style={{
                      fontSize: 14,
                      color: val == null ? '#6e7681' : val > 0 ? '#3fb950' : val < 0 ? '#f85149' : '#6e7681',
                    }}>
                      {val != null ? val.toFixed(dec) : '—'}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {opp.rationale != null && (
            <div>
              <div className="section-title mb-1">Rationale</div>
              <p style={{ fontSize: 12, color: '#8b949e', lineHeight: 1.6, margin: 0 }}>
                {String(opp.rationale)}
              </p>
            </div>
          )}
        </div>

        {/* Col 2: Payoff chart */}
        <div>
          <div className="section-title mb-2">Payoff at Expiry</div>
          {payoff
            ? <ChartCard option={payoff} height={220} />
            : <div className="skeleton" style={{ height: 220, borderRadius: 6 }} />
          }
        </div>

        {/* Col 3: Legs table */}
        <div>
          <div className="section-title mb-2">Legs</div>
          {opp.legs && opp.legs.length > 0 ? (
            <div style={{ overflowX: 'auto' }}>
              <table className="vp-table">
                <thead>
                  <tr>{LEG_COLS.map((c) => <th key={c.key}>{c.label}</th>)}</tr>
                </thead>
                <tbody>
                  {opp.legs.map((leg, i) => {
                    const r = leg as unknown as Record<string, unknown>
                    return (
                      <tr key={i}>
                        <td style={legStyle(leg.action)}>{leg.action}</td>
                        <td>{leg.type}</td>
                        <td>${leg.strike}</td>
                        <td>{leg.expiry}</td>
                        <td>{(r.qty as number) ?? (r.contracts as number) ?? 1}</td>
                        <td>${leg.price?.toFixed(2) ?? '—'}</td>
                        <td>{fmt(r.delta, 3)}</td>
                        <td>{fmt(r.vega, 3)}</td>
                        <td>{fmt(r.theta, 3)}</td>
                        <td>{fmt(r.iv as unknown, 3)}</td>
                        <td style={richStyle(r.rich as number)}>{fmt(r.rich, 3)}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="caption">No legs data</p>
          )}
        </div>
      </div>
    </div>
  )
}
