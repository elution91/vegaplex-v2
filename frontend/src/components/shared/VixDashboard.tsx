/**
 * VixDashboard — replaces the SynthesisCard prose wall with a high-density
 * quantitative dashboard: Arkham-style executive bar + metric grid.
 */
import Tooltip from './Tooltip'

// ── Types ──────────────────────────────────────────────────────────────────

type Metrics = {
  vix?: number
  vix3m?: number
  vix_ratio?: number
  carry_ratio?: number
  vrp?: number
  uvxy_monthly_cost?: number
  svxy_monthly_yield?: number
  vvix_vix_ratio?: number | null
  realized_vol?: number
  carry_on?: boolean
  allocation?: number
  allocation_label?: string
  as_of?: string
  low_vol_regime?: boolean
}

type Percentiles = {
  vix_ratio_pct?: number
  carry_ratio_pct?: number
  vix_pct?: number
  vrp_pct?: number
  vvix_vix_pct?: number
  n_obs?: number
}

type OutcomeBucket = {
  n?: number
  median?: number | null
  p25?: number | null
  p75?: number | null
  spike_pct?: number | null
}

type Outcomes = Record<string, OutcomeBucket>

interface Props {
  metrics:    Metrics
  percentiles: Percentiles
  outcomes:   Outcomes
}

// ── Helpers ─────────────────────────────────────────────────────────────────

function fmt(v: number | null | undefined, dec = 2): string {
  if (v == null || isNaN(v as number)) return '—'
  return (v as number).toFixed(dec)
}

function pctLabel(p: number | undefined): string {
  if (p == null) return '—'
  return `P${Math.round(p)}`
}

/** Pill for percentile — green low, yellow mid, red high (for risk metrics) */
function PctPill({ p, invert = false }: { p: number | undefined; invert?: boolean }) {
  if (p == null) return <span style={{ color: '#484f58', fontSize: 11 }}>—</span>
  const danger = invert ? p < 20 : p > 75
  const warn   = invert ? p < 35 : p > 50
  const color  = danger ? '#f85149' : warn ? '#e3b341' : '#3fb950'
  const bg     = danger ? 'rgba(248,81,73,0.15)' : warn ? 'rgba(227,179,65,0.13)' : 'rgba(63,185,80,0.13)'
  return (
    <span style={{
      display: 'inline-block', padding: '1px 6px', borderRadius: 4,
      fontSize: 11, fontWeight: 600, fontFamily: 'JetBrains Mono, monospace',
      color, background: bg,
    }}>
      {pctLabel(p)}
    </span>
  )
}

/** Thin horizontal bullet bar — current position in 0–100 range */
function BulletBar({ pct, color }: { pct: number | undefined; color: string }) {
  if (pct == null) return null
  const clamped = Math.max(0, Math.min(100, pct))
  return (
    <div style={{ width: 60, height: 4, background: '#21262d', borderRadius: 2, overflow: 'hidden', flexShrink: 0 }}>
      <div style={{ width: `${clamped}%`, height: '100%', background: color, borderRadius: 2 }} />
    </div>
  )
}

/** Single row of the metric grid */
function MetricRow({
  label, tip, value, unit = '', pct, invertPct = false, bullet, bulletColor = '#58a6ff', status,
}: {
  label: string; tip?: string; value: string; unit?: string
  pct?: number; invertPct?: boolean
  bullet?: number; bulletColor?: string
  status?: string
}) {
  return (
    <tr>
      <td style={{ padding: '5px 10px 5px 0', color: '#6e7681', fontSize: 12, whiteSpace: 'nowrap' }}>
        {tip ? <Tooltip text={tip} icon>{label}</Tooltip> : label}
      </td>
      <td style={{ padding: '5px 8px', textAlign: 'right' }}>
        <span style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 13, color: '#e6edf3' }}>
          {value}
        </span>
        {unit && <span style={{ fontSize: 11, color: '#6e7681', marginLeft: 2 }}>{unit}</span>}
      </td>
      <td style={{ padding: '5px 8px', textAlign: 'center' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, justifyContent: 'center' }}>
          <PctPill p={pct} invert={invertPct} />
          {bullet != null && <BulletBar pct={bullet} color={bulletColor} />}
        </div>
      </td>
      <td style={{ padding: '5px 0 5px 8px', fontSize: 11, color: '#6e7681', maxWidth: 180 }}>
        {status}
      </td>
    </tr>
  )
}

/** Section header row */
function SectionRow({ label }: { label: string }) {
  return (
    <tr>
      <td colSpan={4} style={{ padding: '10px 0 4px', fontSize: 10, fontWeight: 700,
        letterSpacing: '0.08em', textTransform: 'uppercase', color: '#484f58',
        borderBottom: '1px solid #21262d' }}>
        {label}
      </td>
    </tr>
  )
}

// ── Basis description ──────────────────────────────────────────────────────

function basisDesc(ratio: number | undefined): string {
  if (ratio == null) return '—'
  if (ratio < 85)  return 'Deep Contango'
  if (ratio < 92)  return 'Moderate Contango'
  if (ratio < 98)  return 'Shallow Contango'
  if (ratio < 100) return 'Near-Flat'
  return 'Backwardation'
}

function carryQuality(carry: number | undefined, pct: number | undefined): string {
  if (carry == null) return '—'
  if (carry < 0.85) return 'INSUFFICIENT'
  if (carry < 0.92) return 'Borderline'
  return 'Sufficient'
}

// ── Executive bar colors ───────────────────────────────────────────────────

const STATE_ON: React.CSSProperties = {
  background: 'rgba(63,185,80,0.12)', border: '1px solid rgba(63,185,80,0.3)',
  color: '#3fb950',
}
const STATE_OFF: React.CSSProperties = {
  background: 'rgba(248,81,73,0.12)', border: '1px solid rgba(248,81,73,0.3)',
  color: '#f85149',
}

// ── Main component ─────────────────────────────────────────────────────────

export default function VixDashboard({ metrics: m, percentiles: p, outcomes }: Props) {
  const carryOn   = m.carry_on ?? false
  const alloc     = m.allocation ?? 0
  const basis     = basisDesc(m.vix_ratio)
  const rvPct     = (m.realized_vol ?? 0) * 100
  const vrp       = m.vrp ?? 0

  // Pick the current carry bucket outcome (closest match)
  const carryR    = m.carry_ratio ?? 0
  let bucketKey   = '< 0.85 (backwardation)'
  if      (carryR >= 0.95) bucketKey = '≥ 0.95'
  else if (carryR >= 0.92) bucketKey = '0.92–0.95'
  else if (carryR >= 0.85) bucketKey = '0.85–0.92'
  const bucket: OutcomeBucket = outcomes?.[bucketKey] ?? {}

  return (
    <div className="space-y-3">

      {/* ── Executive status bar (Arkham-style) ─────────────────────── */}
      <div className="card" style={{ padding: '10px 16px', display: 'flex', alignItems: 'center', gap: 0, overflow: 'hidden' }}>

        {/* Strategy state pill */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, paddingRight: 24, borderRight: '1px solid #21262d', marginRight: 24, flexShrink: 0 }}>
          <div style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 11, color: '#484f58', letterSpacing: '0.07em', textTransform: 'uppercase' }}>
            Strategy
          </div>
          <div style={{
            ...(carryOn ? STATE_ON : STATE_OFF),
            borderRadius: 5, padding: '3px 12px',
            fontFamily: 'JetBrains Mono, monospace', fontSize: 14, fontWeight: 700, letterSpacing: '0.05em',
          }}>
            {carryOn ? 'CARRY ON' : 'CARRY OFF'}
          </div>
        </div>


        {/* Regime */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, paddingRight: 24, borderRight: '1px solid #21262d', marginRight: 24, flexShrink: 0 }}>
          <div style={{ fontSize: 11, color: '#484f58', letterSpacing: '0.07em', textTransform: 'uppercase' }}>
            Regime
          </div>
          <div style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 13, fontWeight: 600, color: '#e6edf3' }}>
            {basis}
            {m.low_vol_regime && (
              <span style={{ marginLeft: 8, fontSize: 10, color: '#e3b341', background: 'rgba(227,179,65,0.13)', padding: '1px 6px', borderRadius: 3 }}>
                Low-Vol
              </span>
            )}
          </div>
        </div>

        {/* VIX spot */}
        <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, paddingRight: 24, borderRight: '1px solid #21262d', marginRight: 24, flexShrink: 0 }}>
          <div style={{ fontSize: 11, color: '#484f58', letterSpacing: '0.07em', textTransform: 'uppercase' }}>
            VIX
          </div>
          <div style={{ fontFamily: 'JetBrains Mono, monospace', fontSize: 22, fontWeight: 700,
            color: (m.vix ?? 0) < 15 ? '#3fb950' : (m.vix ?? 0) <= 25 ? '#e3b341' : '#f85149' }}>
            {fmt(m.vix, 2)}
          </div>
          <PctPill p={p.vix_pct} />
        </div>

        {/* As-of */}
        {m.as_of && (
          <div style={{ marginLeft: 'auto', fontSize: 11, color: '#484f58' }}>
            as of {m.as_of}
          </div>
        )}
      </div>

      {/* ── Quantitative metric grid ─────────────────────────────────── */}
      <div className="card" style={{ padding: '10px 16px' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <colgroup>
            <col style={{ width: '22%' }} />
            <col style={{ width: '16%' }} />
            <col style={{ width: '18%' }} />
            <col style={{ width: '44%' }} />
          </colgroup>
          <thead>
            <tr>
              <th style={{ fontSize: 10, color: '#484f58', textAlign: 'left', padding: '0 0 6px', fontWeight: 600, letterSpacing: '0.07em', textTransform: 'uppercase' }}>
                Metric
              </th>
              <th style={{ fontSize: 10, color: '#484f58', textAlign: 'right', padding: '0 8px 6px', fontWeight: 600, letterSpacing: '0.07em', textTransform: 'uppercase' }}>
                Value
              </th>
              <th style={{ fontSize: 10, color: '#484f58', textAlign: 'center', padding: '0 8px 6px', fontWeight: 600, letterSpacing: '0.07em', textTransform: 'uppercase' }}>
                %ile
              </th>
              <th style={{ fontSize: 10, color: '#484f58', textAlign: 'left', padding: '0 0 6px 8px', fontWeight: 600, letterSpacing: '0.07em', textTransform: 'uppercase' }}>
                Context
              </th>
            </tr>
          </thead>
          <tbody>
            {/* ── Term Structure ── */}
            <SectionRow label="VIX Index Term Structure" />
            <MetricRow
              label="VIX / VIX3M (index ratio)"
              tip="VIX spot index ÷ VIX3M index × 100. Measures the shape of the CBOE volatility index family — not the tradeable futures strip. Below 92 = contango; above 100 = backwardation. Note: the futures strip (chart below) can show a different shape during vol spikes, as futures price expected settlement, not the current index level."
              value={fmt(m.vix_ratio, 3)}
              pct={p.vix_ratio_pct}
              bullet={p.vix_ratio_pct}
              bulletColor={(p.vix_ratio_pct ?? 0) > 75 ? '#f85149' : '#58a6ff'}
              status={basis + ((p.vix_ratio_pct ?? 0) > 75 ? ' — Caution Zone' : '')}
            />
            <MetricRow
              label="VIX3M"
              tip="3-month VIX futures level — the 90-day implied vol expectation used as carry baseline."
              value={fmt(m.vix3m, 2)}
              status="3-month futures level"
            />
            <MetricRow
              label="UVXY Roll Cost"
              tip="Estimated monthly roll loss for UVXY (2× long VIX ETF). High cost is a structural headwind for long-vol."
              value={fmt(m.uvxy_monthly_cost, 1)}
              unit="% / mo"
              status={(m.uvxy_monthly_cost ?? 0) < -15 ? 'High decay for long-vol' : 'Moderate decay'}
            />
            <MetricRow
              label="SVXY Roll Yield"
              tip="Estimated monthly roll income for SVXY (−0.5× VIX ETF). Positive = carry income for short-vol."
              value={fmt(m.svxy_monthly_yield, 1)}
              unit="% / mo"
              status={(m.svxy_monthly_yield ?? 0) > 1 ? 'Good carry accrual' : 'Low accrual'}
            />

            {/* ── Carry Quality ── */}
            <SectionRow label="Carry Quality" />
            <MetricRow
              label="Carry Ratio"
              tip="(VIX3M − VIX) / VIX3M. Measures the roll yield available from selling near-term vol. Below 0 = negative carry (backwardation). 'Sufficient' means positive carry exists but the filter also requires the VIX/VIX3M 10d ratio to be below 92 before turning ON."
              value={fmt(m.carry_ratio, 3)}
              pct={p.carry_ratio_pct}
              invertPct={true}
              bullet={p.carry_ratio_pct}
              bulletColor={(p.carry_ratio_pct ?? 50) < 25 ? '#f85149' : '#3fb950'}
              status={`${carryQuality(m.carry_ratio, p.carry_ratio_pct)} — Filter: ${carryOn ? 'ON' : 'OFF (10d ratio above 92)'}`}
            />
            <MetricRow
              label="Realized Vol (20d)"
              tip="20-day annualised realized volatility. High RV reduces the carry ratio and can turn the signal off."
              value={fmt(rvPct, 1)}
              unit="%"
              status={rvPct > 40 ? 'High — dampens carry signal' : rvPct > 20 ? 'Moderate' : 'Low'}
            />

            {/* ── Risk & VRP ── */}
            <SectionRow label="Risk & VRP" />
            <MetricRow
              label="Vol Risk Premium"
              tip="VIX − 20d Realized Vol. Positive = options priced above realized moves — core edge for short-vol. Negative = unusual, realized > implied (post-spike)."
              value={fmt(vrp, 1)}
              unit="pts"
              pct={p.vrp_pct}
              bullet={p.vrp_pct}
              bulletColor={vrp > 0 ? '#3fb950' : '#f85149'}
              status={vrp < 0 ? 'NEGATIVE — Realized > Implied' : vrp > 5 ? 'Options expensive vs realized' : 'Thin premium'}
            />
            <MetricRow
              label="VVIX / VIX"
              tip="Vol-of-vol relative to spot vol. Above 5 = elevated tail-risk pressure, mean-reversion of short-vol positions more likely."
              value={fmt(m.vvix_vix_ratio, 2)}
              pct={p.vvix_vix_pct}
              bullet={p.vvix_vix_pct}
              bulletColor={(p.vvix_vix_pct ?? 0) > 75 ? '#f85149' : '#58a6ff'}
              status={(m.vvix_vix_ratio ?? 0) > 5 ? 'Elevated tail-risk pressure' : 'Moderate vol-of-vol'}
            />

            {/* ── Probabilities ── */}
            {bucket.n != null && bucket.n > 0 && (
              <>
                <SectionRow label={`Historical Outcomes — ${bucketKey} carry bucket (N=${bucket.n})`} />
                <MetricRow
                  label="21d Return (median)"
                  tip={`Median 21-day SVXY forward return in this carry bucket. Range is P25–P75.`}
                  value={bucket.median != null ? (bucket.median > 0 ? '+' : '') + fmt(bucket.median, 1) : '—'}
                  unit="%"
                  status={bucket.p25 != null && bucket.p75 != null
                    ? `Range: [${bucket.p25 > 0 ? '+' : ''}${fmt(bucket.p25, 1)}%, ${bucket.p75 > 0 ? '+' : ''}${fmt(bucket.p75, 1)}%]`
                    : ''}
                />
                <MetricRow
                  label="30d VIX Spike Prob."
                  tip="Probability of a >30% VIX spike in the next 30 days, based on historical observations in this carry bucket."
                  value={bucket.spike_pct != null ? fmt(bucket.spike_pct, 0) : '—'}
                  unit="%"
                  status={
                    (bucket.spike_pct ?? 0) > 25 ? 'Elevated spike risk' :
                    (bucket.spike_pct ?? 0) > 15 ? 'Moderate spike risk' : 'Low spike risk'
                  }
                />
              </>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
