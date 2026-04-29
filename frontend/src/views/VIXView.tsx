import { useState } from 'react'
import { useLocation } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { useVix } from '../hooks/useVix'
import { getVixSnapshot } from '../api/vixApi'
import ChartCard from '../components/charts/ChartCard'
import MetricCard from '../components/shared/MetricCard'
import EmptyState from '../components/shared/EmptyState'
import Tooltip from '../components/shared/Tooltip'
import VixDashboard from '../components/shared/VixDashboard'

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
  low_vol_regime?: boolean
  as_of?: string
}

type TransitionMatrix = {
  buckets: string[]
  current_bucket: string
  matrix: Record<string, Record<string, number>>
  n_obs: Record<string, number>
}

type ChartStats = { current?: string; mean?: string; std?: string }

type VixData = {
  metrics?: Metrics
  percentiles?: Record<string, number>
  outcomes?: Record<string, unknown>
  transitions?: TransitionMatrix
  synthesis?: string
  charts?: Record<string, Record<string, unknown>>
  chart_stats?: Record<string, ChartStats>
}

type SubTab = 'status' | 'history' | 'analytics'

const SUB_TABS: { id: SubTab; label: string }[] = [
  { id: 'status',    label: 'Status' },
  { id: 'history',   label: 'Time Series' },
  { id: 'analytics', label: 'Analytics' },
]

function fmt(v: number | null | undefined, decimals = 2): string {
  if (v == null) return '—'
  return v.toFixed(decimals)
}

// Build a [Current, Mean, Std] stats array for ChartCard footer
function statsRow(s: ChartStats | undefined): { label: string; value: string }[] | undefined {
  if (!s || !s.current) return undefined
  const out = [{ label: 'Current', value: s.current }]
  if (s.mean) out.push({ label: 'Mean', value: s.mean })
  if (s.std)  out.push({ label: 'Std Dev', value: s.std })
  return out
}

// Find the closest data point to a target date in [[date, value], ...] series data
function findValueAtDate(data: unknown, targetTs: number): number | null {
  if (!Array.isArray(data) || data.length === 0) return null
  let best: [number, number] | null = null
  let bestDelta = Infinity
  for (const point of data) {
    if (!Array.isArray(point) || point.length < 2) continue
    const ts = new Date(point[0] as string | number).getTime()
    const val = Number(point[1])
    if (isNaN(ts) || isNaN(val)) continue
    const delta = Math.abs(ts - targetTs)
    if (delta < bestDelta) {
      bestDelta = delta
      best = [ts, val]
    }
  }
  return best ? best[1] : null
}

// Inject a dataZoom + optional snapshot-date annotation (markLine + markPoint).
// When `markDate` is provided, draws a vertical line at that date and labels
// each series with its value — captured by exports, no hover required.
function withDateZoom(
  option: Record<string, unknown> | null,
  from: string,
  to: string,
  markDate?: string,
): Record<string, unknown> | null {
  if (!option) return option
  const startTs = from ? new Date(from).getTime() : NaN
  const endTs   = to   ? new Date(to).getTime()   : NaN
  const markTs  = markDate ? new Date(markDate).getTime() : NaN
  const haveZoom = !isNaN(startTs) && !isNaN(endTs) && endTs > startTs
  const haveMark = !isNaN(markTs)
  if (!haveZoom && !haveMark) return option

  const next: Record<string, unknown> = { ...option }

  if (haveZoom) {
    next.dataZoom = [
      { type: 'inside', startValue: startTs, endValue: endTs, filterMode: 'none' },
    ]
  }

  if (haveMark && Array.isArray(option.series)) {
    const seriesArr = option.series as Record<string, unknown>[]
    next.series = seriesArr.map((s, idx) => {
      const value = findValueAtDate(s.data, markTs)
      const yIdx  = (s.yAxisIndex as number) ?? 0
      const colour = (s.lineStyle as Record<string, unknown>)?.color as string | undefined
                  ?? (s.itemStyle as Record<string, unknown>)?.color as string | undefined
                  ?? '#8b949e'
      const out: Record<string, unknown> = { ...s }

      // Vertical line at the snapshot date — only on the first series of each axis
      if (idx === 0 || (idx === 1 && yIdx !== 0)) {
        out.markLine = {
          symbol: ['none', 'none'],
          silent: true,
          label: {
            formatter: markDate,
            color: '#e6edf3',
            fontSize: 10,
            fontWeight: 600,
            position: 'insideEndTop',
            backgroundColor: 'rgba(13,17,23,0.85)',
            padding: [3, 6, 3, 6],
            borderRadius: 3,
          },
          lineStyle: { color: '#FACC15', type: 'dashed', width: 1.2, opacity: 0.8 },
          data: [{ xAxis: markTs }],
        }
      }

      // Point marker + value label on each series at the snapshot date
      if (value !== null) {
        out.markPoint = {
          symbol: 'circle',
          symbolSize: 6,
          itemStyle: { color: colour, borderColor: '#0d1117', borderWidth: 1.5 },
          label: {
            formatter: value.toFixed(2),
            color: colour,
            fontSize: 10,
            fontWeight: 700,
            position: 'top',
            distance: 8,
            backgroundColor: 'rgba(13,17,23,0.85)',
            padding: [2, 5, 2, 5],
            borderRadius: 3,
          },
          data: [{ xAxis: markTs, yAxis: value }],
        }
      }
      return out
    })
  }

  return next
}

// ── KPI tooltip definitions ────────────────────────────────────────────────
const KPI_TIPS = {
  vix_spot:    'VIX spot level — the CBOE 30-day implied vol index on SPX options. Values above 20 signal elevated fear; above 30 is crisis territory.',
  vix_ratio:   'VIX / VIX3M ratio. Above 1.0 = front backwardation (fear spike). Below 0.92 = deep contango (carry-on zone).',
  carry_ratio: 'VIX futures carry ratio: front contract / 3-month rolling IV. Drives the carry signal — lower is better for short-vol strategies.',
  vrp:         'Vol Risk Premium = implied vol – realized vol. Positive VRP means options are expensive relative to actual moves; core edge for short-vol traders.',
  vix3m:       'VIX 3-month futures level — the 90-day implied vol expectation. Used as the baseline for carry calculation.',
  uvxy_cost:   'Estimated monthly roll cost for UVXY (2× long VIX ETF). High cost = structural headwind for long-vol positions. Typically –15 to –30% per month in contango.',
  svxy_yield:  'Estimated monthly roll yield for SVXY (–0.5× VIX ETF). Positive = carry income for short-vol positions.',
  vvix_vix:    'VVIX / VIX ratio — measures vol-of-vol relative to spot vol. Ratios above 5 signal tail risk and potential mean-reversion of short-vol positions.',
}

function TransitionMatrixCard({ t }: { t: TransitionMatrix }) {
  const { buckets, current_bucket, matrix, n_obs } = t

  // Short labels for columns — keep the threshold value, drop the parenthetical description
  const shortLabel = (b: string) => b.replace(/\s*\(.*\)/, '').trim()

  function cellColor(pct: number, isCurrentRow: boolean) {
    if (!isCurrentRow) return `rgba(255,255,255,${Math.min(pct / 100 * 0.35, 0.35)})`
    if (pct >= 50) return 'rgba(45,212,191,0.25)'
    if (pct >= 25) return 'rgba(45,212,191,0.12)'
    return 'transparent'
  }

  return (
    <div className="card" style={{ padding: '14px 16px' }}>
      <div className="flex items-baseline gap-2 mb-3">
        <Tooltip text="Empirical probability of transitioning from the current carry-ratio regime to each other regime within 21 trading days (~1 month). Based on full VIX history. Highlighted row = current regime." icon>
          <span className="section-title">Regime Transition Probabilities (21d)</span>
        </Tooltip>
        <span className="caption">current: <strong style={{ color: '#2DD4BF' }}>{current_bucket}</strong></span>
      </div>
      <div style={{ overflowX: 'auto' }}>
        <table className="vp-table" style={{ minWidth: 520 }}>
          <thead>
            <tr>
              <th style={{ width: 140 }}>From \ To</th>
              {buckets.map((b) => (
                <th key={b} style={{ textAlign: 'center', fontSize: 11 }}>{shortLabel(b)}</th>
              ))}
              <th style={{ textAlign: 'right', color: '#484f58', fontSize: 11 }}>n obs</th>
            </tr>
          </thead>
          <tbody>
            {buckets.map((from) => {
              const isCurrent = from === current_bucket
              return (
                <tr key={from} style={isCurrent ? { background: 'rgba(45,212,191,0.06)', fontWeight: 600 } : undefined}>
                  <td style={{ fontSize: 11, color: isCurrent ? '#2DD4BF' : '#8b949e' }}>
                    {from}{isCurrent ? ' ◀' : ''}
                  </td>
                  {buckets.map((to) => {
                    const pct = matrix[from]?.[to] ?? 0
                    return (
                      <td key={to} style={{
                        textAlign: 'center',
                        fontFamily: 'monospace',
                        fontSize: 12,
                        background: cellColor(pct, isCurrent),
                        color: pct >= 40 ? '#e6edf3' : '#8b949e',
                      }}>
                        {pct > 0 ? `${pct}%` : '—'}
                      </td>
                    )
                  })}
                  <td style={{ textAlign: 'right', color: '#484f58', fontSize: 11, fontFamily: 'monospace' }}>
                    {n_obs[from] ?? 0}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default function VIXView() {
  const { data, isLoading, error } = useVix()
  const location = useLocation()
  const subTab: SubTab = location.pathname.endsWith('history') ? 'history'
                       : location.pathname.endsWith('analytics') ? 'analytics'
                       : 'status'

  // ── Snapshot date picker (Status tab) ─────────────────────────────
  const [snapDate, setSnapDate] = useState('')
  const [pendingDate, setPendingDate] = useState('')

  // ── Time Series date range zoom ────────────────────────────────────
  const [zoomFrom, setZoomFrom] = useState('')
  const [zoomTo,   setZoomTo]   = useState('')

  const { data: snapData, isLoading: snapLoading, error: snapError } = useQuery({
    queryKey: ['vix-snapshot', snapDate],
    queryFn: () => getVixSnapshot(snapDate),
    enabled: !!snapDate,
    staleTime: Infinity,  // historical data never changes
    retry: 1,
  })

  // Use snapshot data when a date is selected, otherwise live data
  const activeData = (snapDate && snapData) ? snapData as VixData : data as VixData | undefined
  const activeLoading = snapDate ? snapLoading : isLoading

  const vix = data as VixData | undefined
  const m = (activeData?.metrics ?? {}) as Metrics
  const liveCharts = (vix?.charts ?? {}) as Record<string, Record<string, unknown>>
  const snapCharts = (snapData as VixData | undefined)?.charts ?? {}
  const charts = (snapDate && snapData) ? { ...liveCharts, term_structure: snapCharts.term_structure } : liveCharts
  const chartStats = (vix?.chart_stats ?? {}) as Record<string, ChartStats>
  const percentiles = (activeData?.percentiles ?? {}) as Record<string, number>
  const outcomes = (activeData?.outcomes ?? {}) as Parameters<typeof VixDashboard>[0]['outcomes']

  return (
    <div className="space-y-2">
      {m.as_of && (
        <span className="text-xs text-text-faint">
          as of {m.as_of}{snapDate ? ' (snapshot)' : ''}
        </span>
      )}

      {error && <EmptyState message={`VIX data unavailable: ${error}`} />}

      {/* ── STATUS ──────────────────────────────────────────────────────── */}
      {subTab === 'status' && (
        <div className="space-y-2">

          {/* Date picker */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 11, color: '#484f58', textTransform: 'uppercase', letterSpacing: '0.08em' }}>
              Snapshot
            </span>
            <input
              type="date"
              value={pendingDate}
              onChange={(e) => setPendingDate(e.target.value)}
              max={new Date().toISOString().split('T')[0]}
              style={{
                fontSize: 12, padding: '3px 8px',
                background: '#161b22', border: '1px solid #21262d',
                borderRadius: 4, color: '#8b949e', outline: 'none', cursor: 'pointer',
              }}
            />
            <button
              onClick={() => {
                setSnapDate(pendingDate)
                // Pre-populate Time Series zoom to ±60d around snapshot date
                const d = new Date(pendingDate)
                const from = new Date(d.getTime() - 60 * 86400000).toISOString().split('T')[0]
                const to   = new Date(d.getTime() + 60 * 86400000).toISOString().split('T')[0]
                setZoomFrom(from)
                setZoomTo(to)
              }}
              disabled={!pendingDate}
              style={{
                fontSize: 11, padding: '3px 10px', borderRadius: 4,
                background: pendingDate ? '#1f6feb' : '#161b22',
                border: '1px solid #21262d', color: pendingDate ? '#e6edf3' : '#484f58',
                cursor: pendingDate ? 'pointer' : 'default',
              }}
            >
              Load
            </button>
            {snapDate && (
              <button
                onClick={() => { setSnapDate(''); setPendingDate(''); setZoomFrom(''); setZoomTo('') }}
                style={{
                  fontSize: 11, padding: '3px 10px', borderRadius: 4,
                  background: 'transparent', border: '1px solid #21262d',
                  color: '#8b949e', cursor: 'pointer',
                }}
              >
                ✕ Live
              </button>
            )}
            {snapLoading && <span style={{ fontSize: 11, color: '#484f58' }} className="animate-pulse">loading…</span>}
            {snapError && <span style={{ fontSize: 11, color: '#f85149' }}>snapshot unavailable</span>}
          </div>

          {!activeLoading && activeData && (
            <VixDashboard
              metrics={m as Parameters<typeof VixDashboard>[0]['metrics']}
              percentiles={percentiles}
              outcomes={outcomes}
            />
          )}

          <ChartCard option={charts.term_structure ?? null} height={380} loading={activeLoading} />
        </div>
      )}

      {/* ── TIME SERIES ─────────────────────────────────────────────────── */}
      {subTab === 'history' && (
        <div className="space-y-2">
          <div className="grid grid-cols-4 gap-2">
            <Tooltip text={KPI_TIPS.vix3m}>
              <MetricCard label="VIX 3M" value={fmt(m.vix3m)} sub="3-month future" />
            </Tooltip>
            <Tooltip text={KPI_TIPS.uvxy_cost}>
              <MetricCard label="UVXY Roll Cost" value={fmt(m.uvxy_monthly_cost)} sub="/month" />
            </Tooltip>
            <Tooltip text={KPI_TIPS.svxy_yield}>
              <MetricCard label="SVXY Roll Yield" value={fmt(m.svxy_monthly_yield)} sub="/month" />
            </Tooltip>
            <Tooltip text={KPI_TIPS.vvix_vix}>
              <MetricCard label="VVIX / VIX" value={fmt(m.vvix_vix_ratio)} sub="tail-risk pressure" />
            </Tooltip>
          </div>

          {/* Date range filter */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 11, color: '#484f58', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Range</span>
            <input type="date" value={zoomFrom} onChange={(e) => setZoomFrom(e.target.value)}
              style={{ fontSize: 12, padding: '3px 8px', background: '#161b22', border: '1px solid #21262d', borderRadius: 4, color: '#8b949e', outline: 'none' }} />
            <span style={{ fontSize: 11, color: '#30363d' }}>→</span>
            <input type="date" value={zoomTo} onChange={(e) => setZoomTo(e.target.value)}
              max={new Date().toISOString().split('T')[0]}
              style={{ fontSize: 12, padding: '3px 8px', background: '#161b22', border: '1px solid #21262d', borderRadius: 4, color: '#8b949e', outline: 'none' }} />
            {(zoomFrom || zoomTo) && (
              <button onClick={() => { setZoomFrom(''); setZoomTo('') }}
                style={{ fontSize: 11, padding: '3px 10px', borderRadius: 4, background: 'transparent', border: '1px solid #21262d', color: '#8b949e', cursor: 'pointer' }}>
                ✕ Reset
              </button>
            )}
          </div>

          <ChartCard
            title="VIX / VIX3M Ratio"
            subtitle="Front-month vs 3-month ratio. Below 92 = carry zone; above 100 = backwardation."
            option={withDateZoom(charts.ratio_history ?? null, zoomFrom, zoomTo, snapDate)}
            height={320}
            loading={isLoading}
            exportName="vix_ratio_history"
            stats={statsRow(chartStats.ratio_history)}
          />

          <div className="grid grid-cols-2 gap-3">
            <ChartCard
              title="VIX Percentile"
              subtitle="Where the current VIX ratio sits in its 18-year distribution."
              option={withDateZoom(charts.percentile ?? null, zoomFrom, zoomTo, snapDate)}
              height={260} loading={isLoading}
              exportName="vix_percentile"
              stats={statsRow(chartStats.percentile)}
            />
            <ChartCard
              title="Roll Cost"
              subtitle="Estimated monthly roll cost (UVXY) and yield (SVXY) from the basis."
              option={withDateZoom(charts.roll_cost ?? null, zoomFrom, zoomTo, snapDate)}
              height={260} loading={isLoading}
              exportName="vix_roll_cost"
              stats={statsRow(chartStats.roll_cost)}
            />
          </div>

          <ChartCard
            title="Volatility Risk Premium"
            subtitle="Implied minus realised vol. Positive = options expensive; the short-vol edge."
            option={withDateZoom(charts.vrp ?? null, zoomFrom, zoomTo, snapDate)}
            height={260} loading={isLoading}
            exportName="vix_vrp"
            stats={statsRow(chartStats.vrp)}
          />
        </div>
      )}

      {/* ── ANALYTICS ───────────────────────────────────────────────────── */}
      {subTab === 'analytics' && (() => {
        const pcaStatus = (vix as { pca?: { is_ready?: boolean; n_obs?: number; min_obs?: number } } | undefined)?.pca
        const pcaReady  = pcaStatus?.is_ready === true
        const pcaN      = pcaStatus?.n_obs ?? 0
        const pcaMin    = pcaStatus?.min_obs ?? 60

        return (
          <div className="space-y-2">
            <div className="flex items-baseline gap-2">
              <span className="section-title">Term Structure PCA</span>
              <Tooltip text="Principal Component Analysis of the VIX futures term structure, following Johnson (2017). PC1 captures the level shift; PC2 the slope/carry signal used in regime classification.">
                <span className="text-xs text-text-faint cursor-help">— Johnson (2017) JFQA 52(6) ⓘ</span>
              </Tooltip>
            </div>

            {/* Accumulating-data banner */}
            {!pcaReady && !isLoading && (
              <div style={{
                padding: '6px 12px', borderRadius: 4, fontSize: 12,
                color: '#e3b341', background: 'rgba(227,179,65,0.10)',
                borderLeft: '2px solid #e3b341',
                display: 'flex', alignItems: 'center', gap: 12,
              }}>
                <span style={{ fontWeight: 600 }}>⏳ Accumulating</span>
                <span style={{ color: '#8b949e' }}>
                  PCA needs {pcaMin} days of VX strip history. Currently at <strong style={{ color: '#e6edf3' }}>{pcaN}/{pcaMin}</strong> — charts populate automatically once threshold reached.
                </span>
              </div>
            )}

            <div className="grid grid-cols-2 gap-3">
              <ChartCard
                title="PCA Loadings"
                subtitle="PC1 = level shift (~90% variance). PC2 = slope/carry signal."
                option={charts.pca ?? null} height={260} loading={isLoading}
                exportName="vix_pca_loadings"
              />
              <ChartCard
                title="Slope History"
                subtitle="PC2 score over time. High = steep contango, rich carry."
                option={charts.slope_history ?? null} height={260} loading={isLoading}
                exportName="vix_pca_slope_history"
              />
            </div>

            <hr className="border-border" />

            <div className="flex items-baseline gap-2">
              <span className="section-title">Historical Outcomes</span>
              <Tooltip text="Forward returns on short-VIX positions grouped by the carry ratio bucket at entry. The empirical edge underpinning the carry signal.">
                <span className="text-xs text-text-faint cursor-help">by carry bucket ⓘ</span>
              </Tooltip>
            </div>

            <ChartCard
              title="SVXY 21d Returns by Carry Bucket"
              subtitle="Median forward return + VIX spike rate per bucket. Low carry = post-spike rebound zone."
              option={charts.outcomes ?? null} height={300} loading={isLoading}
              exportName="vix_outcomes_by_carry"
            />

            {vix?.transitions && <TransitionMatrixCard t={vix.transitions} />}
          </div>
        )
      })()}
    </div>
  )
}
