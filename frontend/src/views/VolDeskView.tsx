import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getSurfaceCharts, getSkewCharts, getSkewDynamicsCharts, getSmile } from '../api/surfaceApi'
import { getRegimeCharts } from '../api/regimeApi'
import { useAppStore } from '../store/useAppStore'
import ChartCard from '../components/charts/ChartCard'
import Surface3D from '../components/charts/Surface3D'
import SpotVolScatter from '../components/charts/SpotVolScatter'
import Tooltip from '../components/shared/Tooltip'
import PctPill from '../components/shared/PctPill'
import KPITile, { KPIIntent } from '../components/shared/KPITile'
import KPIGrid from '../components/shared/KPIGrid'
import clsx from 'clsx'

type Tab = 'surface' | 'skew' | 'skew-dynamics' | 'regime'

type RawSurface = {
  strike_grid: number[]
  tte_grid:    number[]
  surface:     (number | null)[][]
  symbol?:     string
  option_type?: string
}

type SurfaceData = {
  raw_surface?: RawSurface
  smile?: Record<string, unknown>
  term_structure?: Record<string, unknown>
  expiries?: string[]
  symbol?: string
}

type MeanReversionSignal = {
  symbol: string
  signal_count: number
  primary_signal: {
    metric: string
    z_score: number
    percentile: number
    direction: string
    current: number
    mean: number
    expected_reversion: number
  }
  all_signals: { metric: string; z_score: number; percentile: number; direction: string }[]
}

type SkewData = Record<string, Record<string, unknown>>

type RegimeData = {
  regime?: Record<string, unknown>
  charts?: Record<string, Record<string, unknown>>
  rv_percentile?: number
  atm_iv_percentile?: number
  atm_iv?: number
}

const TABS: { id: Tab; label: string }[] = [
  { id: 'surface',        label: 'Surface' },
  { id: 'skew',           label: 'Skew' },
  { id: 'skew-dynamics',  label: 'Skew Dynamics' },
  { id: 'regime',         label: 'Regime' },
]

// Map regime label → KPI intent
function regimeIntent(label: string | undefined): KPIIntent {
  const l = (label ?? '').toLowerCase()
  if (l.includes('sticky delta'))  return 'positive'
  if (l.includes('sticky strike')) return 'neutral'
  if (l.includes('local vol'))     return 'warn'
  if (l.includes('jumpy'))         return 'negative'
  return 'neutral'
}

// Map sentiment text → KPI intent
function sentimentIntent(s: string | undefined): KPIIntent {
  const t = (s ?? '').toLowerCase()
  if (t.includes('calm') || t.includes('trending')) return 'positive'
  if (t.includes('normal'))   return 'neutral'
  if (t.includes('elevated')) return 'warn'
  if (t.includes('panic'))    return 'negative'
  return 'neutral'
}

function vixIntent(v: number | undefined): KPIIntent {
  if (v == null) return 'neutral'
  if (v < 15) return 'positive'
  if (v <= 25) return 'warn'
  return 'negative'
}

function pctIntent(p: number | undefined, invert = false): KPIIntent {
  if (p == null) return 'neutral'
  const high = invert ? p < 20 : p > 75
  const mid  = invert ? p < 50 : p > 50
  if (high) return invert ? 'negative' : 'warn'
  if (mid)  return 'neutral'
  return 'positive'
}

export default function VolDeskView() {
  const { activeSymbol, setActiveSymbol } = useAppStore()
  const [tab, setTab]         = useState<Tab>('surface')
  const [input, setInput]     = useState(activeSymbol)
  const [expiry, setExpiry]   = useState('')
  const [optionType, setOptionType] = useState<'call' | 'put'>('call')

  const { data: surfaceData, isLoading: surfaceLoading } = useQuery({
    queryKey: ['surface', activeSymbol, optionType],
    queryFn: () => getSurfaceCharts(activeSymbol, optionType),
    staleTime: 5 * 60_000,
    enabled: activeSymbol !== '',
  })

  const { data: skewData, isLoading: skewLoading } = useQuery({
    queryKey: ['skew', activeSymbol],
    queryFn: () => getSkewCharts(activeSymbol),
    enabled: tab === 'skew' && activeSymbol !== '',
    staleTime: 5 * 60_000,
  })

  const { data: smileData, isLoading: smileLoading } = useQuery({
    queryKey: ['smile', activeSymbol, expiry],
    queryFn: () => getSmile(activeSymbol, expiry),
    enabled: tab === 'skew' && activeSymbol !== '' && expiry !== '',
    staleTime: 5 * 60_000,
  })

  const { data: skewDynData, isLoading: skewDynLoading } = useQuery({
    queryKey: ['skew-dynamics', activeSymbol],
    queryFn: () => getSkewDynamicsCharts(activeSymbol),
    enabled: tab === 'skew-dynamics',
    staleTime: 5 * 60_000,
  })

  const { data: regimeData, isLoading: regimeLoading } = useQuery({
    queryKey: ['regime', activeSymbol],
    queryFn: () => getRegimeCharts(activeSymbol),
    enabled: tab === 'regime',
    staleTime: 5 * 60_000,
  })

  const surface  = surfaceData  as SurfaceData  | undefined
  const skew     = skewData     as SkewData     | undefined
  const skewDynRaw = skewDynData as (SkewData & { mean_reversion?: MeanReversionSignal }) | undefined
  const mrSignal = skewDynRaw?.mean_reversion
  const skewDyn  = skewDynRaw
    ? Object.fromEntries(Object.entries(skewDynRaw).filter(([k]) => k !== 'mean_reversion')) as SkewData
    : undefined
  const regime   = regimeData   as RegimeData   | undefined
  const expiries = surface?.expiries ?? []

  const handleLoad = () => {
    const sym = input.trim().toUpperCase()
    if (!sym) return
    setActiveSymbol(sym)
  }

  return (
    <div className="space-y-2">
      {/* Symbol bar + tab pills */}
      <div className="flex items-center gap-2 flex-wrap">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value.toUpperCase())}
          onKeyDown={(e) => e.key === 'Enter' && handleLoad()}
          placeholder="Symbol…"
          style={{ fontSize: 13 }}
          className="w-24 px-2 py-1 bg-bg-elevated rounded
                     text-text-primary placeholder-text-faint focus:outline-none"
        />
        <button onClick={handleLoad} className="nav-tab-btn">Load</button>

        {/* Call/Put toggle */}
        <div className="flex rounded overflow-hidden ml-1" style={{ background: '#161b22' }}>
          {(['call', 'put'] as const).map((t) => (
            <button
              key={t}
              onClick={() => setOptionType(t)}
              style={{ fontSize: 12, padding: '4px 12px', fontWeight: 500, letterSpacing: '0.04em' }}
              className={clsx(
                'transition-colors uppercase',
                optionType === t
                  ? 'bg-accent/20 text-accent'
                  : 'text-text-muted hover:text-text-primary'
              )}
            >
              {t}
            </button>
          ))}
        </div>

        {activeSymbol && (
          <span style={{ fontSize: 13, color: '#2DD4BF', fontWeight: 600, letterSpacing: '0.03em' }}>
            {activeSymbol}
          </span>
        )}

        <div className="flex gap-0.5 ml-4">
          {TABS.map((t) => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={clsx('nav-tab-btn', tab === t.id && 'active')}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {/* ── SURFACE ────────────────────────────────────────────────────── */}
      {tab === 'surface' && (
        <div className="space-y-2">
          <div className="card" style={{ overflow: 'visible', minHeight: 520 }}>
            {surfaceLoading
              ? <div className="skeleton" style={{ height: 520, margin: 8, borderRadius: 6 }} />
              : <Surface3D rawSurface={surface?.raw_surface ?? null} height={520} />
            }
          </div>
          <div className="grid grid-cols-2 gap-3">
            <ChartCard
              title="IV Smile"
              subtitle="Implied vol by strike, across all expiries."
              option={surface?.smile ?? null}
              height={260} loading={surfaceLoading}
              exportName={`${activeSymbol}_smile`}
            />
            <ChartCard
              title="Term Structure"
              subtitle="ATM implied vol by time to expiry. Upward slope = contango."
              option={surface?.term_structure ?? null}
              height={260} loading={surfaceLoading}
              exportName={`${activeSymbol}_term_structure`}
            />
          </div>
        </div>
      )}

      {/* ── SKEW ───────────────────────────────────────────────────────── */}
      {tab === 'skew' && (
        <div className="space-y-2">
          {expiries.length > 0 && (
            <div className="flex items-center gap-2">
              <span className="caption">Expiry</span>
              <select
                value={expiry}
                onChange={(e) => setExpiry(e.target.value)}
                style={{ fontSize: 13 }}
                className="px-2 py-1 bg-bg-elevated rounded text-text-primary focus:outline-none"
              >
                <option value="">— all expiries —</option>
                {expiries.map((e) => <option key={e} value={e}>{e}</option>)}
              </select>
              {expiry && <span className="caption">showing expiry-specific smile</span>}
            </div>
          )}
          {/* Top row: smile + term structure */}
          <div className="grid grid-cols-2 gap-3">
            <ChartCard
              title="IV Smile"
              subtitle={expiry ? `Implied vol by strike for ${expiry}.` : 'Implied vol by strike across all expiries.'}
              option={(expiry ? smileData as Record<string,unknown> : surface?.smile) ?? null}
              height={280}
              loading={expiry ? smileLoading : surfaceLoading}
              exportName={`${activeSymbol}_iv_smile${expiry ? '_' + expiry : ''}`}
            />
            <ChartCard
              title="Term Structure"
              subtitle="ATM implied vol by time to expiry. Upward slope = contango."
              option={surface?.term_structure ?? null}
              height={280}
              loading={surfaceLoading}
              exportName={`${activeSymbol}_term_structure`}
            />
          </div>
          {/* Bottom row: skew slope + curvature */}
          <div className="grid grid-cols-2 gap-3">
            <ChartCard
              title="Skew Slope"
              subtitle="Slope of the IV smile across expiries. Negative = put skew."
              option={(skew as Record<string,unknown>)?.slope as Record<string,unknown> ?? null}
              height={260}
              loading={skewLoading}
              exportName={`${activeSymbol}_skew_slope`}
            />
            <ChartCard
              title="Skew Curvature"
              subtitle="Convexity of the smile. Higher = more wing risk priced in."
              option={(skew as Record<string,unknown>)?.curvature as Record<string,unknown> ?? null}
              height={260}
              loading={skewLoading}
              exportName={`${activeSymbol}_skew_curvature`}
            />
          </div>
        </div>
      )}

      {/* ── SKEW DYNAMICS ──────────────────────────────────────────────── */}
      {tab === 'skew-dynamics' && (
        <div className="space-y-2">

          {/* Mean-reversion signal — concise inline banner */}
          {mrSignal && (() => {
            const p  = mrSignal.primary_signal
            const z  = p.z_score
            const pct = (p.percentile * 100).toFixed(0)
            const steep = p.direction === 'skew_too_steep'
            const color = Math.abs(z) >= 2.5 ? (steep ? '#f85149' : '#3fb950')
                        : Math.abs(z) >= 1.5 ? '#FACC15' : '#8b949e'
            const metricLabel = p.metric.replace(/_/g, ' ')
            const revDir = steep ? 'flatten' : 'steepen'
            return (
              <div style={{
                padding: '6px 12px', borderRadius: 4, fontSize: 12,
                color, background: color + '14', borderLeft: `2px solid ${color}`,
                display: 'flex', alignItems: 'center', gap: 12,
              }}>
                <span style={{ fontWeight: 600, color, whiteSpace: 'nowrap' }}>
                  ↻ Mean-Reversion
                </span>
                <span style={{ color: '#e6edf3' }}>
                  <strong style={{ color }}>{metricLabel}</strong>
                  {' · '}P{pct} (z={z.toFixed(1)}) · expects {revDir}
                </span>
                {mrSignal.all_signals.length > 1 && (
                  <span style={{ color: '#6e7681', fontSize: 11, marginLeft: 'auto' }}>
                    +{mrSignal.all_signals.length - 1} more
                  </span>
                )}
              </div>
            )
          })()}

          {/* Subtitles per chart — labels match what skew_dynamics returns */}
          {(() => {
            const SUBTITLES: Record<string, { title: string; sub: string }> = {
              atm_iv:         { title: 'ATM IV',          sub: 'Implied vol at-the-money over time.' },
              skew_slopes:    { title: 'Skew Slopes',     sub: '25Δ put vs call slope. Negative = put skew dominant.' },
              term_structure: { title: 'Term Structure',  sub: 'Front- vs long-dated IV. Negative slope = backwardation.' },
              realised_vol:   { title: 'Realised Vol',    sub: 'Multi-horizon historical volatility (5/10/21d).' },
            }
            const items = skewDyn && Object.keys(skewDyn).length > 0
              ? Object.entries(skewDyn)
              : Array.from({ length: 4 }, (_, i) => [`placeholder_${i}`, null] as [string, null])
            return (
              <div className="grid grid-cols-2 gap-3">
                {items.map(([k, v]) => {
                  const meta = SUBTITLES[k]
                  return (
                    <ChartCard
                      key={k}
                      title={meta?.title}
                      subtitle={meta?.sub}
                      option={v ?? null}
                      height={280}
                      loading={skewDynLoading}
                      exportName={`${activeSymbol}_${k}`}
                    />
                  )
                })}
              </div>
            )
          })()}
        </div>
      )}

      {/* ── REGIME ─────────────────────────────────────────────────────── */}
      {tab === 'regime' && (
        <div className="space-y-2">
          {regime?.regime && (() => {
            const r    = regime.regime as Record<string, unknown>
            const info = (r.regime_info ?? {}) as Record<string, unknown>
            const regimeLabel = String(info.label ?? r.regime ?? '—')
            const sentimentText = String(info.sentiment ?? '—')
            const corrText = String(info.spot_vol_corr ?? '—')
            const skewPnl  = String(info.skew_trade_pnl ?? '—')
            const color    = String(info.color ?? '#8b949e')

            const vixCur     = r.vix_current as number | undefined
            const vixPct     = r.vix_percentile as number | undefined
            const rvVal      = r.realised_vol as number | undefined
            const atmIv      = regime?.atm_iv
            const vov        = r.vol_of_vol as number | undefined
            const skewPnlIntent: KPIIntent = skewPnl.toLowerCase().includes('positive') ? 'positive'
                                            : skewPnl.toLowerCase().includes('negative') ? 'negative'
                                            : 'neutral'

            return (
              <>
                {/* ── Unified 3×3 KPI grid — vertical dividers align across rows ── */}
                <KPIGrid cols={3}>
                  {/* Row 1: qualitative regime */}
                  <KPITile
                    label="Regime"
                    tooltip="4-state vol regime based on smile dynamics. Sticky Delta = vol moves with spot; Sticky Strike = vol pinned to strike; Local Vol = both effects; Jumpy Vol = unstable, mean-reverting."
                    value={regimeLabel}
                    intent={regimeIntent(regimeLabel)}
                    sub="Smile dynamics classifier"
                  />
                  <KPITile
                    label="Sentiment"
                    tooltip="Market sentiment derived from VIX level, trend, and skew."
                    value={sentimentText}
                    intent={sentimentIntent(sentimentText)}
                    sub="VIX-driven market mood"
                  />
                  <KPITile
                    label="Spot-Vol Corr"
                    tooltip="21-day rolling correlation between spot returns and IV changes. Negative = inverse (typical equity)."
                    value={corrText}
                    intent="neutral"
                    sub="21d rolling correlation"
                  />

                  {/* Row 2: price / vol levels */}
                  <KPITile
                    label="VIX Level"
                    tooltip="Current CBOE VIX spot. Below 15 = low vol; 15–25 = normal; above 25 = stress; above 30 = crisis."
                    value={vixCur != null ? vixCur.toFixed(2) : '—'}
                    intent={vixIntent(vixCur)}
                    sub={vixCur == null ? '' : vixCur < 15 ? 'Low vol environment' : vixCur <= 25 ? 'Normal range' : vixCur <= 30 ? 'Stress' : 'Crisis'}
                  />
                  <KPITile
                    label="VIX %ile"
                    tooltip="VIX percentile vs trailing 252 trading days. >80% = elevated fear; <20% = complacency."
                    value={vixPct != null ? vixPct.toFixed(1) + '%' : '—'}
                    intent={pctIntent(vixPct)}
                    sub="vs trailing 252 days"
                  />
                  <KPITile
                    label="Realised Vol"
                    tooltip="21-day historical volatility (annualised). Compared against IV to compute Vol Risk Premium."
                    value={rvVal != null ? (rvVal * 100).toFixed(1) + '%' : '—'}
                    intent={pctIntent(regime?.rv_percentile)}
                    badge={<PctPill p={regime?.rv_percentile} />}
                    sub="21-day annualised"
                  />

                  {/* Row 3: options-specific */}
                  <KPITile
                    label="ATM IV"
                    tooltip="ATM implied volatility (call side) from the nearest expiry. Badge shows percentile rank vs 60-day history."
                    value={atmIv != null ? ((atmIv as number) * 100).toFixed(1) + '%' : '—'}
                    intent={pctIntent(regime?.atm_iv_percentile)}
                    badge={<PctPill p={regime?.atm_iv_percentile} />}
                    sub="Nearest expiry, calls"
                  />
                  <KPITile
                    label="Vol-of-Vol"
                    tooltip="Std deviation of daily IV changes (annualised). High = unstable regime; low = stable carry."
                    value={vov != null ? vov.toFixed(3) : '—'}
                    intent="neutral"
                    sub="IV change volatility"
                  />
                  <KPITile
                    label="Skew Trade P&L"
                    tooltip="Expected P&L direction for skew trades (buy downside / sell upside) given the current regime."
                    value={skewPnl}
                    intent={skewPnlIntent}
                    sub="Buy downside / sell upside"
                  />
                </KPIGrid>

                {/* ── Recommendation banner ────────────────────── */}
                {info.recommendation && (
                  <div style={{
                    padding: '7px 12px',
                    borderRadius: 5,
                    fontSize: 12,
                    fontWeight: 500,
                    color,
                    background: color + '18',
                    borderLeft: `3px solid ${color}`,
                  }}>
                    {String(info.recommendation)}
                  </div>
                )}

                {/* ── Description ──────────────────────────────── */}
                {info.description && (
                  <p style={{ fontSize: 13, color: '#8b949e', lineHeight: 1.7, margin: 0 }}>
                    {String(info.description)}
                  </p>
                )}
              </>
            )
          })()}

          {/* Charts */}
          <div className="grid grid-cols-3 gap-3">
            {/* Interactive spot-vol scatter — click points to exclude outliers */}
            <div className="card p-3">
              <div className="caption mb-1">
                <Tooltip text="Scatter of daily spot returns (x) vs ΔVol (y). Slope and sign determine regime: negative = Sticky Delta; flat = Sticky Strike; curved = Local Vol. Click any point to exclude it from the regression — useful for removing outlier days." icon>Spot vs Vol</Tooltip>
              </div>
              {(() => {
                const r = regime?.regime as Record<string, unknown> | undefined
                const rets  = (r?.ts_spot_returns as number[] | undefined) ?? []
                const vols  = (r?.ts_realised_vol as number[] | undefined) ?? []
                const dates = (r?.ts_dates       as string[] | undefined) ?? []
                return rets.length >= 3
                  ? <SpotVolScatter returns={rets} vol={vols} dates={dates} symbol={activeSymbol} height={230} />
                  : <div className="flex items-center justify-center text-text-muted text-xs" style={{ height: 230 }}>No data</div>
              })()}
            </div>
            <ChartCard
              title="Spot-Vol Correlation"
              subtitle="21d rolling correlation. Negative = normal equity regime."
              tooltip="Persistently negative = normal equity regime. Trending toward zero or positive = regime transition signal."
              option={regime?.charts?.correlation ?? null} height={260} loading={regimeLoading}
              exportName={`${activeSymbol}_spot_vol_corr`}
            />
            <ChartCard
              title="√T Skew Scaling"
              subtitle="Skew slope vs √T. Shape identifies the dominant smile dynamic."
              tooltip="Linear fit = Sticky Delta; flat = Sticky Strike; concave = Local Vol."
              option={regime?.charts?.sqrt_t ?? null} height={260} loading={regimeLoading}
              exportName={`${activeSymbol}_sqrt_t_skew`}
            />
          </div>
        </div>
      )}
    </div>
  )
}
