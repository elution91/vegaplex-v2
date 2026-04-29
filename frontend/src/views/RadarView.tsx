import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { getRadar } from '../api/radarApi'
import { useAppStore } from '../store/useAppStore'
import EChart from '../components/charts/EChart'
import Tooltip from '../components/shared/Tooltip'

type RegimeMapRow = {
  symbol?: string
  regime?: string
  sentiment?: string       // calm | trending | elevated | stressed | panic | unknown
  iv_pct?: number | null   // current IV percentile vs trailing 252d
  rv_pct?: number | null   // current RV percentile vs trailing window
  rv?: number
  vov?: number
  iv_rv?: number | null
  mult?: number
  recommendation?: string
  color?: string
  [key: string]: unknown
}

type RadarData = {
  regime_table?: RegimeMapRow[]
  summary_table?: unknown[]
  universe_table?: Record<string, unknown>[]
  charts?: Record<string, Record<string, unknown>>
}

// ── Regime dot color ───────────────────────────────────────────────────────

function regimeColor(regime: string | undefined): string {
  const r = (regime ?? '').toLowerCase()
  if (r.includes('sticky delta'))  return '#3fb950'
  if (r.includes('sticky strike')) return '#2DD4BF'
  if (r.includes('local vol'))     return '#e3b341'
  if (r.includes('jumpy'))         return '#f85149'
  return '#484f58'
}

function sentimentColor(s: string | undefined): string {
  const t = (s ?? '').toLowerCase()
  if (t === 'calm')      return '#3fb950'
  if (t === 'trending')  return '#2DD4BF'
  if (t === 'elevated')  return '#e3b341'
  if (t === 'stressed')  return '#fb8500'
  if (t === 'panic')     return '#f85149'
  return '#484f58'
}

function sentimentLabel(s: string | undefined): string {
  const t = (s ?? '').toLowerCase()
  if (!t || t === 'unknown') return '—'
  return t.charAt(0).toUpperCase() + t.slice(1)
}

function PctPillSmall({ p }: { p: number | null | undefined }) {
  if (p == null) return null
  const danger = p > 75
  const warn   = p > 50
  const color  = danger ? '#f85149' : warn ? '#e3b341' : '#3fb950'
  return (
    <span style={{
      fontSize: 10, fontWeight: 600, color,
      fontFamily: 'JetBrains Mono, ui-monospace, monospace',
      marginLeft: 4,
    }}>
      P{Math.round(p)}
    </span>
  )
}

function ivRvColor(v: number | null | undefined): string {
  if (v == null) return '#484f58'
  if (v >= 1.25) return '#3fb950'
  if (v >= 1.0)  return '#e3b341'
  return '#f85149'
}

function fmt(v: unknown, decimals = 2): string {
  if (v == null) return '—'
  const n = Number(v)
  return isNaN(n) ? String(v) : n.toFixed(decimals)
}

// ── Universe grouping ──────────────────────────────────────────────────────

const LETF_SYMBOLS = new Set([
  'TQQQ', 'SQQQ', 'UPRO', 'SPXU', 'TECL', 'LABU', 'LABD',
  'UVXY', 'SVXY', 'VXX', 'VIXY', 'TVIX',
  'TNA', 'TZA', 'FAS', 'FAZ', 'NUGT', 'DUST',
])

function splitRows(rows: RegimeMapRow[]) {
  const letf: RegimeMapRow[] = []
  const other: RegimeMapRow[] = []
  for (const row of rows) {
    if (row.symbol && LETF_SYMBOLS.has(row.symbol)) letf.push(row)
    else other.push(row)
  }
  return { letf, other }
}

// ── Dot + label cell ───────────────────────────────────────────────────────

function DotLabel({ color, label }: { color: string; label: string }) {
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
      <span style={{
        width: 6, height: 6, borderRadius: '50%',
        background: color, flexShrink: 0, display: 'inline-block',
      }} />
      <span style={{ color: '#8b949e', fontSize: 12 }}>{label}</span>
    </span>
  )
}

// ── Row ────────────────────────────────────────────────────────────────────

function SymbolRow({ row, drillDown }: { row: RegimeMapRow; drillDown: (s: string) => void }) {
  const ivRv = row.iv_rv as number | null
  return (
    <tr>
      <td>
        <button
          onClick={() => row.symbol && drillDown(row.symbol)}
          style={{ color: '#2DD4BF', fontWeight: 700, background: 'none', border: 'none',
                   cursor: 'pointer', padding: 0, fontSize: 13, letterSpacing: '0.02em' }}
        >
          {row.symbol ?? '—'}
        </button>
      </td>
      <td>
        <DotLabel color={regimeColor(row.regime)} label={row.regime ?? '—'} />
      </td>
      <td>
        <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
          <DotLabel color={sentimentColor(row.sentiment)} label={sentimentLabel(row.sentiment)} />
          <PctPillSmall p={row.iv_pct as number | null} />
        </span>
      </td>
      <td className="font-mono" style={{ color: '#8b949e' }}>
        {fmt(row.rv ? row.rv * 100 : null, 1)}%
      </td>
      <td className="font-mono" style={{ color: '#8b949e' }}>
        {fmt(row.vov, 3)}
      </td>
      <td className="font-mono" style={{ color: ivRvColor(ivRv), fontWeight: ivRv != null && ivRv >= 1.25 ? 600 : 400 }}>
        {fmt(ivRv, 2)}
      </td>
      <td className="font-mono" style={{ color: '#8b949e' }}>
        {fmt(row.mult, 2)}
      </td>
    </tr>
  )
}

// ── Section divider ────────────────────────────────────────────────────────

function SectionHeader({ label, topBorder }: { label: string; topBorder?: boolean }) {
  return (
    <tr>
      <td colSpan={7} style={{
        padding: '14px 16px 6px',
        fontSize: 10, fontWeight: 600, letterSpacing: '0.12em',
        color: '#484f58', textTransform: 'uppercase',
        borderTop: topBorder ? '1px solid #21262d' : undefined,
      }}>
        {label}
      </td>
    </tr>
  )
}

// ── Main view ──────────────────────────────────────────────────────────────

export default function RadarView() {
  const universe        = useAppStore((s) => s.universe)
  const setActiveSymbol = useAppStore((s) => s.setActiveSymbol)
  const navigate        = useNavigate()
  const [lookback, setLookback] = useState(252)

  function drillDown(sym: string) {
    setActiveSymbol(sym)
    navigate('/vol-desk')
  }

  const { data, isLoading } = useQuery({
    queryKey: ['radar', universe, lookback],
    queryFn: () => getRadar(universe, lookback),
    enabled: true,
    staleTime: 10 * 60_000,
    refetchInterval: 10 * 60_000,
  })

  const radar = data as RadarData | undefined
  const regimeRows = radar?.regime_table ?? (radar?.universe_table as RegimeMapRow[] | undefined) ?? []
  const { letf, other } = splitRows(regimeRows)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 50px)', background: '#0d1117' }}>

      {/* ── Controls ────────────────────────────────────────────────── */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '12px 20px 0', flexShrink: 0 }}>
        <span style={{ fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.1em', color: '#484f58' }}>
          Universe Radar
        </span>
        <select
          value={lookback}
          onChange={(e) => setLookback(Number(e.target.value))}
          style={{ fontSize: 11, padding: '2px 6px', background: 'transparent',
                   border: '1px solid #21262d', borderRadius: 4, color: '#6e7681',
                   outline: 'none', cursor: 'pointer' }}
        >
          <option value={63}>3M</option>
          <option value={126}>6M</option>
          <option value={252}>1Y</option>
        </select>
        {isLoading && <span style={{ fontSize: 11, color: '#484f58' }} className="animate-pulse">scanning…</span>}
        <span style={{ fontSize: 11, color: '#30363d', marginLeft: 'auto' }}>{universe.length} symbols</span>
      </div>

      {/* ── Charts row — borderless, seamless ───────────────────────── */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', flexShrink: 0, padding: '12px 12px 0' }}>
        {/* Dividers between charts via borderRight */}
        <div style={{ borderRight: '1px solid #161b22', paddingRight: 8 }}>
          <div style={{ fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.1em', color: '#30363d', padding: '0 8px 4px' }}>
            <Tooltip text="Scatter of each symbol by Realised Vol (x) and Vol-of-Vol (y), colored by regime." icon>RV vs Vol-of-Vol</Tooltip>
          </div>
          {isLoading
            ? <div className="skeleton" style={{ height: 220, margin: '0 8px', borderRadius: 4 }} />
            : <EChart option={radar?.charts?.scatter ?? {}} height={220} />
          }
        </div>
        <div style={{ borderRight: '1px solid #161b22', paddingRight: 8 }}>
          <div style={{ fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.1em', color: '#30363d', padding: '0 8px 4px' }}>
            <Tooltip text="Bars show 21-day Realised Vol. Diamond markers show IV/RV ratio on right axis — above 1.0 means options price more than realised moves." icon>Realised Vol &amp; IV/RV</Tooltip>
          </div>
          {isLoading
            ? <div className="skeleton" style={{ height: 220, margin: '0 8px', borderRadius: 4 }} />
            : <EChart option={radar?.charts?.iv_rv ?? {}} height={220} />
          }
        </div>
        <div>
          <div style={{ fontSize: 10, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.1em', color: '#30363d', padding: '0 8px 4px' }}>
            <Tooltip text="21-day rolling correlation between spot returns and IV changes. Negative = normal equity. Near zero = Sticky Strike. Positive = Sticky Delta." icon>Spot-Vol Correlation</Tooltip>
          </div>
          {isLoading
            ? <div className="skeleton" style={{ height: 220, margin: '0 8px', borderRadius: 4 }} />
            : <EChart option={radar?.charts?.persistence ?? {}} height={220} />
          }
        </div>
      </div>

      {/* ── Divider ─────────────────────────────────────────────────── */}
      <div style={{ height: 1, background: '#161b22', margin: '12px 0 0', flexShrink: 0 }} />

      {/* ── Table — fills remaining space ────────────────────────────── */}
      <div style={{ flex: 1, overflowY: 'auto', minHeight: 0 }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              {[
                { label: 'Symbol' },
                { label: 'Regime' },
                { label: 'Sentiment', tip: 'Per-ticker mood derived from this ticker\'s own IV/RV percentiles. Calm <P25 · Trending mid · Elevated >P70 · Stressed >P85 · Panic >P95. Pill shows IV percentile vs trailing 252d.' },
                { label: 'Realised Vol', tip: '21-day annualised historical volatility.' },
                { label: 'Vol-of-Vol',   tip: 'Std dev of daily IV changes (annualised). High = unstable.' },
                { label: 'IV / RV',      tip: 'ATM IV ÷ RV. ≥1.25 green, ≥1.0 yellow, <1.0 red.' },
                { label: 'Conf.',        tip: 'Regime confidence multiplier. >1 boosts signals.' },
              ].map(({ label, tip }) => (
                <th key={label} style={{
                  padding: '8px 16px', textAlign: 'left',
                  fontSize: 10, fontWeight: 600, textTransform: 'uppercase',
                  letterSpacing: '0.08em', color: '#484f58',
                  background: '#0d1117',
                  borderBottom: '1px solid #161b22',
                  position: 'sticky', top: 0, zIndex: 1,
                  whiteSpace: 'nowrap',
                }}>
                  {tip ? <Tooltip text={tip} icon>{label}</Tooltip> : label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {regimeRows.length === 0 && !isLoading && (
              <tr>
                <td colSpan={7} style={{ color: '#30363d', textAlign: 'center', padding: '40px 0', fontSize: 13 }}>
                  No data — add symbols to the universe via the navbar
                </td>
              </tr>
            )}

            {letf.length > 0 && <SectionHeader label="Leveraged ETF · Vol Complex" />}
            {letf.map((row, i) => <SymbolRow key={`letf-${i}`} row={row} drillDown={drillDown} />)}

            {other.length > 0 && <SectionHeader label="Indices · MAG7" topBorder={letf.length > 0} />}
            {other.map((row, i) => <SymbolRow key={`other-${i}`} row={row} drillDown={drillDown} />)}
          </tbody>
        </table>
      </div>
    </div>
  )
}
