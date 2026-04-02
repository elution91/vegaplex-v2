import { useVix } from '../hooks/useVix'
import ChartCard from '../components/charts/ChartCard'
import MetricCard from '../components/shared/MetricCard'
import EmptyState from '../components/shared/EmptyState'
import clsx from 'clsx'

type VixData = {
  kpis?: Record<string, number | string>
  carry_on?: boolean
  strategy_banner?: { text?: string }
  synthesis?: string
  charts?: Record<string, Record<string, unknown>>
}

const CHART_DEFS = [
  { key: 'term_structure',  label: 'Term Structure',   height: 280 },
  { key: 'ratio_history',   label: 'Carry Ratio',       height: 280 },
  { key: 'vrp',             label: 'VRP',               height: 240 },
  { key: 'pca',             label: 'PCA Loadings',      height: 240 },
  { key: 'slope_history',   label: 'Slope History',     height: 240 },
  { key: 'roll_cost',       label: 'Roll Cost/Yield',   height: 240 },
  { key: 'outcomes',        label: 'Historical Outcomes', height: 240 },
  { key: 'percentile',      label: 'VIX Percentile',    height: 240 },
]

export default function VIXView() {
  const { data, isLoading, error, refetch } = useVix()
  const vix = data as VixData | undefined

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h2 className="text-sm font-semibold text-text-primary">VIX Futures & Carry</h2>
        <button onClick={() => refetch()} className="nav-tab-btn text-xs">
          Refresh
        </button>
      </div>

      {/* Strategy banner */}
      {vix?.strategy_banner && (
        <div className={clsx(vix.carry_on ? 'banner-carry-on' : 'banner-carry-off')}>
          {vix.strategy_banner.text ?? (vix.carry_on ? 'CARRY ON' : 'CARRY OFF')}
        </div>
      )}

      {/* KPI row */}
      {vix?.kpis && (
        <div className="grid grid-cols-4 gap-2 xl:grid-cols-8">
          {Object.entries(vix.kpis).map(([k, v]) => (
            <MetricCard key={k} label={k.replace(/_/g, ' ')} value={String(v)} />
          ))}
        </div>
      )}

      {/* Charts grid */}
      {isLoading ? (
        <div className="grid grid-cols-2 gap-3 xl:grid-cols-4">
          {CHART_DEFS.map((c) => (
            <div key={c.key} className="card animate-pulse" style={{ height: c.height }} />
          ))}
        </div>
      ) : error ? (
        <EmptyState message={`VIX data unavailable: ${error}`} />
      ) : (
        <div className="grid grid-cols-2 gap-3 xl:grid-cols-4">
          {CHART_DEFS.map((c) => (
            <ChartCard
              key={c.key}
              option={vix?.charts?.[c.key] ?? null}
              height={c.height}
            />
          ))}
        </div>
      )}

      {/* Synthesis */}
      {vix?.synthesis && (
        <div className="card p-3 text-xs text-text-muted leading-relaxed">
          {vix.synthesis}
        </div>
      )}
    </div>
  )
}
