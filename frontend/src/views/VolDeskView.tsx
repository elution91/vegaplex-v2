import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getSurfaceCharts } from '../api/surfaceApi'
import { getRegimeCharts, classifyRegime } from '../api/regimeApi'
import { useAppStore } from '../store/useAppStore'
import ChartCard from '../components/charts/ChartCard'
import Surface3D from '../components/charts/Surface3D'
import MetricCard from '../components/shared/MetricCard'
import clsx from 'clsx'

type Tab = 'surface' | 'skew' | 'regime'

type SurfaceData = {
  surface_3d?: { data: unknown[]; layout: Record<string, unknown> }
  smile?: Record<string, unknown>
  term_structure?: Record<string, unknown>
  expiries?: string[]
  symbol?: string
}

type RegimeData = {
  regime?: Record<string, unknown>
  charts?: Record<string, Record<string, unknown>>
}

export default function VolDeskView() {
  const { activeSymbol, setActiveSymbol } = useAppStore()
  const [tab, setTab] = useState<Tab>('surface')
  const [input, setInput] = useState(activeSymbol)

  const { data: surfaceData, isLoading: surfaceLoading, refetch: refetchSurface } = useQuery({
    queryKey: ['surface', activeSymbol],
    queryFn: () => getSurfaceCharts(activeSymbol),
    staleTime: 5 * 60_000,
  })

  const { data: regimeData, isLoading: regimeLoading } = useQuery({
    queryKey: ['regime', activeSymbol],
    queryFn: () => getRegimeCharts(activeSymbol),
    enabled: tab === 'regime',
    staleTime: 5 * 60_000,
  })

  const surface = surfaceData as SurfaceData | undefined
  const regime  = regimeData  as RegimeData  | undefined

  const handleLoad = () => {
    const sym = input.trim().toUpperCase()
    if (!sym) return
    setActiveSymbol(sym)
    refetchSurface()
  }

  const TABS: { id: Tab; label: string }[] = [
    { id: 'surface', label: 'Surface' },
    { id: 'skew',    label: 'Skew' },
    { id: 'regime',  label: 'Regime' },
  ]

  return (
    <div className="space-y-4">
      {/* Symbol bar */}
      <div className="flex items-center gap-2">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value.toUpperCase())}
          onKeyDown={(e) => e.key === 'Enter' && handleLoad()}
          placeholder="Symbol…"
          className="w-24 px-2 py-1 text-xs bg-bg-elevated border border-border rounded
                     text-text-primary placeholder-text-faint focus:outline-none focus:border-accent"
        />
        <button onClick={handleLoad} className="nav-tab-btn">Load</button>
        <span className="text-xs text-text-muted">{activeSymbol}</span>
        <div className="flex gap-1 ml-4">
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

      {/* Surface tab */}
      {tab === 'surface' && (
        <div className="space-y-3">
          <ChartCard height={420} loading={surfaceLoading}>
            <Surface3D figure={surface?.surface_3d ?? null} height={420} />
          </ChartCard>
          <div className="grid grid-cols-2 gap-3">
            <ChartCard option={surface?.smile ?? null}          height={260} loading={surfaceLoading} />
            <ChartCard option={surface?.term_structure ?? null} height={260} loading={surfaceLoading} />
          </div>
        </div>
      )}

      {/* Skew tab — smile curves, placeholder for full skew panel */}
      {tab === 'skew' && (
        <div className="grid grid-cols-2 gap-3">
          <ChartCard option={surface?.smile ?? null}          height={280} loading={surfaceLoading} />
          <ChartCard option={surface?.term_structure ?? null} height={280} loading={surfaceLoading} />
        </div>
      )}

      {/* Regime tab */}
      {tab === 'regime' && (
        <div className="space-y-3">
          {regime?.regime && (
            <div className="grid grid-cols-4 gap-2">
              {(['regime', 'sentiment', 'description', 'recommendation'] as const).map((k) => (
                <MetricCard key={k} label={k} value={String((regime.regime as Record<string, unknown>)[k] ?? '—')} />
              ))}
            </div>
          )}
          <div className="grid grid-cols-3 gap-3">
            <ChartCard option={regime?.charts?.spot_vol     ?? null} height={260} loading={regimeLoading} />
            <ChartCard option={regime?.charts?.correlation  ?? null} height={260} loading={regimeLoading} />
            <ChartCard option={regime?.charts?.sqrt_t       ?? null} height={260} loading={regimeLoading} />
          </div>
        </div>
      )}
    </div>
  )
}
