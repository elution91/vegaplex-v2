import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { getRadar } from '../api/radarApi'
import { useAppStore } from '../store/useAppStore'
import ChartCard from '../components/charts/ChartCard'
import EmptyState from '../components/shared/EmptyState'

type RadarData = {
  universe_table?: Record<string, unknown>[]
  charts?: Record<string, Record<string, unknown>>
}

export default function RadarView() {
  const universe = useAppStore((s) => s.universe)
  const [lookback, setLookback] = useState(252)
  const [enabled, setEnabled] = useState(false)

  const { data, isLoading, refetch } = useQuery({
    queryKey: ['radar', universe, lookback],
    queryFn: () => getRadar(universe, lookback),
    enabled,
    staleTime: 10 * 60_000,
  })

  const radar = data as RadarData | undefined

  return (
    <div className="space-y-4">
      {/* Controls */}
      <div className="flex items-center gap-3">
        <h2 className="text-sm font-semibold text-text-primary">Universe Radar</h2>
        <select
          value={lookback}
          onChange={(e) => setLookback(Number(e.target.value))}
          className="px-2 py-1 text-xs bg-bg-elevated border border-border rounded text-text-primary focus:outline-none focus:border-accent"
        >
          <option value={63}>3M</option>
          <option value={126}>6M</option>
          <option value={252}>1Y</option>
        </select>
        <button
          onClick={() => { setEnabled(true); refetch() }}
          className="nav-tab-btn"
          disabled={isLoading}
        >
          {isLoading ? 'Loading…' : 'Refresh'}
        </button>
        <span className="text-xs text-text-muted">{universe.length} tickers</span>
      </div>

      {!enabled && <EmptyState message="Click Refresh to load radar data" />}

      {enabled && (
        <>
          {/* Chart row */}
          <div className="grid grid-cols-3 gap-3">
            <ChartCard option={radar?.charts?.scatter ?? null}    height={320} loading={isLoading} />
            <ChartCard option={radar?.charts?.iv_rv ?? null}      height={320} loading={isLoading} />
            <ChartCard option={radar?.charts?.persistence ?? null} height={320} loading={isLoading} />
          </div>

          {/* Universe table */}
          {radar?.universe_table && radar.universe_table.length > 0 && (
            <div className="card overflow-auto max-h-64">
              <table className="vp-table">
                <thead>
                  <tr>
                    {Object.keys(radar.universe_table[0]).map((k) => (
                      <th key={k}>{k}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {radar.universe_table.map((row, i) => (
                    <tr key={i}>
                      {Object.values(row).map((v, j) => (
                        <td key={j}>{String(v ?? '—')}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  )
}
