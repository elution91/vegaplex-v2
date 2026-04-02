/**
 * Bridge component — renders the Plotly 3D surface figure returned by
 * build_3d_surface_figure() in dashboard_helpers.py.
 * Will be replaced by echarts-gl in a later phase.
 */
import { lazy, Suspense } from 'react'

// Plotly is large — lazy load it
const Plot = lazy(() => import('react-plotly.js'))

interface Props {
  figure: { data: unknown[]; layout: Record<string, unknown> } | null
  height?: number
}

export default function Surface3D({ figure, height = 420 }: Props) {
  if (!figure) {
    return (
      <div
        className="flex items-center justify-center text-text-muted text-xs bg-bg-elevated rounded"
        style={{ height }}
      >
        No surface data
      </div>
    )
  }

  return (
    <Suspense fallback={<div style={{ height }} className="bg-bg-elevated rounded animate-pulse" />}>
      <Plot
        data={figure.data as Plotly.Data[]}
        layout={{
          height,
          paper_bgcolor: '#161b22',
          plot_bgcolor:  '#161b22',
          font: { color: '#8b949e', family: 'JetBrains Mono, monospace', size: 11 },
          margin: { l: 0, r: 0, t: 40, b: 0 },
          ...figure.layout,
        }}
        config={{ displayModeBar: false, responsive: true }}
        style={{ width: '100%' }}
        useResizeHandler
      />
    </Suspense>
  )
}
