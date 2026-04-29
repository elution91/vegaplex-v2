/**
 * Interactive spot-vol scatter chart.
 * Click a point to exclude it from the OLS regression + correlation.
 * Click again to restore it. Excluded points render greyed out.
 */
import { useEffect, useRef, useState, useCallback } from 'react'
import * as echarts from 'echarts'

interface Props {
  /** Daily spot log-returns */
  returns: number[]
  /** Daily realised-vol values (same length as returns) */
  vol: number[]
  /** ISO date strings (same length) */
  dates: string[]
  symbol?: string
  height?: number
}

function ols(xs: number[], ys: number[]): { slope: number; intercept: number; r2: number } {
  const n = xs.length
  if (n < 3) return { slope: 0, intercept: 0, r2: 0 }
  const mx = xs.reduce((a, b) => a + b, 0) / n
  const my = ys.reduce((a, b) => a + b, 0) / n
  let sxx = 0, sxy = 0, syy = 0
  for (let i = 0; i < n; i++) {
    sxx += (xs[i] - mx) ** 2
    sxy += (xs[i] - mx) * (ys[i] - my)
    syy += (ys[i] - my) ** 2
  }
  const slope = sxx > 1e-12 ? sxy / sxx : 0
  const intercept = my - slope * mx
  const r2 = (sxx > 1e-12 && syy > 1e-12) ? (sxy / Math.sqrt(sxx * syy)) ** 2 : 0
  const corr = (sxx > 1e-12 && syy > 1e-12) ? sxy / Math.sqrt(sxx * syy) : 0
  return { slope, intercept, r2: corr < 0 ? -Math.sqrt(r2) : Math.sqrt(r2) } // return signed R for display
}

export default function SpotVolScatter({ returns, vol, dates, symbol = '', height = 260 }: Props) {
  const ref = useRef<HTMLDivElement>(null)
  const chartRef = useRef<echarts.ECharts | null>(null)
  const [excluded, setExcluded] = useState<Set<number>>(new Set())

  // Build scatter points: [return_i, delta_vol_i, date_i]
  const points = (() => {
    const volChanges = vol.slice(1).map((v, i) => v - vol[i])
    const rets = returns.slice(1)
    const ds = dates.slice(1)
    return rets.map((r, i) => ({ x: r, y: volChanges[i], date: ds[i]?.slice(0, 10) ?? '' }))
  })()

  const buildOption = useCallback(() => {
    const active = points.filter((_, i) => !excluded.has(i))
    const activeXs = active.map(p => p.x)
    const activeYs = active.map(p => p.y)
    const { slope, intercept, r2 } = ols(activeXs, activeYs)

    const xMin = points.length ? Math.min(...points.map(p => p.x)) : -0.05
    const xMax = points.length ? Math.max(...points.map(p => p.x)) : 0.05
    const regData = [
      [xMin, slope * xMin + intercept],
      [xMax, slope * xMax + intercept],
    ]

    const scatterActive = points
      .map((p, i) => ({ value: [p.x, p.y, i], date: p.date, excluded: false }))
      .filter((_, i) => !excluded.has(i))

    const scatterExcluded = points
      .map((p, i) => ({ value: [p.x, p.y, i], date: p.date, excluded: true }))
      .filter((_, i) => excluded.has(i))

    const excludedCount = excluded.size
    const subtextParts = [
      `vol β = ${slope.toFixed(2)}`,
      `R = ${r2.toFixed(2)}`,
      `n = ${active.length}`,
      excludedCount > 0 ? `(${excludedCount} excluded)` : '',
    ].filter(Boolean).join('   ')

    return {
      backgroundColor: '#0d1117',
      animation: false,
      title: {
        text: '',
        subtext: subtextParts,
        textStyle: { color: '#6e7681', fontSize: 11, fontWeight: 400 },
        subtextStyle: { color: '#8b949e', fontSize: 10 },
        left: 8, top: 6,
      },
      tooltip: {
        trigger: 'item',
        backgroundColor: '#161b22',
        borderColor: '#30363d',
        textStyle: { color: '#e6edf3', fontSize: 11 },
        formatter: (p: { value: number[]; data?: { date?: string; excluded?: boolean } }) => {
          if (!p.value || p.value.length < 2) return ''
          const date = (p.data as { date?: string })?.date ?? ''
          const excl = (p.data as { excluded?: boolean })?.excluded ? ' <span style="color:#484f58">[excluded]</span>' : ''
          return `${date}${excl}<br/>Return: <b>${p.value[0].toFixed(4)}</b><br/>ΔVol: <b>${p.value[1].toFixed(4)}</b>`
        },
      },
      grid: { left: 52, right: 16, top: 52, bottom: 38 },
      xAxis: {
        type: 'value',
        name: 'Spot Return',
        nameLocation: 'middle',
        nameGap: 26,
        nameTextStyle: { color: '#8b949e', fontSize: 10 },
        axisLabel: { color: '#8b949e', fontSize: 9 },
        axisLine: { lineStyle: { color: '#30363d' } },
        splitLine: { lineStyle: { color: '#161b22', type: 'dashed' as const } },
      },
      yAxis: {
        type: 'value',
        name: 'ΔVol',
        nameLocation: 'middle',
        nameGap: 40,
        nameTextStyle: { color: '#8b949e', fontSize: 10 },
        axisLabel: { color: '#8b949e', fontSize: 9 },
        axisLine: { lineStyle: { color: '#30363d' } },
        splitLine: { lineStyle: { color: '#161b22', type: 'dashed' as const } },
      },
      series: [
        {
          type: 'scatter',
          name: 'Active',
          data: scatterActive,
          symbolSize: 6,
          itemStyle: { color: '#00d4aa', opacity: 0.6 },
          emphasis: { itemStyle: { color: '#2DD4BF', opacity: 1, borderColor: '#fff', borderWidth: 1 } },
        },
        {
          type: 'scatter',
          name: 'Excluded',
          data: scatterExcluded,
          symbolSize: 6,
          itemStyle: { color: '#30363d', opacity: 0.5 },
          emphasis: { itemStyle: { color: '#484f58', opacity: 0.9, borderColor: '#8b949e', borderWidth: 1 } },
        },
        {
          type: 'line',
          name: 'Vol β',
          data: regData,
          symbol: 'none',
          lineStyle: { color: '#f85149', width: 1.5, type: 'dashed' as const },
          tooltip: { show: false },
        },
      ],
    }
  }, [points, excluded])

  // Init chart
  useEffect(() => {
    if (!ref.current) return
    if (!chartRef.current) {
      chartRef.current = echarts.init(ref.current, undefined, { renderer: 'canvas' })
    }
    chartRef.current.setOption(buildOption(), true)

    const chart = chartRef.current
    const handler = (params: { seriesIndex?: number; dataIndex?: number; data?: { value?: number[] } }) => {
      // Only respond to clicks on the two scatter series (index 0 = active, 1 = excluded)
      if (params.seriesIndex === 2) return // regression line
      const pt = params.data as { value?: number[] } | undefined
      if (!pt?.value || pt.value.length < 3) return
      const idx = Math.round(pt.value[2])
      setExcluded(prev => {
        const next = new Set(prev)
        if (next.has(idx)) next.delete(idx)
        else next.add(idx)
        return next
      })
    }
    chart.on('click', handler)
    return () => { chart.off('click', handler) }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Update option when data or excluded changes
  useEffect(() => {
    chartRef.current?.setOption(buildOption(), true)
  }, [buildOption])

  // Resize observer
  useEffect(() => {
    const el = ref.current
    if (!el) return
    const ro = new ResizeObserver(() => chartRef.current?.resize())
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  // Cleanup
  useEffect(() => {
    return () => { chartRef.current?.dispose(); chartRef.current = null }
  }, [])

  const hasData = points.length > 0

  return (
    <div className="relative" style={{ height }}>
      <div ref={ref} style={{ width: '100%', height }} />
      {!hasData && (
        <div className="absolute inset-0 flex items-center justify-center text-text-muted text-xs">
          Insufficient data
        </div>
      )}
      {excluded.size > 0 && (
        <button
          onClick={() => setExcluded(new Set())}
          className="absolute bottom-2 right-2 text-[10px] text-text-muted hover:text-text-primary
                     bg-bg-elevated border border-border rounded px-2 py-0.5 transition-colors"
        >
          Reset ({excluded.size} excluded)
        </button>
      )}
      <div className="absolute top-0 left-0 right-0 flex justify-center pt-1 pointer-events-none">
        <span className="text-[10px]" style={{ color: '#bbbcbe' }}>click point to exclude / restore</span>
      </div>
    </div>
  )
}
