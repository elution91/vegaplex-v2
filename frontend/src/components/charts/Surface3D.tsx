/**
 * WebGL 3D volatility surface using echarts-gl.
 * Replaces the previous Plotly implementation.
 */
import { useEffect, useRef } from 'react'
import * as echarts from 'echarts'
import 'echarts-gl'

interface RawSurface {
  strike_grid: number[]        // 1-D: shape (S,)
  tte_grid:    number[]        // 1-D: shape (T,)
  surface:     (number | null)[][]  // 2-D: shape (T, S)
  symbol?:     string
  option_type?: string
}

interface Props {
  rawSurface?: RawSurface | null
  height?: number
}

// Vegaplex theme colormap: navy → blue → teal → amber → red
const COLORMAP = [
  [0,    '#0d1117'],  // bg-base
  [0.20, '#1f6feb'],  // accent-dim
  [0.40, '#58a6ff'],  // accent
  [0.58, '#39d353'],  // teal
  [0.75, '#d29922'],  // warning
  [0.88, '#f0883e'],  // orange bridge
  [1,    '#f85149'],  // negative
]

export default function Surface3D({ rawSurface, height = 520 }: Props) {
  const ref = useRef<HTMLDivElement>(null)
  const chartRef = useRef<echarts.ECharts | null>(null)

  useEffect(() => {
    if (!ref.current) return
    if (!chartRef.current) {
      chartRef.current = echarts.init(ref.current, undefined, { renderer: 'canvas' })
    }
    const chart = chartRef.current

    if (!rawSurface?.strike_grid || !rawSurface?.tte_grid || !rawSurface?.surface) {
      chart.setOption({
        backgroundColor: '#0d1117',
        graphic: [{
          type: 'text',
          left: 'center', top: 'middle',
          style: { text: 'No surface data', fill: '#484f58', fontSize: 13 },
        }],
      })
      return
    }

    const { strike_grid, tte_grid, surface, symbol = '', option_type = 'call' } = rawSurface

    // strikes: 1-D (S,), ttes: 1-D (T,), surface: 2-D (T x S)
    // Flatten into [strike, tte, iv] triples
    const data: [number, number, number][] = []
    const T = tte_grid.length
    const S = strike_grid.length

    // Convert TTE (years) → months for a readable Y axis
    const tte_months = tte_grid.map(t => Math.round(t * 12 * 10) / 10)

    for (let i = 0; i < T; i++) {
      for (let j = 0; j < S; j++) {
        const iv = surface[i]?.[j]
        if (iv == null || isNaN(iv) || iv <= 0) continue
        data.push([strike_grid[j], tte_months[i], iv])
      }
    }

    // Axis ranges
    const strikes = data.map(d => d[0])
    const ttes    = data.map(d => d[1])
    const ivs     = data.map(d => d[2])
    const sMin = Math.min(...strikes), sMax = Math.max(...strikes)
    const tMin = Math.min(...ttes),    tMax = Math.max(...ttes)
    const vMin = Math.min(...ivs),     vMax = Math.max(...ivs)

    const title = `${option_type.charAt(0).toUpperCase() + option_type.slice(1)} Vol Surface — ${symbol}`

    chart.setOption({
      backgroundColor: '#0d1117',
      title: {
        text: title,
        textStyle: { color: '#6e7681', fontSize: 11, fontFamily: 'Inter, system-ui, sans-serif', fontWeight: 400 },
        left: 8, top: 6,
      },
      tooltip: { show: false },
      visualMap: {
        show: true,
        dimension: 2,
        min: vMin,
        max: vMax,
        inRange: { color: COLORMAP.map(c => c[1] as string) },
        textStyle: { color: '#6e7681', fontSize: 10 },
        itemWidth: 10,
        itemHeight: 80,
        right: 8,
        top: 'middle',
        formatter: (v: number) => v.toFixed(2),
      },
      grid3D: {
        boxWidth:  160,
        boxDepth:  100,
        boxHeight: 70,
        environment: '#0d1117',
        viewControl: {
          autoRotate: false,
          distance:   200,
          alpha:      30,
          beta:       45,
          rotateSensitivity: 1,
          zoomSensitivity:   1,
        },
        light: {
          main:    { intensity: 1.6, shadow: false },
          ambient: { intensity: 0.5 },
        },
        axisLine:  { lineStyle: { color: '#30363d' } },
        axisLabel: { textStyle: { color: '#6e7681', fontSize: 9 } },
        axisTick:  { lineStyle: { color: '#30363d' } },
        splitLine: { lineStyle: { color: '#21262d', opacity: 0.6 } },
        postEffect: {
          enable: true,
          SSAO: { enable: true, radius: 2, intensity: 1.0, quality: 'medium' },
        },
      },
      xAxis3D: {
        name: 'Strike',
        type: 'value',
        min: sMin, max: sMax,
        nameTextStyle: { color: '#8b949e', fontSize: 10 },
      },
      yAxis3D: {
        name: 'Expiry (months)',
        type: 'value',
        min: tMin, max: tMax,
        nameTextStyle: { color: '#8b949e', fontSize: 10 },
        axisLabel: { formatter: (v: number) => `${Math.round(v)}M` },
      },
      zAxis3D: {
        name: 'IV',
        type: 'value',
        min: vMin, max: vMax,
        nameTextStyle: { color: '#8b949e', fontSize: 10 },
      },
      series: [{
        type: 'surface',
        data,
        shading: 'lambert',
        wireframe: {
          show: true,
          lineStyle: { color: 'rgba(255,255,255,0.06)', width: 0.5 },
        },
        itemStyle: { opacity: 0.92 },
        encode: { x: 0, y: 1, z: 2 },
      }],
    })
  }, [rawSurface])

  // Resize on container size change
  useEffect(() => {
    const el = ref.current
    if (!el) return
    const ro = new ResizeObserver(() => chartRef.current?.resize())
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  useEffect(() => {
    return () => { chartRef.current?.dispose(); chartRef.current = null }
  }, [])

  if (!rawSurface) {
    return (
      <div
        className="flex items-center justify-center text-text-muted text-xs bg-bg-elevated rounded"
        style={{ height }}
      >
        No surface data
      </div>
    )
  }

  return <div ref={ref} style={{ width: '100%', height }} />
}
