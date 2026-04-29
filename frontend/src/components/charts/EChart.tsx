import ReactECharts from 'echarts-for-react'
import * as echarts from 'echarts/core'
import type { EChartsType } from 'echarts/types/dist/shared'

// ── Global vegaplex dark theme ─────────────────────────────────────────────
// Registered once; all EChart instances reference it via theme="vegaplex"

echarts.registerTheme('vegaplex', {
  backgroundColor: '#0d1117',
  textStyle: { color: '#8b949e', fontFamily: 'Inter, ui-sans-serif, sans-serif', fontSize: 11 },
  title: {
    textStyle:    { color: '#8b949e', fontSize: 11, fontWeight: 600, letterSpacing: '0.06em' },
    subtextStyle: { color: '#484f58', fontSize: 10 },
  },
  legend: {
    textStyle: { color: '#8b949e', fontSize: 11 },
    pageTextStyle: { color: '#8b949e' },
  },
  categoryAxis: {
    axisLine:  { lineStyle: { color: '#21262d' } },
    axisTick:  { lineStyle: { color: '#21262d' } },
    axisLabel: { color: '#6e7681', fontSize: 10 },
    splitLine: { lineStyle: { color: '#161b22', type: 'dashed' } },
    splitArea: { areaStyle: { color: ['transparent', 'rgba(255,255,255,0.01)'] } },
  },
  valueAxis: {
    axisLine:  { lineStyle: { color: '#21262d' } },
    axisTick:  { lineStyle: { color: '#21262d' } },
    axisLabel: { color: '#6e7681', fontSize: 10 },
    splitLine: { lineStyle: { color: '#161b22', type: 'dashed' } },
    nameTextStyle: { color: '#6e7681', fontSize: 10 },
  },
  tooltip: {
    backgroundColor: '#161b22',
    borderColor:     '#30363d',
    borderWidth:     1,
    textStyle:       { color: '#e6edf3', fontSize: 11 },
    axisPointer:     { lineStyle: { color: '#30363d' }, crossStyle: { color: '#30363d' } },
  },
  grid: { borderColor: '#21262d' },
  line:    { smooth: true },
  bar:     { itemStyle: { borderRadius: [2, 2, 0, 0] } },
  scatter: { itemStyle: {} },
  color: ['#58a6ff', '#3fb950', '#FACC15', '#f85149', '#2DD4BF', '#a371f7', '#fb8500'],
})

// ── Watermark ─────────────────────────────────────────────────────────────

interface Props {
  option: Record<string, unknown>
  height?: string | number
  className?: string
  loading?: boolean
  onChartReady?: (instance: EChartsType) => void
}

const LOADING_OPTS = {
  text:      '',
  color:     '#58a6ff',
  textColor: '#8b949e',
  maskColor: 'rgba(13,17,23,0.6)',
  zlevel:    0,
}

const WATERMARK_GRAPHIC = {
  type: 'image',
  left: 'center',
  top:  'middle',
  z:    -1,
  style: { image: '/logo-test.png', width: 110, height: 33, opacity: 0.055 },
}

/**
 * ECharts serialises formatter functions as strings when coming from the Python
 * backend. This recursively converts any string that looks like a JS function
 * back into a real callable so tooltips work correctly.
 */
export function reviveOption(obj: unknown): unknown {
  if (typeof obj === 'string') {
    const t = obj.trim()
    if (t.startsWith('function') || t.startsWith('(function')) {
      try { return new Function('return ' + t)() } catch { /* leave as-is */ }
    }
    return obj
  }
  if (Array.isArray(obj)) return obj.map(reviveOption)
  if (obj !== null && typeof obj === 'object') {
    const out: Record<string, unknown> = {}
    for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
      out[k] = reviveOption(v)
    }
    return out
  }
  return obj
}

export default function EChart({ option, height = 260, className, loading = false, onChartReady }: Props) {
  const existingGraphic = option.graphic
  const mergedGraphic   = existingGraphic
    ? (Array.isArray(existingGraphic)
        ? [WATERMARK_GRAPHIC, ...existingGraphic]
        : [WATERMARK_GRAPHIC, existingGraphic])
    : [WATERMARK_GRAPHIC]

  const mergedOption = reviveOption({ ...option, graphic: mergedGraphic }) as Record<string, unknown>

  return (
    <ReactECharts
      option={mergedOption}
      style={{ height, width: '100%' }}
      className={className}
      showLoading={loading}
      loadingOption={LOADING_OPTS}
      notMerge
      lazyUpdate={false}
      theme="vegaplex"
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      onChartReady={onChartReady as any}
    />
  )
}
