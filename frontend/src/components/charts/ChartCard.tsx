import { useRef, useState, ReactNode } from 'react'
import * as echarts from 'echarts/core'
import type { EChartsType } from 'echarts/types/dist/shared'
import EChart, { reviveOption } from './EChart'
import Tooltip from '../shared/Tooltip'

const EXPORT_W = 1200
const EXPORT_H = 500

export type StatItem = { label: string; value: string | number; color?: string }

interface Props {
  title?: string
  subtitle?: string         // short purpose statement under title
  tooltip?: string
  option?: Record<string, unknown> | null
  height?: number
  loading?: boolean
  children?: ReactNode
  className?: string
  exportName?: string
  controls?: ReactNode      // top-right chip toggles (e.g. Call/Put/RR)
  stats?: StatItem[]        // footer stats (Current / Mean / Std Dev)
}

function DownloadIcon() {
  return (
    <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M8 2v8M5 7l3 3 3-3" />
      <path d="M3 13h10" />
    </svg>
  )
}

function ExpandIcon() {
  return (
    <svg width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
      <path d="M3 7V3h4M13 9v4H9M3 9v4h4M13 7V3H9" />
    </svg>
  )
}

function CloseIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
      <path d="M3 3l10 10M13 3L3 13" />
    </svg>
  )
}

export default function ChartCard({
  title, subtitle, tooltip, option, height = 260, loading = false,
  children, className, exportName, controls, stats,
}: Props) {
  const chartRef = useRef<EChartsType | null>(null)
  const [fullscreen, setFullscreen] = useState(false)

  function handleDownload() {
    if (!option) return

    const container = document.createElement('div')
    container.style.cssText = `position:fixed;left:-9999px;top:0;width:${EXPORT_W}px;height:${EXPORT_H}px;`
    document.body.appendChild(container)

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const instance = (echarts as any).init(container, 'vegaplex', { width: EXPORT_W, height: EXPORT_H })
    // Revive stringified JS functions (formatters etc.) — backend serialises
    // them as strings; without this, formatters render as raw text in exports.
    const revived = reviveOption({ ...option, animation: false }) as Record<string, unknown>
    if (Array.isArray(revived.dataZoom)) {
      revived.dataZoom = (revived.dataZoom as Record<string, unknown>[])
        .filter((z) => z.type !== 'slider')
        .map((z) => ({ ...z, type: 'inside' }))
    }
    instance.setOption(revived)

    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        try {
          const url = instance.getDataURL({ type: 'png', pixelRatio: 2, backgroundColor: '#0d1117' })
          const a = document.createElement('a')
          a.href = url
          a.download = `${exportName ?? title ?? 'vegaplex'}.png`
          a.click()
        } finally {
          instance.dispose()
          document.body.removeChild(container)
        }
      })
    })
  }

  // ── Inner content (chart or skeleton) ─────────────────────────────────────
  const renderChart = (h: number) => (
    loading ? (
      <div className="skeleton" style={{ height: h, margin: '0 8px', borderRadius: 4 }} />
    ) : children ? (
      <div style={{ height: h }}>{children}</div>
    ) : option ? (
      <EChart
        option={option}
        height={h}
        onChartReady={(instance) => { chartRef.current = instance }}
      />
    ) : (
      <div className="skeleton" style={{ height: h, margin: '0 8px', borderRadius: 4, opacity: 0.4 }} />
    )
  )

  // ── Action buttons (download + expand) ────────────────────────────────────
  const actionBtnStyle = (color = '#484f58') => ({
    display: 'flex' as const, alignItems: 'center' as const, justifyContent: 'center' as const,
    width: 22, height: 22, borderRadius: 4,
    background: 'transparent', border: '1px solid transparent',
    color, cursor: 'pointer', transition: 'color 0.12s, border-color 0.12s',
  })

  const ActionButtons = (
    <>
      {option && !loading && (
        <>
          <button
            onClick={handleDownload}
            title="Export PNG"
            style={actionBtnStyle()}
            onMouseEnter={(e) => {
              const el = e.currentTarget as HTMLElement
              el.style.color = '#e6edf3'; el.style.borderColor = '#30363d'
            }}
            onMouseLeave={(e) => {
              const el = e.currentTarget as HTMLElement
              el.style.color = '#484f58'; el.style.borderColor = 'transparent'
            }}
          >
            <DownloadIcon />
          </button>
          <button
            onClick={() => setFullscreen(true)}
            title="Expand"
            style={actionBtnStyle()}
            onMouseEnter={(e) => {
              const el = e.currentTarget as HTMLElement
              el.style.color = '#e6edf3'; el.style.borderColor = '#30363d'
            }}
            onMouseLeave={(e) => {
              const el = e.currentTarget as HTMLElement
              el.style.color = '#484f58'; el.style.borderColor = 'transparent'
            }}
          >
            <ExpandIcon />
          </button>
        </>
      )}
    </>
  )

  // ── Header (title + subtitle + controls + actions) ────────────────────────
  const Header = (title || subtitle || controls) && (
    <div style={{ padding: '10px 12px 4px', display: 'flex', alignItems: 'flex-start', gap: 8 }}>
      <div style={{ flex: 1, minWidth: 0 }}>
        {title && (
          <div style={{ fontSize: 14, fontWeight: 600, color: '#e6edf3', letterSpacing: '0.01em', lineHeight: 1.3 }}>
            {tooltip ? <Tooltip text={tooltip} icon>{title}</Tooltip> : title}
          </div>
        )}
        {subtitle && (
          <div style={{ fontSize: 11, color: '#6e7681', marginTop: 2, lineHeight: 1.4 }}>
            {subtitle}
          </div>
        )}
      </div>
      {controls && <div style={{ flexShrink: 0 }}>{controls}</div>}
      <div style={{ display: 'flex', gap: 2, flexShrink: 0 }}>
        {ActionButtons}
      </div>
    </div>
  )

  // ── Stats footer ──────────────────────────────────────────────────────────
  const StatsFooter = stats && stats.length > 0 && (
    <div style={{
      padding: '6px 14px 10px', display: 'flex', flexWrap: 'wrap', gap: 16,
      fontSize: 11, fontFamily: 'JetBrains Mono, ui-monospace, monospace',
      borderTop: '1px solid #161b22',
    }}>
      {stats.map((s) => (
        <div key={s.label} style={{ display: 'flex', gap: 6 }}>
          <span style={{ color: '#6e7681' }}>{s.label}</span>
          <span style={{ color: s.color ?? '#e6edf3', fontWeight: 600 }}>{s.value}</span>
        </div>
      ))}
    </div>
  )

  return (
    <>
      <div className={`card overflow-hidden ${className ?? ''}`} style={{ position: 'relative', display: 'flex', flexDirection: 'column' }}>
        {Header}
        {/* If no header was rendered, show floating action buttons in top-right */}
        {!Header && option && !loading && (
          <div style={{ position: 'absolute', top: 6, right: 6, zIndex: 10, display: 'flex', gap: 2,
                        background: 'rgba(13,17,23,0.7)', borderRadius: 4 }}>
            {ActionButtons}
          </div>
        )}
        <div style={{ flex: 1, minHeight: 0 }}>
          {renderChart(height)}
        </div>
        {StatsFooter}
      </div>

      {/* ── Fullscreen modal ──────────────────────────────────────────── */}
      {fullscreen && (
        <div
          onClick={() => setFullscreen(false)}
          style={{
            position: 'fixed', inset: 0, zIndex: 9999,
            background: 'rgba(13,17,23,0.92)',
            display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 24,
          }}
        >
          <div
            onClick={(e) => e.stopPropagation()}
            style={{
              width: '100%', maxWidth: 1400, height: '100%', maxHeight: 900,
              background: '#0d1117', border: '1px solid #21262d', borderRadius: 8,
              display: 'flex', flexDirection: 'column', position: 'relative',
            }}
          >
            <div style={{ padding: '14px 18px', display: 'flex', alignItems: 'center', gap: 12, borderBottom: '1px solid #161b22' }}>
              <div style={{ flex: 1 }}>
                {title && <div style={{ fontSize: 16, fontWeight: 600, color: '#e6edf3' }}>{title}</div>}
                {subtitle && <div style={{ fontSize: 12, color: '#6e7681', marginTop: 3 }}>{subtitle}</div>}
              </div>
              {controls}
              <button
                onClick={() => setFullscreen(false)}
                style={{ ...actionBtnStyle('#8b949e'), width: 28, height: 28 }}
                onMouseEnter={(e) => { (e.currentTarget as HTMLElement).style.color = '#e6edf3' }}
                onMouseLeave={(e) => { (e.currentTarget as HTMLElement).style.color = '#8b949e' }}
              >
                <CloseIcon />
              </button>
            </div>
            <div style={{ flex: 1, minHeight: 0, padding: 8 }}>
              {option ? <EChart option={option} height="100%" /> : null}
            </div>
            {stats && stats.length > 0 && (
              <div style={{
                padding: '10px 18px', display: 'flex', flexWrap: 'wrap', gap: 24,
                fontSize: 12, fontFamily: 'JetBrains Mono, ui-monospace, monospace',
                borderTop: '1px solid #161b22',
              }}>
                {stats.map((s) => (
                  <div key={s.label} style={{ display: 'flex', gap: 8 }}>
                    <span style={{ color: '#6e7681' }}>{s.label}</span>
                    <span style={{ color: s.color ?? '#e6edf3', fontWeight: 600 }}>{s.value}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </>
  )
}
