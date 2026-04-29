import { ReactNode } from 'react'
import Tooltip from './Tooltip'

export type KPIIntent = 'positive' | 'negative' | 'neutral' | 'warn'

interface Props {
  label:    string
  value:    ReactNode
  sub?:     string         // one-line description under value
  intent?:  KPIIntent      // drives the dot + value color
  tooltip?: string
  badge?:   ReactNode       // small element to right of value (e.g. percentile pill)
}

const COLORS: Record<KPIIntent, string> = {
  positive: '#3fb950',
  negative: '#f85149',
  warn:     '#e3b341',
  neutral:  '#8b949e',
}

function Dot({ color }: { color: string }) {
  return (
    <span style={{
      width: 8, height: 8, borderRadius: '50%',
      background: color, flexShrink: 0, display: 'inline-block',
      boxShadow: `0 0 6px ${color}`,
    }} />
  )
}

export default function KPITile({ label, value, sub, intent = 'neutral', tooltip, badge }: Props) {
  const color = COLORS[intent]
  return (
    <div style={{
      padding: '14px 18px',
      display: 'flex', flexDirection: 'column', gap: 8,
      minHeight: 96,
    }}>
      <div style={{
        fontSize: 12, fontWeight: 600, color: '#e6edf3',
        letterSpacing: '0.01em',
      }}>
        {tooltip ? <Tooltip text={tooltip} icon>{label}</Tooltip> : label}
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <Dot color={color} />
        <span style={{
          fontSize: 22, fontWeight: 600, color,
          fontFamily: 'JetBrains Mono, ui-monospace, monospace',
          letterSpacing: '-0.01em',
        }}>
          {value}
        </span>
        {badge}
      </div>
      {sub && (
        <div style={{ fontSize: 11, color: '#6e7681', lineHeight: 1.4 }}>
          {sub}
        </div>
      )}
    </div>
  )
}
