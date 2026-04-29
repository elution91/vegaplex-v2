import clsx from 'clsx'

interface Props {
  label: string
  value: string | number | null | undefined
  sub?: string
  valueClass?: string
}

export default function MetricCard({ label, value, sub, valueClass }: Props) {
  return (
    <div className="metric-card">
      <div className="metric-label">{label}</div>
      <div className={clsx('metric-value', valueClass)}>
        {value ?? '—'}
      </div>
      {sub && <div className="caption">{sub}</div>}
    </div>
  )
}
