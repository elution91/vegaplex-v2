import { ReactNode } from 'react'
import EChart from './EChart'
import EmptyState from '../shared/EmptyState'

interface Props {
  title?: string
  option?: Record<string, unknown> | null
  height?: number
  loading?: boolean
  children?: ReactNode   // use children to embed non-EChart content (e.g. Surface3D)
  className?: string
}

export default function ChartCard({ title, option, height = 260, loading = false, children, className }: Props) {
  return (
    <div className={`card overflow-hidden ${className ?? ''}`}>
      {title && (
        <div className="px-3 pt-2 pb-0 text-xs text-text-muted font-medium">{title}</div>
      )}
      {children ? (
        children
      ) : loading ? (
        <div className="animate-pulse bg-bg-elevated" style={{ height }} />
      ) : option ? (
        <EChart option={option} height={height} loading={loading} />
      ) : (
        <EmptyState />
      )}
    </div>
  )
}
