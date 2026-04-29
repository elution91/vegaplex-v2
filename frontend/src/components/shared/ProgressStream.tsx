interface Props {
  done: number
  total: number
  currentTicker: string
  errors?: { ticker: string; message: string }[]
  visible: boolean
}

export default function ProgressStream({ done, total, currentTicker, errors = [], visible }: Props) {
  if (!visible) return null

  const pct = total > 0 ? Math.round((done / total) * 100) : 0

  return (
    <div className="card p-3 mb-3 space-y-2">
      <div className="flex items-center justify-between text-text-muted" style={{ fontSize: 13 }}>
        <span>
          Scanning{currentTicker ? ` — ${currentTicker}` : ''}
        </span>
        <span>{done} / {total}</span>
      </div>
      <div className="h-1 bg-bg-elevated rounded-full overflow-hidden">
        <div
          className="h-full bg-accent rounded-full transition-all duration-300"
          style={{ width: `${pct}%` }}
        />
      </div>
      {errors.length > 0 && (
        <div className="text-warning space-y-0.5" style={{ fontSize: 13 }}>
          {errors.slice(-3).map((e, i) => (
            <div key={i}>{e.ticker ? `[${e.ticker}] ` : ''}{e.message}</div>
          ))}
        </div>
      )}
    </div>
  )
}
