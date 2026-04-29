/** Shared percentile badge — green low, yellow mid, red high.
 *  Use `invert` for metrics where low = bad (e.g. carry ratio). */
export default function PctPill({
  p,
  invert = false,
}: {
  p: number | null | undefined
  invert?: boolean
}) {
  if (p == null) return <span style={{ color: '#484f58', fontSize: 11 }}>—</span>
  const danger = invert ? p < 20 : p > 75
  const warn   = invert ? p < 35 : p > 50
  const color  = danger ? '#f85149' : warn ? '#e3b341' : '#3fb950'
  const bg     = danger ? 'rgba(248,81,73,0.15)' : warn ? 'rgba(227,179,65,0.13)' : 'rgba(63,185,80,0.13)'
  return (
    <span style={{
      display: 'inline-block', padding: '1px 6px', borderRadius: 4,
      fontSize: 11, fontWeight: 600, fontFamily: 'JetBrains Mono, monospace',
      color, background: bg,
    }}>
      P{Math.round(p)}
    </span>
  )
}
