interface Props {
  text: string | null | undefined
  label?: string
}

/** Split synthesis text into bullet points on sentence boundaries or newlines */
function toBullets(text: string): string[] {
  const lines = text
    .split(/\n|(?<=\.)\s+(?=[A-Z])/)
    .map((s) => s.trim())
    .filter((s) => s.length > 4)
  return lines
}

export default function SynthesisCard({ text, label = 'Key Takeaways' }: Props) {
  if (!text) return null
  const bullets = toBullets(text)

  return (
    <div className="card p-3 space-y-2">
      <div className="section-title">{label}</div>
      {bullets.length > 1 ? (
        <ul style={{ margin: 0, padding: 0, listStyle: 'none', display: 'flex', flexDirection: 'column', gap: 5 }}>
          {bullets.map((b, i) => (
            <li key={i} style={{ display: 'flex', gap: 8, alignItems: 'flex-start' }}>
              <span style={{ color: '#58a6ff', fontSize: 10, marginTop: 4, flexShrink: 0 }}>▸</span>
              <span style={{ fontSize: 13, color: '#8b949e', lineHeight: 1.6 }}>{b}</span>
            </li>
          ))}
        </ul>
      ) : (
        <p style={{ fontSize: 13, color: '#8b949e', lineHeight: 1.7, margin: 0 }}>{text}</p>
      )}
    </div>
  )
}
