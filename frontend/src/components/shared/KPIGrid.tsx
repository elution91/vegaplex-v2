import { ReactNode, Children, isValidElement } from 'react'

interface Props {
  cols?: number
  children: ReactNode
}

/**
 * Renders KPITile children in a uniform grid with thin internal dividers
 * (matches the oquants/Glassnode tile pattern). The outer card border holds
 * the whole group; cells get a 1px right/bottom border that we strip on edges.
 */
export default function KPIGrid({ cols = 3, children }: Props) {
  const items = Children.toArray(children).filter(isValidElement)
  const total = items.length

  return (
    <div className="card" style={{
      display: 'grid',
      gridTemplateColumns: `repeat(${cols}, 1fr)`,
      overflow: 'hidden',
    }}>
      {items.map((child, i) => {
        const colIdx  = i % cols
        const rowIdx  = Math.floor(i / cols)
        const isLastCol = colIdx === cols - 1
        const isLastRow = rowIdx === Math.floor((total - 1) / cols)
        return (
          <div
            key={i}
            style={{
              borderRight:  isLastCol ? 'none' : '1px solid #21262d',
              borderBottom: isLastRow ? 'none' : '1px solid #21262d',
            }}
          >
            {child}
          </div>
        )
      })}
    </div>
  )
}
