import { useState, useRef, useEffect, type ReactNode } from 'react'
import { createPortal } from 'react-dom'

interface Props {
  text: string
  children?: ReactNode
  /** If true, renders a small ⓘ icon as the trigger */
  icon?: boolean
  /** Custom color for the (i) icon (e.g., "#a804b3" or "red") */
  iconColor?: string 
}

export default function Tooltip({ text, children, icon = false, iconColor = '#a307c2' }: Props) {
  const [visible, setVisible] = useState(false)
  const [coords, setCoords]   = useState({ top: 0, left: 0 })
  const ref = useRef<HTMLSpanElement>(null)

  useEffect(() => {
    if (!visible || !ref.current) return
    const r = ref.current.getBoundingClientRect()
    const tooltipWidth = 288 // maxWidth in px (18rem)
    const margin = 8
    // Default: centred on icon
    let left = r.left + window.scrollX + r.width / 2
    // Clamp so tooltip doesn't overflow right edge
    const maxLeft = window.innerWidth - tooltipWidth / 2 - margin
    // Clamp so tooltip doesn't overflow left edge
    const minLeft = tooltipWidth / 2 + margin
    left = Math.min(maxLeft, Math.max(minLeft, left))
    setCoords({
      top:  r.top + window.scrollY - 8,
      left,
    })
  }, [visible])

  return (
    <span
      ref={ref}
      className="inline-flex items-center gap-0.5"
      onMouseEnter={() => setVisible(true)}
      onMouseLeave={() => setVisible(false)}
    >
      {children}
      {icon && (
  <span 
    style={{ color: iconColor }} 
    className={`${!iconColor ? 'text-text-faint' : ''} cursor-help text-[11px] font-bold leading-none select-none px-0.5`}
  >
    ℹ
  </span>

      )}
      {visible && text && createPortal(
        <span
          style={{
            position:  'fixed',
            bottom:    `calc(100vh - ${coords.top}px)`,
            left:      coords.left,
            transform: 'translateX(-50%)',
            zIndex:    9999,
            minWidth:  '14rem',
            maxWidth:  '18rem',
            pointerEvents: 'none',
          }}
          className="px-2.5 py-2 rounded text-xs text-text-primary leading-relaxed
                     bg-bg-card border border-border shadow-lg whitespace-normal"
        >
          {text}
          <span
            className="absolute top-full left-1/2 -translate-x-1/2 -mt-px
                       border-4 border-transparent border-t-border"
          />
        </span>,
        document.body
      )}
    </span>
  )
}