import { NavLink, useNavigate } from 'react-router-dom'
import { useState } from 'react'
import clsx from 'clsx'
import { useAppStore } from '../../store/useAppStore'
import { useScan } from '../../hooks/useScan'
import { useScanStore } from '../../store/useScanStore'

const TABS = [
  { path: '/radar',    label: 'Radar' },
  { path: '/vol-desk', label: 'Vol Desk' },
  { path: '/results',  label: 'Results' },
  { path: '/vix',      label: 'VIX' },
  { path: '/earnings', label: 'Earnings' },
]

export default function Navbar() {
  const [input, setInput] = useState('')
  const { setActiveSymbol } = useAppStore()
  const { universe } = useAppStore()
  const { scanSingle, scanUniverse } = useScan()
  const scanStatus = useScanStore((s) => s.status)
  const navigate = useNavigate()

  const handleScan = () => {
    const sym = input.trim().toUpperCase()
    if (!sym) return
    setActiveSymbol(sym)
    scanSingle(sym)
    navigate('/results')
  }

  const handleUniverseScan = () => {
    scanUniverse(universe)
    navigate('/results')
  }

  return (
    <nav className="flex items-center gap-2 px-4 py-2 border-b border-border bg-bg-card">
      {/* Logo */}
      <span className="text-sm font-semibold text-accent mr-2 tracking-tight">νegaPlex</span>

      {/* Tab links */}
      <div className="flex gap-1">
        {TABS.map((t) => (
          <NavLink
            key={t.path}
            to={t.path}
            className={({ isActive }) => clsx('nav-tab-btn', isActive && 'active')}
          >
            {t.label}
          </NavLink>
        ))}
      </div>

      {/* Spacer */}
      <div className="flex-1" />

      {/* Scan bar */}
      <div className="flex items-center gap-2">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value.toUpperCase())}
          onKeyDown={(e) => e.key === 'Enter' && handleScan()}
          placeholder="Symbol…"
          className="w-24 px-2 py-1 text-xs bg-bg-elevated border border-border rounded
                     text-text-primary placeholder-text-faint focus:outline-none focus:border-accent"
        />
        <button
          onClick={handleScan}
          disabled={scanStatus === 'running'}
          className="nav-tab-btn disabled:opacity-40"
        >
          Scan
        </button>
        <button
          onClick={handleUniverseScan}
          disabled={scanStatus === 'running'}
          className="nav-tab-btn disabled:opacity-40"
        >
          Scan Universe
        </button>
        {scanStatus === 'running' && (
          <span className="text-xs text-accent animate-pulse">scanning…</span>
        )}
      </div>
    </nav>
  )
}
