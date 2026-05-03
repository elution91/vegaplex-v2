import { NavLink, useNavigate, useLocation } from 'react-router-dom'
import { useState, useRef, useEffect, useCallback } from 'react'
import clsx from 'clsx'
import { useAppStore } from '../../store/useAppStore'
import { useScan } from '../../hooks/useScan'
import { useScanStore } from '../../store/useScanStore'

// ── Nav structure ──────────────────────────────────────────────────────────

type NavItem = { path: string; label: string }
type NavGroup = { label: string; items: NavItem[] } | { label: string; path: string }

const NAV: NavGroup[] = [
  {
    label: 'LETF Universe',
    items: [
      { path: '/radar', label: 'Radar' },
    ],
  },
  {
    label: 'Volatility',
    items: [
      { path: '/vol-desk', label: 'Surface & Skew' },
      { path: '/results',  label: 'Ticker Analysis' },
    ],
  },
  {
    label: 'VIX Regime',
    items: [
      { path: '/vix/status',    label: 'Status' },
      { path: '/vix/history',   label: 'Time Series' },
      { path: '/vix/analytics', label: 'Analytics' },
    ],
  },
  {
    label: 'Event IV',
    items: [
      { path: '/earnings/earnings', label: 'Earnings' },
      { path: '/earnings/macro',    label: 'Macro Events' },
    ],
  },
  { label: 'Resources', path: '/resources' },
]

// ── Category presets ───────────────────────────────────────────────────────

const CATEGORY_PRESETS: Record<string, string[]> = {
  indices:   ['SPY', 'QQQ', 'IWM', 'DIA', 'TLT', 'GLD', 'SLV', 'USO'],
  leveraged: ['TQQQ', 'SQQQ', 'UPRO', 'SPXU', 'TECL', 'LABU'],
  vol_etfs:  ['UVXY', 'SVXY', 'VXX', 'VIXY'],
  mega_cap:  ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'AMZN', 'META', 'GOOGL'],
}

const REFRESH_OPTIONS = [
  { label: '1m',  ms: 60_000 },
  { label: '5m',  ms: 300_000 },
  { label: '15m', ms: 900_000 },
  { label: '30m', ms: 1_800_000 },
]

const HISTORY_KEY = 'vp_search_history'
const MAX_HISTORY = 20

function loadHistory(): string[] {
  try { return JSON.parse(localStorage.getItem(HISTORY_KEY) ?? '[]') } catch { return [] }
}

function saveHistory(sym: string, prev: string[]): string[] {
  const next = [sym, ...prev.filter((s) => s !== sym)].slice(0, MAX_HISTORY)
  localStorage.setItem(HISTORY_KEY, JSON.stringify(next))
  return next
}

function removeFromHistory(sym: string, prev: string[]): string[] {
  const next = prev.filter((s) => s !== sym)
  localStorage.setItem(HISTORY_KEY, JSON.stringify(next))
  return next
}

// ── Search dropdown ────────────────────────────────────────────────────────

function SearchDropdown({
  history,
  filter,
  onSelect,
  onRemove,
  onClearAll,
}: {
  history:    string[]
  filter:     string
  onSelect:   (sym: string) => void
  onRemove:   (sym: string) => void
  onClearAll: () => void
}) {
  const filtered = filter
    ? history.filter((s) => s.startsWith(filter))
    : history

  if (filtered.length === 0 && !filter) return null

  return (
    <div style={{
      position:     'absolute',
      top:          'calc(100% + 6px)',
      left:         0,
      right:        0,
      background:   '#161b22',
      border:       '1px solid #21262d',
      borderRadius: 8,
      zIndex:       9999,
      overflow:     'hidden',
      boxShadow:    '0 8px 24px rgba(0,0,0,0.5)',
    }}>
      <div style={{
        display:        'flex',
        alignItems:     'center',
        justifyContent: 'space-between',
        padding:        '7px 12px 5px',
        borderBottom:   '1px solid #21262d',
      }}>
        <span style={{ fontSize: 11, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.07em', color: '#6e7681' }}>
          {filter ? `Matches for "${filter}"` : 'Recent Searches'}
        </span>
        {history.length > 0 && !filter && (
          <button
            onMouseDown={(e) => { e.preventDefault(); onClearAll() }}
            style={{ fontSize: 11, color: '#6e7681', background: 'none', border: 'none', cursor: 'pointer', padding: '0 2px' }}
          >
            Clear all
          </button>
        )}
      </div>

      {filtered.length === 0 ? (
        <div style={{ padding: '10px 12px', fontSize: 12, color: '#6e7681' }}>No matches</div>
      ) : (
        <div style={{ maxHeight: 280, overflowY: 'auto' }}>
          {filtered.map((sym) => (
            <div
              key={sym}
              style={{ display: 'flex', alignItems: 'center', padding: '6px 12px', cursor: 'pointer' }}
              onMouseDown={(e) => { e.preventDefault(); onSelect(sym) }}
              onMouseEnter={(e) => { (e.currentTarget as HTMLElement).style.background = 'rgba(255,255,255,0.04)' }}
              onMouseLeave={(e) => { (e.currentTarget as HTMLElement).style.background = '' }}
            >
              <span style={{ fontSize: 11, color: '#484f58', marginRight: 10, flexShrink: 0 }}>↺</span>
              <span style={{ flex: 1, fontSize: 13, fontWeight: 600, color: '#2DD4BF', fontFamily: 'inherit', letterSpacing: '0.02em' }}>
                {sym}
              </span>
              <button
                onMouseDown={(e) => { e.stopPropagation(); e.preventDefault(); onRemove(sym) }}
                style={{ fontSize: 14, color: '#484f58', background: 'none', border: 'none', cursor: 'pointer', lineHeight: 1, padding: '0 2px' }}
              >
                ×
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ── Secondary sub-nav bar ──────────────────────────────────────────────────

export function SubNav() {
  const location = useLocation()
  const activeGroup = NAV.find(
    (g) => 'items' in g && g.items.some((i) => location.pathname === i.path || location.pathname.startsWith(i.path + '/'))
  ) as (NavGroup & { items: NavItem[] }) | undefined

  if (!activeGroup) return null

  return (
    <div style={{ background: '#0d1117', borderBottom: '1px solid #21262d', paddingLeft: 16, display: 'flex', alignItems: 'center', gap: 2, height: 34 }}>
      <span style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.1em', color: '#484f58', marginRight: 8 }}>
        {activeGroup.label}
      </span>
      {activeGroup.items.map((item) => (
        <NavLink
          key={item.path}
          to={item.path}
          className={({ isActive }) => clsx('nav-tab-btn', isActive && 'active')}
          style={{ fontSize: 12, padding: '3px 10px' }}
        >
          {item.label}
        </NavLink>
      ))}
    </div>
  )
}

// ── Main Navbar ────────────────────────────────────────────────────────────

export default function Navbar() {
  const [input,        setInput]        = useState('')
  const [customTicker, setCustomTicker] = useState('')
  const [showUniverse, setShowUniverse] = useState(false)
  const [refreshMs,    setRefreshMs]    = useState(300_000)
  const [history,      setHistory]      = useState<string[]>(loadHistory)
  const [showHistory,  setShowHistory]  = useState(false)

  const timerRef      = useRef<ReturnType<typeof setInterval> | null>(null)
  const searchWrapRef = useRef<HTMLDivElement>(null)

  const { universe, setUniverse, autoRefresh, toggleAutoRefresh, setActiveSymbol } = useAppStore()
  const { scanSingle, scanUniverse } = useScan()
  const scanStatus = useScanStore((s) => s.status)
  const navigate = useNavigate()

  // Close search dropdown on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (searchWrapRef.current && !searchWrapRef.current.contains(e.target as Node)) {
        setShowHistory(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  useEffect(() => {
    if (timerRef.current) clearInterval(timerRef.current)
    if (autoRefresh) {
      timerRef.current = setInterval(() => scanUniverse(universe), refreshMs)
    }
    return () => { if (timerRef.current) clearInterval(timerRef.current) }
  }, [autoRefresh, refreshMs, universe, scanUniverse])

  const handleScan = useCallback((sym?: string) => {
    const s = (sym ?? input).trim().toUpperCase()
    if (!s) return
    setHistory((prev) => saveHistory(s, prev))
    setInput(s)
    setShowHistory(false)
    setActiveSymbol(s)
    scanSingle(s)
    navigate('/results')
  }, [input, setActiveSymbol, scanSingle, navigate])

  const handleSelect = (sym: string) => {
    setHistory((prev) => saveHistory(sym, prev))
    setInput(sym)
    setShowHistory(false)
    setActiveSymbol(sym)
    scanSingle(sym)
    navigate('/results')
  }

  const handleRemove = (sym: string) => {
    setHistory((prev) => removeFromHistory(sym, prev))
  }

  const handleClearAll = () => {
    localStorage.removeItem(HISTORY_KEY)
    setHistory([])
  }

  const handleUniverseScan = () => {
    scanUniverse(universe)
    navigate('/results')
  }

  const addTicker = () => {
    const sym = customTicker.trim().toUpperCase()
    if (!sym || universe.includes(sym)) return
    setUniverse([...universe, sym])
    setCustomTicker('')
  }

  const removeTicker = (sym: string) => setUniverse(universe.filter((t) => t !== sym))

  const toggleCategory = (category: string) => {
    const tickers = CATEGORY_PRESETS[category] ?? []
    const allPresent = tickers.every((t) => universe.includes(t))
    setUniverse(allPresent
      ? universe.filter((t) => !tickers.includes(t))
      : [...new Set([...universe, ...tickers])]
    )
  }

  const showDrop = showHistory && (history.length > 0 || input.length > 0)

  const location = useLocation()
  const UNIVERSE_PATHS = ['/radar', '/skew-arb', '/results']
  const isUniverseTab = UNIVERSE_PATHS.some((p) => location.pathname === p || location.pathname.startsWith(p + '/'))

  return (
    <div style={{ background: '#0d1117', borderBottom: '1px solid #161b22' }}>
      {/* ── Single row: Logo + Search + Nav + right controls ─────────── */}
      <div className="flex items-center gap-3 px-4" style={{ height: 48 }}>
        {/* Logo */}
        <div className="shrink-0 select-none flex items-center" style={{ minWidth: 110 }}>
          <img src="/logo-test.png" alt="νegaPlex" style={{ height: 110, width: 'auto', mixBlendMode: 'screen', filter: 'brightness(2) contrast(1.2)' }} />
        </div>

        {/* Search */}
        <div ref={searchWrapRef} style={{ flex: '0 1 420px', minWidth: 160, position: 'relative' }}>
          <div
            className="flex items-center gap-2 px-3 rounded-md transition-all duration-150"
            style={{ background: '#161b22', border: '1px solid transparent', paddingTop: 7, paddingBottom: 7 }}
          >
            <span style={{ fontSize: 12, color: '#ced1d4', flexShrink: 0 }}>⌕</span>
            <input
              value={input}
              onChange={(e) => setInput(e.target.value.toUpperCase())}
              onKeyDown={(e) => {
                if (e.key === 'Enter') handleScan()
                if (e.key === 'Escape') { setShowHistory(false); setInput('') }
              }}
              onFocus={(e) => {
                setShowHistory(true)
                ;(e.currentTarget.parentElement as HTMLElement).style.boxShadow = '0 0 0 2px rgba(88,166,255,0.25)'
              }}
              onBlur={(e) => {
                ;(e.currentTarget.parentElement as HTMLElement).style.boxShadow = ''
              }}
              placeholder="Search symbol…"
              disabled={scanStatus === 'running'}
              style={{ flex: 1, background: 'transparent', border: 'none', outline: 'none', fontSize: 13, color: '#e6edf3', caretColor: '#58a6ff' }}
            />
            {input && (
              <button
                onMouseDown={(e) => { e.preventDefault(); setInput(''); setShowHistory(true) }}
                style={{ fontSize: 14, color: '#484f58', background: 'none', border: 'none', cursor: 'pointer', lineHeight: 1, flexShrink: 0 }}
              >×</button>
            )}
            {scanStatus === 'running' && (
              <span style={{ fontSize: 12, color: '#58a6ff', flexShrink: 0 }} className="animate-pulse">scanning…</span>
            )}
          </div>

          {showDrop && (
            <SearchDropdown
              history={history}
              filter={input}
              onSelect={handleSelect}
              onRemove={handleRemove}
              onClearAll={handleClearAll}
            />
          )}
        </div>

        {/* Right controls */}
        <div className="flex items-center gap-2 shrink-0 ml-auto">
          <button
            onClick={() => setShowUniverse((v) => !v)}
            className={clsx('nav-tab-btn', showUniverse && 'active')}
            style={{ display: 'flex', alignItems: 'center', gap: 6 }}
          >
            <span>⚙</span>
            <span>{universe.length} tickers</span>
          </button>
          <NavLink
            to="/settings"
            className={({ isActive }) => clsx('nav-tab-btn', isActive && 'active')}
            title="Settings"
            style={{ display: 'flex', alignItems: 'center', gap: 5, padding: '4px 9px' }}
          >
            <svg width="13" height="13" viewBox="0 0 16 16" fill="currentColor" style={{ flexShrink: 0 }}>
              <path d="M1 3h14M1 8h14M1 13h14" stroke="currentColor" strokeWidth="2" strokeLinecap="round" fill="none"/>
              <circle cx="5"  cy="3"  r="2" fill="currentColor"/>
              <circle cx="11" cy="8"  r="2" fill="currentColor"/>
              <circle cx="5"  cy="13" r="2" fill="currentColor"/>
            </svg>
            <span>Settings</span>
          </NavLink>
        </div>

        {/* Nav — inline groups with sub-items */}
        <div style={{ flex: 1, display: 'flex', justifyContent: 'center' }}>
          <div className="flex items-center" style={{ gap: 2 }}>
            {NAV.map((group, gi) => {
              // Simple single-path item
              if ('path' in group) {
                return (
                  <NavLink
                    key={group.path}
                    to={group.path}
                    className={({ isActive }) => clsx('nav-tab-btn', isActive && 'active')}
                  >
                    {group.label}
                  </NavLink>
                )
              }
              // Group with sub-items — render label + sub-items inline
              return (
                <div key={group.label} style={{
                  display: 'flex', alignItems: 'center', gap: 1,
                  paddingLeft: gi > 0 ? 10 : 0,
                  marginLeft:  gi > 0 ? 8 : 0,
                  borderLeft:  gi > 0 ? '1px solid #30363d' : 'none',
                }}>
                  {/* Group label — white, bold */}
                  <span style={{
                    fontSize: 11, fontWeight: 700, textTransform: 'uppercase',
                    letterSpacing: '0.08em',
                    color: '#e6edf3',
                    paddingRight: 7, whiteSpace: 'nowrap',
                    transition: 'color 0.15s',
                  }}>
                    {group.label}
                  </span>
                  {/* Sub-items — muted */}
                  {group.items.map((item) => (
                    <NavLink
                      key={item.path}
                      to={item.path}
                      className={({ isActive }) => clsx('nav-tab-btn', isActive && 'active')}
                      style={{ fontSize: 12, padding: '4px 9px' }}
                    >
                      {item.label}
                    </NavLink>
                  ))}
                </div>
              )
            })}
          </div>
        </div>

        {/* Auto-refresh + Scan Universe — only on Universe routes */}
        <div className="flex items-center gap-2 shrink-0" style={{ visibility: isUniverseTab ? 'visible' : 'hidden' }}>
          <label className="flex items-center gap-1.5 cursor-pointer select-none" style={{ fontSize: 12, color: '#8b949e' }}>
            <input type="checkbox" checked={autoRefresh} onChange={toggleAutoRefresh} className="accent-accent" />
            Auto-refresh
          </label>
          {autoRefresh && (
            <select
              value={refreshMs}
              onChange={(e) => setRefreshMs(Number(e.target.value))}
              style={{ fontSize: 12, padding: '2px 6px', background: '#161b22', border: '1px solid #21262d', borderRadius: 5, color: '#e6edf3', outline: 'none' }}
            >
              {REFRESH_OPTIONS.map((o) => (
                <option key={o.ms} value={o.ms}>{o.label}</option>
              ))}
            </select>
          )}
          <button
            onClick={handleUniverseScan}
            disabled={scanStatus === 'running'}
            style={{ fontSize: 12, fontWeight: 600, padding: '4px 12px', borderRadius: 6,
                     background: '#1f6feb', color: '#e6edf3', border: 'none', cursor: 'pointer',
                     opacity: scanStatus === 'running' ? 0.4 : 1, transition: 'background 0.12s' }}
            onMouseEnter={(e) => { (e.currentTarget as HTMLElement).style.background = '#388bfd' }}
            onMouseLeave={(e) => { (e.currentTarget as HTMLElement).style.background = '#1f6feb' }}
          >
            Scan Universe
          </button>
        </div>
      </div>

      {/* ── Universe panel ───────────────────────────────────────────── */}
      {showUniverse && (
        <div className="px-4 py-3 space-y-3" style={{ background: '#161b22', borderTop: '1px solid #21262d' }}>
          <div className="flex items-center gap-2 flex-wrap">
            <span className="caption">Presets:</span>
            {Object.keys(CATEGORY_PRESETS).map((cat) => {
              const tickers = CATEGORY_PRESETS[cat]
              const active  = tickers.every((t) => universe.includes(t))
              return (
                <label key={cat} className="flex items-center gap-1 cursor-pointer" style={{ fontSize: 12 }}>
                  <input type="checkbox" checked={active} onChange={() => toggleCategory(cat)} className="accent-accent" />
                  <span style={{ color: active ? '#e6edf3' : '#8b949e' }}>{cat.replace('_', ' ')}</span>
                </label>
              )
            })}
          </div>
          <div className="flex flex-wrap gap-1" style={{ maxHeight: 80, overflowY: 'auto' }}>
            {universe.map((sym) => (
              <span key={sym} className="flex items-center gap-1 px-2 py-0.5 rounded" style={{ fontSize: 12, background: '#0d1117', color: '#e6edf3' }}>
                {sym}
                <button onClick={() => removeTicker(sym)} style={{ color: '#484f58', background: 'none', border: 'none', cursor: 'pointer', lineHeight: 1, fontSize: 14 }}>×</button>
              </span>
            ))}
          </div>
          <div className="flex items-center gap-2">
            <input
              value={customTicker}
              onChange={(e) => setCustomTicker(e.target.value.toUpperCase())}
              onKeyDown={(e) => e.key === 'Enter' && addTicker()}
              placeholder="Add ticker…"
              style={{ fontSize: 12, width: 100, padding: '4px 8px', background: '#0d1117', border: 'none', borderRadius: 5, color: '#e6edf3', outline: 'none' }}
            />
            <button onClick={addTicker} className="nav-tab-btn">Add</button>
          </div>
        </div>
      )}
    </div>
  )
}
