import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface AppState {
  // Universe
  universe: string[]
  setUniverse: (tickers: string[]) => void

  // Active symbol (Vol Desk / scan input)
  activeSymbol: string
  setActiveSymbol: (s: string) => void

  // Auto-refresh
  autoRefresh: boolean
  toggleAutoRefresh: () => void

  // Broker config
  dataSource: 'yfinance' | 'ibkr'
  setDataSource: (s: 'yfinance' | 'ibkr') => void
  ibkrHost: string
  ibkrPort: number
  setIbkrHost: (h: string) => void
  setIbkrPort: (p: number) => void
}

const DEFAULT_UNIVERSE = [
  'SPY', 'QQQ', 'IWM', 'TLT', 'GLD', 'SLV', 'USO',
  'TQQQ', 'SQQQ', 'UPRO', 'SPXU', 'UVXY', 'SVXY',
  'AAPL', 'MSFT', 'NVDA', 'TSLA', 'AMZN', 'META', 'GOOGL',
]

export const useAppStore = create<AppState>()(
  persist(
    (set) => ({
      universe: DEFAULT_UNIVERSE,
      setUniverse: (tickers) => set({ universe: tickers }),

      activeSymbol: 'SPY',
      setActiveSymbol: (s) => set({ activeSymbol: s.toUpperCase() }),

      autoRefresh: false,
      toggleAutoRefresh: () => set((s) => ({ autoRefresh: !s.autoRefresh })),

      dataSource: 'yfinance',
      setDataSource: (ds) => set({ dataSource: ds }),
      ibkrHost: '127.0.0.1',
      ibkrPort: 7497,
      setIbkrHost: (h) => set({ ibkrHost: h }),
      setIbkrPort: (p) => set({ ibkrPort: p }),
    }),
    { name: 'vegaplex-app' }
  )
)
