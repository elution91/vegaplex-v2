import { create } from 'zustand'

export type ScanStatus = 'idle' | 'running' | 'complete' | 'error'

export interface ScanRow {
  symbol: string
  opportunities: Opportunity[]
  surface_data?: Record<string, unknown>
  error?: string
}

export interface Opportunity {
  type: string
  symbol: string
  confidence: number
  rr: number
  legs: Leg[]
  metrics?: Greeks
  greeks?: string
  _vega_bias?: string
  _greek_aligned?: string
  [key: string]: unknown
}

export interface Leg {
  action: string
  type: string
  strike: number
  expiry: string
  price: number
  contracts: number
}

export interface Greeks {
  total_delta: number
  total_gamma: number
  total_vega: number
  total_theta: number
}

interface ScanState {
  status: ScanStatus
  progress: { done: number; total: number; currentTicker: string }
  results: ScanRow[]
  selectedOpportunity: Opportunity | null
  errors: { ticker: string; message: string }[]

  setRunning: (total: number) => void
  addProgress: (ticker: string, done: number, total: number, result: ScanRow) => void
  addError: (ticker: string, message: string) => void
  setComplete: (results: ScanRow[], status: string) => void
  setIdle: () => void
  selectOpportunity: (opp: Opportunity | null) => void
}

export const useScanStore = create<ScanState>((set) => ({
  status: 'idle',
  progress: { done: 0, total: 0, currentTicker: '' },
  results: [],
  selectedOpportunity: null,
  errors: [],

  setRunning: (total) =>
    set({ status: 'running', progress: { done: 0, total, currentTicker: '' }, results: [], errors: [] }),

  addProgress: (ticker, done, total, result) =>
    set((s) => ({
      progress: { done, total, currentTicker: ticker },
      results: [...s.results, result],
    })),

  addError: (ticker, message) =>
    set((s) => ({ errors: [...s.errors, { ticker, message }] })),

  setComplete: (_results, _status) =>
    set({ status: 'complete', progress: (s => s.progress)(useScanStore.getState()) }),

  setIdle: () =>
    set({ status: 'idle', progress: { done: 0, total: 0, currentTicker: '' } }),

  selectOpportunity: (opp) => set({ selectedOpportunity: opp }),
}))
