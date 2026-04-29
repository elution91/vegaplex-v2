import { apiClient } from './client'

export type MacroEventRow = {
  event: string
  date: string
  days: number
  impact: number
  spot?: number
  near_exp?: string
  far_exp?: string
  near_iv?: number
  far_iv?: number
  background_vol?: number
  event_vol?: number
  implied_move_pct?: number | null
  hist_avg_move?: number | null
  hist_p75_move?: number | null
  vix_pct?: number
  vix_regime?: string
  richness?: number | null
  signal: string
  signal_color: string
  error?: string | null
}

export async function getMacroEvents(daysAhead = 60): Promise<MacroEventRow[]> {
  const { data } = await apiClient.get('/api/macro-events', {
    params: { days_ahead: daysAhead },
  })
  return data
}
