import { apiClient } from './client'

export interface EarningsScanParams {
  days_ahead?: number
  min_iv_rv_ratio?: number
  data_source?: 'yfinance' | 'ibkr'
  ibkr_host?: string
  ibkr_port?: number
}

export async function startEarningsScan(params: EarningsScanParams): Promise<{ job_id: string }> {
  const { data } = await apiClient.post('/api/earnings/scan', params)
  return data
}

export function earningsStreamUrl(jobId: string): string {
  return `/api/earnings/stream/${jobId}`
}
