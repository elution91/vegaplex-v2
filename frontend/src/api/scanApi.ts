import { apiClient } from './client'

export async function scanSymbol(symbol: string): Promise<unknown> {
  const { data } = await apiClient.post('/api/scan/symbol', { symbol })
  return data
}

export async function startUniverseScan(symbols: string[]): Promise<{ job_id: string }> {
  const { data } = await apiClient.post('/api/scan/universe', { symbols })
  return data
}

export async function getSymbolDetail(symbol: string): Promise<unknown> {
  const { data } = await apiClient.get(`/api/scan/symbol/${symbol}/detail`)
  return data
}

export function scanStreamUrl(jobId: string): string {
  return `/api/scan/stream/${jobId}`
}
