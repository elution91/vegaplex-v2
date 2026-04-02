import { apiClient } from './client'

export async function getRadar(symbols: string[], lookback = 252): Promise<unknown> {
  const { data } = await apiClient.post('/api/radar', { symbols, lookback })
  return data
}
