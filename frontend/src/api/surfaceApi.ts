import { apiClient } from './client'

export async function getSurfaceCharts(symbol: string, optionType = 'call'): Promise<unknown> {
  const { data } = await apiClient.get(`/api/surface/${symbol}/charts`, {
    params: { option_type: optionType },
  })
  return data
}

export async function getSmile(symbol: string, expiry: string): Promise<unknown> {
  const { data } = await apiClient.get(`/api/surface/${symbol}/smile`, {
    params: { expiry },
  })
  return data
}
