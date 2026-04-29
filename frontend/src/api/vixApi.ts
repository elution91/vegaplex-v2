import { apiClient } from './client'

export async function getVix(): Promise<unknown> {
  const { data } = await apiClient.get('/api/vix', { timeout: 120_000 })
  return data
}

export async function getVixSnapshot(date: string): Promise<unknown> {
  const { data } = await apiClient.get('/api/vix/snapshot', { params: { date }, timeout: 60_000 })
  return data
}
