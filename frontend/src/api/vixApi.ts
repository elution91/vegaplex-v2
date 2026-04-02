import { apiClient } from './client'

export async function getVix(): Promise<unknown> {
  const { data } = await apiClient.get('/api/vix')
  return data
}
