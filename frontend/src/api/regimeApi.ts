import { apiClient } from './client'

export async function classifyRegime(symbol: string, lookback = 252): Promise<unknown> {
  const { data } = await apiClient.post('/api/regime/classify', { symbol, lookback })
  return data
}

export async function getRegimeCharts(symbol: string): Promise<unknown> {
  const { data } = await apiClient.get(`/api/regime/${symbol}/charts`)
  return data
}

export async function testBroker(host: string, port: number): Promise<{ connected: boolean; message: string }> {
  const { data } = await apiClient.post('/api/broker/test', {
    broker: 'ibkr',
    host,
    port,
    client_id: 2,
  })
  return data
}
