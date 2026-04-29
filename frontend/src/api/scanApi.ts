import { apiClient } from './client'
import type { ThresholdSettings } from '../store/useSettingsStore'

type ScanThresholds = Pick<ThresholdSettings, 'rr_acceptable' | 'confidence_med'>

function toApiThresholds(t: ScanThresholds) {
  return {
    min_risk_reward: t.rr_acceptable,
    min_confidence:  t.confidence_med,
    confidence_high: 0.70,
    confidence_med:  t.confidence_med,
  }
}

export async function scanSymbol(symbol: string, thresholds?: ScanThresholds): Promise<unknown> {
  const { data } = await apiClient.post('/api/scan/symbol', {
    symbol,
    thresholds: thresholds ? toApiThresholds(thresholds) : undefined,
  })
  return data
}

export async function startUniverseScan(symbols: string[], thresholds?: ScanThresholds): Promise<{ job_id: string }> {
  const { data } = await apiClient.post('/api/scan/universe', {
    symbols,
    thresholds: thresholds ? toApiThresholds(thresholds) : undefined,
  })
  return data
}

export async function getSymbolDetail(symbol: string): Promise<unknown> {
  const { data } = await apiClient.get(`/api/scan/symbol/${symbol}/detail`)
  return data
}

export function scanStreamUrl(jobId: string): string {
  return `/api/scan/stream/${jobId}`
}
