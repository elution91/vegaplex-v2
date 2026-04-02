import { useCallback, useState } from 'react'
import { startEarningsScan, earningsStreamUrl, EarningsScanParams } from '../api/earningsApi'
import { openSSEStream } from '../api/sseClient'

export interface EarningsRow {
  ticker: string
  date: string
  [key: string]: unknown
}

export function useEarnings() {
  const [rows, setRows] = useState<EarningsRow[]>([])
  const [status, setStatus] = useState<'idle' | 'running' | 'complete' | 'error'>('idle')
  const [progress, setProgress] = useState({ done: 0, total: 0, currentTicker: '' })
  const [errors, setErrors] = useState<{ ticker: string; message: string }[]>([])

  const scan = useCallback(async (params: EarningsScanParams) => {
    setRows([])
    setErrors([])
    setStatus('running')
    setProgress({ done: 0, total: 0, currentTicker: '' })

    try {
      const { job_id } = await startEarningsScan(params)
      const cleanup = openSSEStream(earningsStreamUrl(job_id), {
        onProgress: (ticker, done, total, result) => {
          setProgress({ done, total, currentTicker: ticker })
          if (result) setRows((prev) => [...prev, result as EarningsRow])
        },
        onError: (ticker, message) => {
          setErrors((prev) => [...prev, { ticker, message }])
        },
        onComplete: (results, _status) => {
          setRows(results as EarningsRow[])
          setStatus('complete')
          cleanup()
        },
      })
    } catch (e) {
      setStatus('error')
      setErrors([{ ticker: '', message: String(e) }])
    }
  }, [])

  return { rows, status, progress, errors, scan }
}
