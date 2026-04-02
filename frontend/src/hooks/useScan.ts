import { useCallback } from 'react'
import { scanSymbol, startUniverseScan, scanStreamUrl } from '../api/scanApi'
import { openSSEStream } from '../api/sseClient'
import { useScanStore, ScanRow } from '../store/useScanStore'
import { useAppStore } from '../store/useAppStore'

export function useScan() {
  const { setRunning, addProgress, addError, setComplete, setIdle } = useScanStore()
  const universe = useAppStore((s) => s.universe)
  const dataSource = useAppStore((s) => s.dataSource)

  const scanSingle = useCallback(async (symbol: string) => {
    setRunning(1)
    try {
      const result = await scanSymbol(symbol)
      setComplete([result as ScanRow], `Scanned ${symbol}`)
    } catch (e) {
      addError(symbol, String(e))
      setComplete([], 'Scan failed')
    }
  }, [setRunning, setComplete, addError])

  const scanUniverse = useCallback(async (symbols?: string[]) => {
    const tickers = symbols ?? universe
    setRunning(tickers.length)
    try {
      const { job_id } = await startUniverseScan(tickers)
      const cleanup = openSSEStream(scanStreamUrl(job_id), {
        onProgress: (ticker, done, total, result) => {
          addProgress(ticker, done, total, result as ScanRow)
        },
        onError: (ticker, message) => addError(ticker, message),
        onComplete: (results, status) => {
          setComplete(results as ScanRow[], status)
          cleanup()
        },
      })
    } catch (e) {
      addError('', String(e))
      setIdle()
    }
  }, [universe, setRunning, addProgress, addError, setComplete, setIdle])

  return { scanSingle, scanUniverse }
}
