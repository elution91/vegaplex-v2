export type SSEHandler = {
  onProgress?: (ticker: string, done: number, total: number, result: unknown) => void
  onError?: (ticker: string, message: string) => void
  onComplete?: (results: unknown[], status: string) => void
}

/**
 * Opens an EventSource stream and wires up the three event types
 * used by /api/scan/stream/{jobId} and /api/earnings/stream/{jobId}.
 * Returns a cleanup function — call it to close the stream.
 */
export function openSSEStream(url: string, handlers: SSEHandler): () => void {
  const es = new EventSource(url)

  es.addEventListener('progress', (e) => {
    const d = JSON.parse((e as MessageEvent).data)
    handlers.onProgress?.(d.ticker, d.done, d.total, d.result)
  })

  es.addEventListener('error', (e) => {
    const raw = (e as MessageEvent).data
    if (raw) {
      const d = JSON.parse(raw)
      handlers.onError?.(d.ticker, d.message)
    }
  })

  es.addEventListener('complete', (e) => {
    const d = JSON.parse((e as MessageEvent).data)
    handlers.onComplete?.(d.results, d.status)
    es.close()
  })

  return () => es.close()
}
