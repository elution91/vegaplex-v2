import { useScanStore, Opportunity } from '../store/useScanStore'
import ProgressStream from '../components/shared/ProgressStream'
import ChartCard from '../components/charts/ChartCard'
import EmptyState from '../components/shared/EmptyState'
import MetricCard from '../components/shared/MetricCard'
import clsx from 'clsx'

function confidenceClass(conf: number) {
  if (conf >= 0.7) return 'conf-high'
  if (conf >= 0.45) return 'conf-medium'
  return 'conf-low'
}

function greekClass(bias: string | undefined, aligned: string | undefined) {
  if (aligned === 'False') return 'greek-warn'
  if (bias === 'short') return 'greek-short'
  if (bias === 'long') return 'greek-long'
  return ''
}

export default function ResultsView() {
  const { status, progress, results, errors, selectedOpportunity, selectOpportunity } = useScanStore()

  // Flatten all opportunities across all scan rows
  const allOpps: Opportunity[] = results.flatMap((r) => r.opportunities ?? [])

  const totalOpps = allOpps.length
  const avgConf = totalOpps
    ? (allOpps.reduce((s, o) => s + (o.confidence ?? 0), 0) / totalOpps).toFixed(2)
    : '—'
  const avgRR = totalOpps
    ? (allOpps.reduce((s, o) => s + (o.rr ?? 0), 0) / totalOpps).toFixed(2)
    : '—'

  return (
    <div className="space-y-4">
      {/* Progress */}
      <ProgressStream
        done={progress.done}
        total={progress.total}
        currentTicker={progress.currentTicker}
        errors={errors}
        visible={status === 'running'}
      />

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-2">
        <MetricCard label="Opportunities" value={totalOpps} />
        <MetricCard label="Avg Confidence" value={avgConf} />
        <MetricCard label="Avg R/R" value={avgRR} />
      </div>

      {totalOpps === 0 && status !== 'running' && (
        <EmptyState message="Scan a symbol or universe to see results" />
      )}

      {totalOpps > 0 && (
        <div className="flex gap-4">
          {/* Opportunities table */}
          <div className="flex-1 card overflow-hidden">
            <table className="vp-table">
              <thead>
                <tr>
                  <th>Symbol</th>
                  <th>Type</th>
                  <th>Confidence</th>
                  <th>R/R</th>
                  <th>Greeks</th>
                </tr>
              </thead>
              <tbody>
                {allOpps.map((opp, i) => (
                  <tr
                    key={i}
                    onClick={() => selectOpportunity(opp)}
                    className={clsx('cursor-pointer', selectedOpportunity === opp && 'selected')}
                  >
                    <td className="text-accent">{opp.symbol}</td>
                    <td>{opp.type}</td>
                    <td className={confidenceClass(opp.confidence ?? 0)}>
                      {opp.confidence != null ? (opp.confidence * 100).toFixed(0) + '%' : '—'}
                    </td>
                    <td>{opp.rr != null ? opp.rr.toFixed(2) : '—'}</td>
                    <td className={greekClass(opp._vega_bias, opp._greek_aligned)}>
                      {opp.greeks ?? '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Drill-down panel */}
          {selectedOpportunity && (
            <DrillDown opp={selectedOpportunity} />
          )}
        </div>
      )}
    </div>
  )
}

function DrillDown({ opp }: { opp: Opportunity }) {
  return (
    <div className="w-80 space-y-3 shrink-0">
      <div className="card p-3 text-xs space-y-1">
        <div className="font-semibold text-text-primary">{opp.symbol} — {opp.type}</div>
        {opp.legs?.map((leg, i) => (
          <div key={i} className="flex justify-between text-text-muted">
            <span>{leg.action} {leg.type} ${leg.strike} {leg.expiry}</span>
            <span>${leg.price?.toFixed(2)}</span>
          </div>
        ))}
      </div>

      {/* Payoff chart — option dict comes from the backend when available */}
      {(opp as Record<string, unknown>).payoff_chart && (
        <ChartCard
          title="Payoff"
          option={(opp as Record<string, unknown>).payoff_chart as Record<string, unknown>}
          height={220}
        />
      )}
    </div>
  )
}
