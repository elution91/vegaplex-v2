import { useSettingsStore, DEFAULT_SETTINGS, ThresholdSettings } from '../store/useSettingsStore'

type FieldDef = {
  key:   keyof ThresholdSettings
  label: string
  tip:   string
  step:  number
  min:   number
  max:   number
  unit?: string
}

const SECTIONS: { title: string; fields: FieldDef[] }[] = [
  {
    title: 'Opportunity Scanner',
    fields: [
      { key: 'confidence_high', label: 'High Confidence', tip: 'Opportunities at or above this score are highlighted green.', step: 0.05, min: 0.5, max: 1.0, unit: '%ile' },
      { key: 'confidence_med',  label: 'Medium Confidence', tip: 'Opportunities at or above this score are highlighted yellow.', step: 0.05, min: 0.1, max: 0.9, unit: '%ile' },
      { key: 'rr_excellent',    label: 'R/R Excellent', tip: 'Risk/reward at or above this level is highlighted green.', step: 0.25, min: 1.0, max: 10.0 },
      { key: 'rr_acceptable',   label: 'R/R Acceptable', tip: 'Risk/reward at or above this level is highlighted yellow.', step: 0.25, min: 0.5, max: 5.0 },
    ],
  },
  {
    title: 'Earnings Scanner',
    fields: [
      { key: 'iv_rv_pass',      label: 'IV/RV Pass Gate',      tip: 'IV/RV ratio must be at or above this to pass the earnings quality gate (green).', step: 0.05, min: 0.8, max: 2.0 },
      { key: 'iv_rv_near_miss', label: 'IV/RV Near-Miss',      tip: 'IV/RV ratio at or above this is a near-miss (yellow).', step: 0.05, min: 0.5, max: 1.5 },
      { key: 'rich_threshold',  label: 'Straddle Rich Cutoff', tip: 'Straddle price ÷ expected move above this = rich (green). Sell premium signal.', step: 0.05, min: 0.9, max: 2.0 },
      { key: 'cheap_threshold', label: 'Straddle Cheap Cutoff', tip: 'Straddle price ÷ expected move below this = cheap (red). Buy premium signal.', step: 0.05, min: 0.5, max: 1.0 },
    ],
  },
  {
    title: 'VIX / Carry Signal',
    fields: [
      { key: 'vix_ratio_carry_on', label: 'VIX Ratio — Carry On Below', tip: 'VIX/VIX3M below this threshold = deep enough contango to activate carry signal.', step: 0.01, min: 0.80, max: 1.0 },
      { key: 'carry_ratio_min',    label: 'Carry Ratio — Min Sufficient', tip: 'Carry ratio must be at or above this for the carry filter to pass.', step: 0.01, min: 0.70, max: 1.0 },
      { key: 'vrp_good',           label: 'VRP — Strong Edge Above', tip: 'Vol Risk Premium above this is highlighted as a strong edge for short-vol.', step: 0.5, min: 0.0, max: 20.0, unit: 'pts' },
      { key: 'vvix_vix_danger',    label: 'VVIX/VIX — Danger Above', tip: 'VVIX/VIX ratio above this signals elevated tail risk pressure.', step: 0.25, min: 2.0, max: 10.0 },
    ],
  },
  {
    title: 'Radar — Realised Vol',
    fields: [
      { key: 'rv_high',   label: 'RV High (Red) Above',   tip: 'Realised vol % above this is shown in red on the radar chart.', step: 5, min: 10, max: 100, unit: '%' },
      { key: 'rv_medium', label: 'RV Medium (Yellow) Above', tip: 'Realised vol % above this (but below High) is shown in yellow.', step: 5, min: 5, max: 80, unit: '%' },
    ],
  },
]

export default function SettingsView() {
  const { thresholds, setThreshold, resetThresholds } = useSettingsStore()

  return (
    <div className="space-y-4 max-w-3xl">
      <div className="flex items-center justify-between">
        <h2>Settings</h2>
        <button
          onClick={resetThresholds}
          className="nav-tab-btn"
          style={{ fontSize: 12 }}
        >
          Reset to Defaults
        </button>
      </div>

      <p style={{ fontSize: 12, color: '#6e7681', margin: 0 }}>
        Thresholds used for conditional formatting and signal filtering across all views.
        Changes take effect immediately and are saved to your browser.
      </p>

      {SECTIONS.map((section) => (
        <div key={section.title} className="card p-4 space-y-3">
          <div className="section-title" style={{ borderBottom: '1px solid #21262d', paddingBottom: 8 }}>
            {section.title}
          </div>
          <table style={{ width: '100%', borderCollapse: 'collapse' }}>
            <colgroup>
              <col style={{ width: '32%' }} />
              <col style={{ width: '18%' }} />
              <col style={{ width: '18%' }} />
              <col style={{ width: '32%' }} />
            </colgroup>
            <thead>
              <tr>
                <th style={{ fontSize: 10, color: '#484f58', textAlign: 'left', padding: '0 0 6px', fontWeight: 600, letterSpacing: '0.07em', textTransform: 'uppercase' }}>Threshold</th>
                <th style={{ fontSize: 10, color: '#484f58', textAlign: 'center', padding: '0 8px 6px', fontWeight: 600, letterSpacing: '0.07em', textTransform: 'uppercase' }}>Current</th>
                <th style={{ fontSize: 10, color: '#484f58', textAlign: 'center', padding: '0 8px 6px', fontWeight: 600, letterSpacing: '0.07em', textTransform: 'uppercase' }}>Default</th>
                <th style={{ fontSize: 10, color: '#484f58', textAlign: 'left', padding: '0 0 6px 8px', fontWeight: 600, letterSpacing: '0.07em', textTransform: 'uppercase' }}>Description</th>
              </tr>
            </thead>
            <tbody>
              {section.fields.map((f) => {
                const val     = thresholds[f.key] as number
                const def     = DEFAULT_SETTINGS[f.key] as number
                const changed = Math.abs(val - def) > 0.0001
                return (
                  <tr key={f.key}>
                    <td style={{ padding: '6px 0', fontSize: 12, color: '#8b949e' }}>
                      {f.label}
                      {changed && (
                        <span style={{ marginLeft: 6, fontSize: 10, color: '#e3b341', background: 'rgba(227,179,65,0.13)', padding: '1px 5px', borderRadius: 3 }}>
                          modified
                        </span>
                      )}
                    </td>
                    <td style={{ padding: '6px 8px', textAlign: 'center' }}>
                      <input
                        type="number"
                        value={val}
                        step={f.step}
                        min={f.min}
                        max={f.max}
                        onChange={(e) => {
                          const n = parseFloat(e.target.value)
                          if (!isNaN(n) && n >= f.min && n <= f.max) {
                            setThreshold(f.key, n as ThresholdSettings[typeof f.key])
                          }
                        }}
                        style={{
                          width: 80,
                          textAlign: 'center',
                          fontFamily: 'JetBrains Mono, monospace',
                          fontSize: 13,
                          fontWeight: 600,
                          color: changed ? '#e3b341' : '#e6edf3',
                          background: '#0d1117',
                          border: `1px solid ${changed ? 'rgba(227,179,65,0.4)' : '#21262d'}`,
                          borderRadius: 5,
                          padding: '3px 6px',
                          outline: 'none',
                        }}
                      />
                      {f.unit && <span style={{ marginLeft: 4, fontSize: 11, color: '#484f58' }}>{f.unit}</span>}
                    </td>
                    <td style={{ padding: '6px 8px', textAlign: 'center', fontFamily: 'JetBrains Mono, monospace', fontSize: 12, color: '#484f58' }}>
                      {def}
                    </td>
                    <td style={{ padding: '6px 0 6px 8px', fontSize: 11, color: '#484f58', lineHeight: 1.5 }}>
                      {f.tip}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  )
}
