import { create } from 'zustand'
import { persist } from 'zustand/middleware'

export interface ThresholdSettings {
  // Opportunity scanner
  confidence_high:  number   // default 0.70
  confidence_med:   number   // default 0.50
  rr_excellent:     number   // default 3.0
  rr_acceptable:    number   // default 1.5

  // Earnings scanner
  iv_rv_pass:       number   // default 1.25
  iv_rv_near_miss:  number   // default 1.0
  rich_threshold:   number   // default 1.10
  cheap_threshold:  number   // default 0.90

  // VIX / carry
  vix_ratio_carry_on:  number  // default 0.92 — below = carry on
  carry_ratio_min:     number  // default 0.85 — above = sufficient
  vrp_good:            number  // default 3.0  — above = strong edge
  vvix_vix_danger:     number  // default 5.0  — above = tail risk

  // Radar
  rv_high:    number  // default 40  — RV% above = red
  rv_medium:  number  // default 20  — RV% above = yellow
}

export const DEFAULT_SETTINGS: ThresholdSettings = {
  confidence_high:     0.70,
  confidence_med:      0.15,
  rr_excellent:        3.0,
  rr_acceptable:       1.5,
  iv_rv_pass:          1.25,
  iv_rv_near_miss:     1.0,
  rich_threshold:      1.10,
  cheap_threshold:     0.90,
  vix_ratio_carry_on:  0.92,
  carry_ratio_min:     0.85,
  vrp_good:            3.0,
  vvix_vix_danger:     5.0,
  rv_high:             40,
  rv_medium:           20,
}

interface SettingsState {
  thresholds: ThresholdSettings
  setThreshold: <K extends keyof ThresholdSettings>(key: K, value: ThresholdSettings[K]) => void
  resetThresholds: () => void
}

export const useSettingsStore = create<SettingsState>()(
  persist(
    (set) => ({
      thresholds: { ...DEFAULT_SETTINGS },
      setThreshold: (key, value) =>
        set((s) => ({ thresholds: { ...s.thresholds, [key]: value } })),
      resetThresholds: () => set({ thresholds: { ...DEFAULT_SETTINGS } }),
    }),
    {
      name: 'vp_settings',
      version: 2,
      migrate: (state: unknown) => ({ ...(state as SettingsState), thresholds: { ...DEFAULT_SETTINGS } }),
    }
  )
)
