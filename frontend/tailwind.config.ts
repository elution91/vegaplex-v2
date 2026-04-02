import type { Config } from 'tailwindcss'

export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        // Dark theme — mirrors custom.css CSS variables from v1
        'bg-base':    '#0d1117',
        'bg-card':    '#161b22',
        'bg-elevated':'#1c2128',
        'border':     '#30363d',
        'text-primary':  '#e6edf3',
        'text-muted':    '#8b949e',
        'text-faint':    '#484f58',
        'accent':        '#58a6ff',
        'accent-dim':    '#1f6feb',
        'positive':      '#3fb950',
        'negative':      '#f85149',
        'warning':       '#d29922',
        'teal':          '#39d353',
        // Greek bias colours
        'greek-short':   '#4dd0e1',
        'greek-long':    '#5c9eff',
        'greek-warn':    '#ffb74d',
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'Fira Code', 'ui-monospace', 'monospace'],
      },
    },
  },
  plugins: [],
} satisfies Config
