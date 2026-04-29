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
        sans: ['Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'Fira Code', 'ui-monospace', 'monospace'],
      },
      fontSize: {
        // Vegaplex type scale
        '2xs': ['11px', { lineHeight: '1.4' }],  // captions, table headers, metadata
        'xs':  ['12px', { lineHeight: '1.5' }],  // utility labels, badge text
        'sm':  ['13px', { lineHeight: '1.5' }],  // nav tabs, secondary text, table rows
        'base':['14px', { lineHeight: '1.6' }],  // body text, standard labels
        'lg':  ['16px', { lineHeight: '1.4' }],  // section headers (H2/H3)
        'xl':  ['20px', { lineHeight: '1.3' }],  // metric values, emphasis numbers
        '2xl': ['24px', { lineHeight: '1.2' }],  // page titles (H1)
        '3xl': ['32px', { lineHeight: '1.1' }],  // hero / entity name headers
      },
    },
  },
  plugins: [],
} satisfies Config
