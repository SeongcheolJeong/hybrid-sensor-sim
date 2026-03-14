import type { Config } from 'tailwindcss'

export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        cp: {
          bg: '#081018',
          panel: '#101a24',
          panelAlt: '#152331',
          line: '#243445',
          text: '#ebf1f7',
          mut: '#93a5b5',
          cyan: '#55d4ff',
          green: '#46d483',
          amber: '#ffb44c',
          red: '#ff6b6b',
        },
      },
      fontFamily: {
        sans: ['"IBM Plex Sans"', 'ui-sans-serif', 'system-ui'],
        mono: ['"IBM Plex Mono"', 'ui-monospace', 'monospace'],
      },
      boxShadow: {
        panel: '0 16px 40px rgba(0, 0, 0, 0.28)',
      },
    },
  },
  plugins: [],
} satisfies Config
