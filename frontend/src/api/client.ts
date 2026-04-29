import axios from 'axios'

const BASE_URL = import.meta.env.VITE_API_URL ?? ''

export const apiClient = axios.create({
  baseURL: BASE_URL,
  timeout: 30_000,
  headers: { 'Content-Type': 'application/json' },
  withCredentials: true,   // send vegaplex_auth cookie cross-site
})

// Some hosts (Vercel / Render combo) don't reliably forward cookies on first
// load. Fall back to a localStorage-backed header for resilience.
apiClient.interceptors.request.use((config) => {
  const token = localStorage.getItem('vegaplex_auth')
  if (token) {
    config.headers.set?.('X-Vegaplex-Auth', token)
  }
  return config
})

apiClient.interceptors.response.use(
  (res) => res,
  (err) => {
    if (err.response?.status === 401) {
      // Auth required — drop any stale token and let the app redirect
      localStorage.removeItem('vegaplex_auth')
      if (window.location.pathname !== '/login') {
        window.location.href = '/login'
      }
    }
    const msg = err.response?.data?.detail ?? err.message
    return Promise.reject(new Error(msg))
  }
)
