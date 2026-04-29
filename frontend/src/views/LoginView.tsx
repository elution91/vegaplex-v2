import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { apiClient } from '../api/client'

export default function LoginView() {
  const [password, setPassword] = useState('')
  const [error, setError]       = useState('')
  const [busy, setBusy]         = useState(false)
  const navigate = useNavigate()

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError('')
    setBusy(true)
    try {
      await apiClient.post('/api/auth/login', { password })
      // Persist for header-based fallback
      localStorage.setItem('vegaplex_auth', password)
      navigate('/radar')
    } catch (err) {
      setError((err as Error).message || 'Login failed')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div style={{
      minHeight: '100vh', background: '#0d1117',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
    }}>
      <form onSubmit={handleSubmit} style={{
        width: 320, padding: 28, background: '#161b22',
        border: '1px solid #21262d', borderRadius: 8,
        display: 'flex', flexDirection: 'column', gap: 14,
      }}>
        <div style={{
          fontSize: 18, fontWeight: 600, color: '#e6edf3',
          letterSpacing: '0.02em',
        }}>
          νegaPlex — Beta Access
        </div>
        <div style={{ fontSize: 12, color: '#8b949e', lineHeight: 1.5 }}>
          Enter the shared password provided to you. Sessions persist for 30 days.
        </div>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Password"
          autoFocus
          style={{
            padding: '8px 10px', fontSize: 13,
            background: '#0d1117', border: '1px solid #21262d',
            borderRadius: 4, color: '#e6edf3', outline: 'none',
          }}
        />
        {error && (
          <div style={{ fontSize: 12, color: '#f85149' }}>{error}</div>
        )}
        <button
          type="submit"
          disabled={busy || !password}
          style={{
            padding: '8px 12px', fontSize: 13, fontWeight: 600,
            background: busy || !password ? '#21262d' : '#1f6feb',
            color: '#e6edf3', border: 'none', borderRadius: 4,
            cursor: busy || !password ? 'default' : 'pointer',
          }}
        >
          {busy ? 'Signing in…' : 'Continue'}
        </button>
      </form>
    </div>
  )
}
