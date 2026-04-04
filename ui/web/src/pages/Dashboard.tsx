import { useEffect, useState } from 'react'
import { api, type Stats } from '../lib/api'
import MetricsPanel from '../components/MetricsPanel'

export default function Dashboard() {
  const [stats, setStats] = useState<Stats | null>(null)
  const [health, setHealth] = useState<string>('')
  const [error, setError] = useState<string>('')

  useEffect(() => {
    api.health()
      .then(h => setHealth(h.status))
      .catch(e => setError(e.message))
    api.stats()
      .then(s => setStats(s))
      .catch(e => setError(e.message))
  }, [])

  if (error) {
    return (
      <div style={{ border: '1px solid var(--danger)', borderRadius: 8, padding: 16, background: 'var(--bg-card)' }}>
        <p style={{ color: 'var(--danger)', margin: 0, fontSize: 14 }}>Connection error: {error}</p>
        <p style={{ color: 'var(--text-tertiary)', margin: '4px 0 0', fontSize: 13 }}>Is GVDB running?</p>
      </div>
    )
  }

  return (
    <div>
      <h1 style={{ fontSize: 24, fontWeight: 600, margin: '0 0 24px', letterSpacing: -0.5 }}>Dashboard</h1>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
        <StatCard label="Status" value={health || '—'} highlight={health === 'SERVING'} />
        <StatCard label="Collections" value={stats?.total_collections ?? '—'} />
        <StatCard label="Vectors" value={stats?.total_vectors ?? '—'} />
        <StatCard label="Avg Query" value={stats ? `${stats.avg_query_time_ms.toFixed(1)}ms` : '—'} />
      </div>

      <div style={{ marginTop: 32 }}>
        <MetricsPanel />
      </div>
    </div>
  )
}

function StatCard({ label, value, highlight }: { label: string; value: string | number; highlight?: boolean }) {
  return (
    <div style={{
      border: '1px solid var(--border)',
      borderRadius: 8,
      padding: '16px 20px',
      background: 'var(--bg-card)',
    }}>
      <p style={{ fontSize: 12, color: 'var(--text-tertiary)', margin: 0, textTransform: 'uppercase', letterSpacing: 0.5 }}>{label}</p>
      <p style={{
        fontSize: 28,
        fontWeight: 600,
        margin: '8px 0 0',
        letterSpacing: -0.5,
        color: highlight ? 'var(--success)' : 'var(--text-primary)',
      }}>{value}</p>
    </div>
  )
}
