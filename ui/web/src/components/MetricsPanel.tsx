import { useEffect, useState } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts'

interface Metric {
  name: string
  labels: Record<string, string> | null
  value: number
}

export default function MetricsPanel() {
  const [metrics, setMetrics] = useState<Metric[]>([])
  const [error, setError] = useState('')

  useEffect(() => {
    fetch('/api/metrics')
      .then(r => r.ok ? r.json() : Promise.reject(new Error('Failed')))
      .then(setMetrics)
      .catch(e => setError(e.message))
  }, [])

  if (error) {
    return <p style={{ fontSize: 13, color: 'var(--text-tertiary)' }}>Metrics unavailable</p>
  }

  if (metrics.length === 0) {
    return <p style={{ fontSize: 13, color: 'var(--text-tertiary)' }}>No metrics yet. Perform some operations first.</p>
  }

  const requestCounts = metrics
    .filter(m => m.name.endsWith('_requests_total'))
    .reduce<Record<string, number>>((acc, m) => {
      const op = m.name.replace('gvdb_', '').replace('_requests_total', '')
      acc[op] = (acc[op] || 0) + m.value
      return acc
    }, {})

  const requestData = Object.entries(requestCounts).map(([op, count]) => ({
    operation: op,
    count,
  }))

  return (
    <div>
      <h2 style={{ fontSize: 16, fontWeight: 500, margin: '0 0 16px' }}>Metrics</h2>

      {requestData.length > 0 && (
        <div style={{ border: '1px solid var(--border)', borderRadius: 8, padding: 20, background: 'var(--bg-card)', marginBottom: 16 }}>
          <p style={{ fontSize: 12, color: 'var(--text-tertiary)', margin: '0 0 16px', textTransform: 'uppercase', letterSpacing: 0.5 }}>Requests by Operation</p>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={requestData}>
              <XAxis dataKey="operation" tick={{ fontSize: 11, fill: 'var(--text-tertiary)' }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontSize: 11, fill: 'var(--text-tertiary)' }} axisLine={false} tickLine={false} />
              <Tooltip
                contentStyle={{ background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 6, fontSize: 12 }}
                labelStyle={{ color: 'var(--text-primary)' }}
              />
              <Bar dataKey="count" fill="var(--accent)" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      <details style={{ border: '1px solid var(--border)', borderRadius: 8, background: 'var(--bg-card)' }}>
        <summary style={{ padding: '12px 20px', cursor: 'pointer', fontSize: 12, color: 'var(--text-tertiary)', textTransform: 'uppercase', letterSpacing: 0.5 }}>
          Raw Metrics ({metrics.length})
        </summary>
        <div style={{ maxHeight: 300, overflow: 'auto', padding: '0 20px 16px' }}>
          <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
            <thead>
              <tr style={{ color: 'var(--text-tertiary)', textAlign: 'left' }}>
                <th style={{ padding: '6px 0', fontWeight: 500 }}>Name</th>
                <th style={{ padding: '6px 0', fontWeight: 500 }}>Labels</th>
                <th style={{ padding: '6px 0', fontWeight: 500, textAlign: 'right' }}>Value</th>
              </tr>
            </thead>
            <tbody>
              {metrics.map((m, i) => (
                <tr key={i} style={{ borderTop: '1px solid var(--border)' }}>
                  <td style={{ padding: '6px 0', fontFamily: 'monospace', fontSize: 11 }}>{m.name}</td>
                  <td style={{ padding: '6px 0', color: 'var(--text-tertiary)', fontSize: 11 }}>
                    {Object.entries(m.labels || {}).map(([k, v]) => `${k}=${v}`).join(', ')}
                  </td>
                  <td style={{ padding: '6px 0', textAlign: 'right', fontFamily: 'monospace', fontSize: 11 }}>{m.value}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
    </div>
  )
}
