import { useEffect, useState } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts'

interface Metric {
  name: string
  labels: Record<string, string>
  value: number
}

export default function MetricsPanel() {
  const [metrics, setMetrics] = useState<Metric[]>([])
  const [error, setError] = useState('')

  useEffect(() => {
    fetch('/api/metrics')
      .then(r => r.ok ? r.json() : Promise.reject(new Error('Failed to fetch metrics')))
      .then(setMetrics)
      .catch(e => setError(e.message))
  }, [])

  if (error) {
    return (
      <div className="rounded-lg border border-yellow-200 dark:border-yellow-800 bg-yellow-50 dark:bg-yellow-950 p-4">
        <p className="text-yellow-600 dark:text-yellow-400 text-sm">Metrics unavailable: {error}</p>
        <p className="text-xs text-gray-500 mt-1">Prometheus metrics endpoint may not be reachable.</p>
      </div>
    )
  }

  if (metrics.length === 0) {
    return (
      <div className="text-sm text-gray-500">No metrics available yet. Perform some operations first.</div>
    )
  }

  // Group metrics for display
  const requestCounts = metrics
    .filter(m => m.name.endsWith('_requests_total'))
    .reduce<Record<string, { success: number; error: number }>>((acc, m) => {
      const op = m.name.replace('gvdb_', '').replace('_requests_total', '')
      if (!acc[op]) acc[op] = { success: 0, error: 0 }
      if (m.labels.status === 'success') acc[op].success += m.value
      else if (m.labels.status === 'error') acc[op].error += m.value
      return acc
    }, {})

  const requestData = Object.entries(requestCounts).map(([op, counts]) => ({
    operation: op,
    success: counts.success,
    errors: counts.error,
  }))

  const vectorCounts = metrics
    .filter(m => m.name === 'gvdb_vector_count')
    .map(m => ({
      collection: m.labels.collection || 'unknown',
      vectors: m.value,
    }))

  return (
    <div className="space-y-6">
      <h2 className="text-lg font-semibold">Metrics</h2>

      {requestData.length > 0 && (
        <div className="rounded-lg border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 p-4">
          <h3 className="text-sm font-medium text-gray-500 mb-4">Requests by Operation</h3>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={requestData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="operation" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Bar dataKey="success" fill="#22c55e" name="Success" />
              <Bar dataKey="errors" fill="#ef4444" name="Errors" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {vectorCounts.length > 0 && (
        <div className="rounded-lg border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 p-4">
          <h3 className="text-sm font-medium text-gray-500 mb-4">Vectors per Collection</h3>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={vectorCounts}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="collection" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Bar dataKey="vectors" fill="#3b82f6" name="Vectors" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      <div className="rounded-lg border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 p-4">
        <h3 className="text-sm font-medium text-gray-500 mb-3">Raw Metrics</h3>
        <div className="overflow-x-auto max-h-64 overflow-y-auto">
          <table className="w-full text-xs font-mono">
            <thead>
              <tr className="text-left text-gray-500 border-b border-gray-200 dark:border-gray-800">
                <th className="py-1 px-2">Name</th>
                <th className="py-1 px-2">Labels</th>
                <th className="py-1 px-2 text-right">Value</th>
              </tr>
            </thead>
            <tbody>
              {metrics.map((m, i) => (
                <tr key={i} className="border-b border-gray-100 dark:border-gray-800/30">
                  <td className="py-1 px-2">{m.name}</td>
                  <td className="py-1 px-2 text-gray-500">
                    {Object.entries(m.labels).map(([k, v]) => `${k}=${v}`).join(', ')}
                  </td>
                  <td className="py-1 px-2 text-right">{m.value}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
