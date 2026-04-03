import { useEffect, useState } from 'react'
import { api, type Stats } from '../lib/api'

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
      <div className="rounded-lg border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-950 p-4">
        <p className="text-red-600 dark:text-red-400">Connection error: {error}</p>
        <p className="text-sm text-gray-500 mt-1">Is GVDB running?</p>
      </div>
    )
  }

  return (
    <div>
      <h1 className="text-2xl font-bold mb-6">Dashboard</h1>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard label="Status" value={health || '...'} color={health === 'SERVING' ? 'green' : 'gray'} />
        <StatCard label="Collections" value={stats?.total_collections ?? '...'} />
        <StatCard label="Total Vectors" value={stats?.total_vectors ?? '...'} />
        <StatCard label="Avg Query Time" value={stats ? `${stats.avg_query_time_ms.toFixed(1)}ms` : '...'} />
      </div>
    </div>
  )
}

function StatCard({ label, value, color }: { label: string; value: string | number; color?: string }) {
  const colorClass = color === 'green'
    ? 'text-green-600 dark:text-green-400'
    : 'text-gray-900 dark:text-gray-100'

  return (
    <div className="rounded-lg border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900 p-4">
      <p className="text-sm text-gray-500 dark:text-gray-400">{label}</p>
      <p className={`text-2xl font-bold mt-1 ${colorClass}`}>{value}</p>
    </div>
  )
}
