import { useEffect, useState } from 'react'
import { api, type Collection } from '../lib/api'

export default function Collections({ onSelect }: { onSelect?: (name: string) => void }) {
  const [collections, setCollections] = useState<Collection[]>([])
  const [error, setError] = useState('')
  const [showCreate, setShowCreate] = useState(false)

  const load = () => {
    api.listCollections()
      .then(setCollections)
      .catch(e => setError(e.message))
  }

  useEffect(load, [])

  const handleDrop = async (name: string) => {
    if (!confirm(`Drop collection "${name}"? This cannot be undone.`)) return
    try {
      await api.dropCollection(name)
      load()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Unknown error')
    }
  }

  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold">Collections</h1>
        <button
          onClick={() => setShowCreate(!showCreate)}
          className="px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700"
        >
          {showCreate ? 'Cancel' : 'Create Collection'}
        </button>
      </div>

      {error && (
        <div className="mb-4 p-3 rounded-lg bg-red-50 dark:bg-red-950 text-red-600 dark:text-red-400 text-sm">
          {error}
        </div>
      )}

      {showCreate && <CreateForm onCreated={() => { setShowCreate(false); load() }} />}

      {collections.length === 0 ? (
        <p className="text-gray-500">No collections yet.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 dark:border-gray-800 text-left text-gray-500">
                <th className="py-3 px-4 font-medium">Name</th>
                <th className="py-3 px-4 font-medium">Dimension</th>
                <th className="py-3 px-4 font-medium">Vectors</th>
                <th className="py-3 px-4 font-medium">Actions</th>
              </tr>
            </thead>
            <tbody>
              {collections.map(c => (
                <tr key={c.id} className="border-b border-gray-100 dark:border-gray-800/50">
                  <td className="py-3 px-4 font-medium">
                    <button onClick={() => onSelect?.(c.name)} className="text-blue-600 dark:text-blue-400 hover:underline">
                      {c.name}
                    </button>
                  </td>
                  <td className="py-3 px-4">{c.dimension}</td>
                  <td className="py-3 px-4">{c.vector_count.toLocaleString()}</td>
                  <td className="py-3 px-4">
                    <button
                      onClick={() => handleDrop(c.name)}
                      className="text-red-500 hover:text-red-700 text-xs"
                    >
                      Drop
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function CreateForm({ onCreated }: { onCreated: () => void }) {
  const [name, setName] = useState('')
  const [dimension, setDimension] = useState(128)
  const [metric, setMetric] = useState('l2')
  const [indexType, setIndexType] = useState('hnsw')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setLoading(true)
    setError('')
    try {
      await api.createCollection({ name, dimension, metric, index_type: indexType })
      onCreated()
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }

  return (
    <form onSubmit={handleSubmit} className="mb-6 p-4 rounded-lg border border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900">
      {error && <p className="text-red-500 text-sm mb-3">{error}</p>}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div>
          <label className="block text-xs text-gray-500 mb-1">Name</label>
          <input
            value={name}
            onChange={e => setName(e.target.value)}
            required
            className="w-full px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-700 bg-transparent"
            placeholder="my_collection"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Dimension</label>
          <input
            type="number"
            value={dimension}
            onChange={e => setDimension(Number(e.target.value))}
            required
            min={1}
            className="w-full px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-700 bg-transparent"
          />
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Metric</label>
          <select
            value={metric}
            onChange={e => setMetric(e.target.value)}
            className="w-full px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-700 bg-transparent"
          >
            <option value="l2">L2</option>
            <option value="cosine">Cosine</option>
            <option value="inner_product">Inner Product</option>
          </select>
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Index Type</label>
          <select
            value={indexType}
            onChange={e => setIndexType(e.target.value)}
            className="w-full px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-700 bg-transparent"
          >
            <option value="hnsw">HNSW</option>
            <option value="flat">FLAT</option>
            <option value="ivf_flat">IVF_FLAT</option>
            <option value="ivf_pq">IVF_PQ</option>
          </select>
        </div>
      </div>
      <button
        type="submit"
        disabled={loading}
        className="mt-4 px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 disabled:opacity-50"
      >
        {loading ? 'Creating...' : 'Create'}
      </button>
    </form>
  )
}
