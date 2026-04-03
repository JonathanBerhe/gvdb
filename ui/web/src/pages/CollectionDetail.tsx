import { useEffect, useState } from 'react'
import { api, type VectorEntry, type SearchResult } from '../lib/api'

interface Props {
  name: string
  onBack: () => void
}

export default function CollectionDetail({ name, onBack }: Props) {
  const [tab, setTab] = useState<'vectors' | 'search' | 'hybrid'>('vectors')

  return (
    <div>
      <div className="flex items-center gap-3 mb-6">
        <button onClick={onBack} className="text-sm text-gray-500 hover:text-gray-700 dark:hover:text-gray-300">&larr; Back</button>
        <h1 className="text-2xl font-bold">{name}</h1>
      </div>

      <div className="flex gap-1 mb-6 border-b border-gray-200 dark:border-gray-800">
        {(['vectors', 'search', 'hybrid'] as const).map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 text-sm capitalize ${tab === t
              ? 'border-b-2 border-blue-600 text-blue-600 dark:text-blue-400 font-medium'
              : 'text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'}`}
          >
            {t === 'hybrid' ? 'Hybrid Search' : t === 'search' ? 'Vector Search' : 'Vectors'}
          </button>
        ))}
      </div>

      {tab === 'vectors' && <VectorBrowser name={name} />}
      {tab === 'search' && <SearchPlayground name={name} mode="vector" />}
      {tab === 'hybrid' && <SearchPlayground name={name} mode="hybrid" />}
    </div>
  )
}

function VectorBrowser({ name }: { name: string }) {
  const [vectors, setVectors] = useState<VectorEntry[]>([])
  const [total, setTotal] = useState(0)
  const [offset, setOffset] = useState(0)
  const [hasMore, setHasMore] = useState(false)
  const [error, setError] = useState('')
  const limit = 20

  const load = (off: number) => {
    api.listVectors(name, limit, off, true)
      .then(resp => {
        setVectors(resp.vectors)
        setTotal(resp.total_count)
        setHasMore(resp.has_more)
        setOffset(off)
      })
      .catch(e => setError(e.message))
  }

  useEffect(() => { load(0) }, [name])

  if (error) return <p className="text-red-500">{error}</p>

  return (
    <div>
      <p className="text-sm text-gray-500 mb-4">{total} vectors total</p>
      {vectors.length === 0 ? (
        <p className="text-gray-500">No vectors in this collection.</p>
      ) : (
        <>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 dark:border-gray-800 text-left text-gray-500">
                  <th className="py-2 px-3 font-medium">ID</th>
                  <th className="py-2 px-3 font-medium">Dimension</th>
                  <th className="py-2 px-3 font-medium">Metadata</th>
                </tr>
              </thead>
              <tbody>
                {vectors.map(v => (
                  <tr key={v.id} className="border-b border-gray-100 dark:border-gray-800/50">
                    <td className="py-2 px-3 font-mono">{v.id}</td>
                    <td className="py-2 px-3">{v.dimension}</td>
                    <td className="py-2 px-3 text-xs text-gray-500 max-w-md truncate">
                      {v.metadata ? JSON.stringify(v.metadata) : '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="flex gap-2 mt-4">
            <button
              disabled={offset === 0}
              onClick={() => load(Math.max(0, offset - limit))}
              className="px-3 py-1 text-sm rounded border border-gray-300 dark:border-gray-700 disabled:opacity-30"
            >
              Previous
            </button>
            <span className="text-sm text-gray-500 py-1">
              {offset + 1}-{Math.min(offset + limit, total)} of {total}
            </span>
            <button
              disabled={!hasMore}
              onClick={() => load(offset + limit)}
              className="px-3 py-1 text-sm rounded border border-gray-300 dark:border-gray-700 disabled:opacity-30"
            >
              Next
            </button>
          </div>
        </>
      )}
    </div>
  )
}

function SearchPlayground({ name, mode }: { name: string; mode: 'vector' | 'hybrid' }) {
  const [queryVector, setQueryVector] = useState('')
  const [textQuery, setTextQuery] = useState('')
  const [topK, setTopK] = useState(10)
  const [vectorWeight, setVectorWeight] = useState(0.5)
  const [textWeight, setTextWeight] = useState(0.5)
  const [results, setResults] = useState<SearchResult[]>([])
  const [queryTime, setQueryTime] = useState<number | null>(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  const handleSearch = async () => {
    setError('')
    setLoading(true)
    try {
      let parsedVector: number[] = []
      if (queryVector.trim()) {
        parsedVector = JSON.parse(queryVector)
        if (!Array.isArray(parsedVector)) throw new Error('Query vector must be a JSON array')
      }

      let resp
      if (mode === 'hybrid') {
        resp = await api.hybridSearch(name, {
          query_vector: parsedVector.length > 0 ? parsedVector : undefined,
          text_query: textQuery || undefined,
          top_k: topK,
          vector_weight: vectorWeight,
          text_weight: textWeight,
          return_metadata: true,
        })
      } else {
        if (parsedVector.length === 0) throw new Error('Query vector is required for vector search')
        resp = await api.search(name, {
          query_vector: parsedVector,
          top_k: topK,
          return_metadata: true,
        })
      }

      setResults(resp.results)
      setQueryTime(resp.query_time_ms)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Search failed')
      setResults([])
    } finally {
      setLoading(false)
    }
  }

  return (
    <div>
      <div className="grid gap-4 mb-4">
        {(mode === 'vector' || mode === 'hybrid') && (
          <div>
            <label className="block text-xs text-gray-500 mb-1">Query Vector (JSON array)</label>
            <textarea
              value={queryVector}
              onChange={e => setQueryVector(e.target.value)}
              placeholder='[0.1, 0.2, 0.3, ...]'
              rows={2}
              className="w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-300 dark:border-gray-700 bg-transparent"
            />
          </div>
        )}

        {mode === 'hybrid' && (
          <>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Text Query</label>
              <input
                value={textQuery}
                onChange={e => setTextQuery(e.target.value)}
                placeholder="running shoes marathon"
                className="w-full px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-700 bg-transparent"
              />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-xs text-gray-500 mb-1">Vector Weight: {vectorWeight.toFixed(2)}</label>
                <input type="range" min="0" max="1" step="0.05" value={vectorWeight}
                  onChange={e => { setVectorWeight(Number(e.target.value)); setTextWeight(1 - Number(e.target.value)) }}
                  className="w-full" />
              </div>
              <div>
                <label className="block text-xs text-gray-500 mb-1">Text Weight: {textWeight.toFixed(2)}</label>
                <input type="range" min="0" max="1" step="0.05" value={textWeight}
                  onChange={e => { setTextWeight(Number(e.target.value)); setVectorWeight(1 - Number(e.target.value)) }}
                  className="w-full" />
              </div>
            </div>
          </>
        )}

        <div className="flex items-end gap-4">
          <div>
            <label className="block text-xs text-gray-500 mb-1">Top K</label>
            <input type="number" value={topK} onChange={e => setTopK(Number(e.target.value))}
              min={1} max={1000}
              className="w-24 px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-700 bg-transparent" />
          </div>
          <button
            onClick={handleSearch}
            disabled={loading}
            className="px-6 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            {loading ? 'Searching...' : 'Search'}
          </button>
        </div>
      </div>

      {error && <p className="text-red-500 text-sm mb-4">{error}</p>}

      {queryTime !== null && (
        <p className="text-sm text-gray-500 mb-4">{results.length} results in {queryTime.toFixed(2)}ms</p>
      )}

      {results.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-200 dark:border-gray-800 text-left text-gray-500">
                <th className="py-2 px-3 font-medium">#</th>
                <th className="py-2 px-3 font-medium">ID</th>
                <th className="py-2 px-3 font-medium">Score / Distance</th>
                <th className="py-2 px-3 font-medium">Metadata</th>
              </tr>
            </thead>
            <tbody>
              {results.map((r, i) => (
                <tr key={r.id} className="border-b border-gray-100 dark:border-gray-800/50">
                  <td className="py-2 px-3 text-gray-400">{i + 1}</td>
                  <td className="py-2 px-3 font-mono">{r.id}</td>
                  <td className="py-2 px-3 font-mono">{r.distance.toFixed(6)}</td>
                  <td className="py-2 px-3 text-xs text-gray-500 max-w-md truncate">
                    {r.metadata ? JSON.stringify(r.metadata) : '-'}
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
