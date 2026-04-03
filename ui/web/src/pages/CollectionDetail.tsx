import { useEffect, useState } from 'react'
import { api, type VectorEntry, type SearchResult } from '../lib/api'

interface Props { name: string; onBack: () => void }

export default function CollectionDetail({ name, onBack }: Props) {
  const [tab, setTab] = useState<'vectors' | 'search' | 'hybrid'>('vectors')

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 24 }}>
        <button onClick={onBack} style={{ background: 'none', border: 'none', color: 'var(--text-tertiary)', cursor: 'pointer', fontSize: 13 }}>← Back</button>
        <h1 style={{ fontSize: 24, fontWeight: 600, margin: 0, letterSpacing: -0.5 }}>{name}</h1>
      </div>

      <div style={{ display: 'flex', gap: 0, borderBottom: '1px solid var(--border)', marginBottom: 24 }}>
        {(['vectors', 'search', 'hybrid'] as const).map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            style={{
              background: 'none', border: 'none', cursor: 'pointer',
              padding: '8px 16px', fontSize: 13,
              color: tab === t ? 'var(--text-primary)' : 'var(--text-tertiary)',
              fontWeight: tab === t ? 500 : 400,
              borderBottom: tab === t ? '2px solid var(--text-primary)' : '2px solid transparent',
              marginBottom: -1,
            }}
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
      .then(resp => { setVectors(resp.vectors); setTotal(resp.total_count); setHasMore(resp.has_more); setOffset(off) })
      .catch(e => setError(e.message))
  }
  useEffect(() => { load(0) }, [name])

  if (error) return <p style={{ color: 'var(--danger)', fontSize: 13 }}>{error}</p>

  return (
    <div>
      <p style={{ fontSize: 13, color: 'var(--text-tertiary)', marginBottom: 16 }}>{total} vectors</p>
      {vectors.length === 0 ? (
        <p style={{ color: 'var(--text-tertiary)', fontSize: 14 }}>Empty collection.</p>
      ) : (
        <>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: '1px solid var(--border)' }}>
                <th style={thStyle}>ID</th>
                <th style={thStyle}>Dim</th>
                <th style={thStyle}>Metadata</th>
              </tr>
            </thead>
            <tbody>
              {vectors.map(v => (
                <tr key={v.id} style={{ borderBottom: '1px solid var(--border)' }}>
                  <td style={{ ...tdStyle, fontFamily: 'monospace' }}>{v.id}</td>
                  <td style={tdStyle}>{v.dimension}</td>
                  <td style={{ ...tdStyle, color: 'var(--text-tertiary)', fontSize: 12, fontFamily: 'monospace', maxWidth: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {v.metadata ? JSON.stringify(v.metadata) : '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 12 }}>
            <button disabled={offset === 0} onClick={() => load(Math.max(0, offset - limit))} style={pageBtnStyle}>Prev</button>
            <span style={{ fontSize: 12, color: 'var(--text-tertiary)' }}>{offset + 1}–{Math.min(offset + limit, total)} of {total}</span>
            <button disabled={!hasMore} onClick={() => load(offset + limit)} style={pageBtnStyle}>Next</button>
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
  const [results, setResults] = useState<SearchResult[]>([])
  const [queryTime, setQueryTime] = useState<number | null>(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  const handleSearch = async () => {
    setError(''); setLoading(true)
    try {
      let parsedVector: number[] = []
      if (queryVector.trim()) {
        parsedVector = JSON.parse(queryVector)
        if (!Array.isArray(parsedVector)) throw new Error('Must be a JSON array')
      }
      let resp
      if (mode === 'hybrid') {
        resp = await api.hybridSearch(name, {
          query_vector: parsedVector.length > 0 ? parsedVector : undefined,
          text_query: textQuery || undefined,
          top_k: topK,
          vector_weight: vectorWeight,
          text_weight: 1 - vectorWeight,
          return_metadata: true,
        })
      } else {
        if (parsedVector.length === 0) throw new Error('Query vector required')
        resp = await api.search(name, { query_vector: parsedVector, top_k: topK, return_metadata: true })
      }
      setResults(resp.results); setQueryTime(resp.query_time_ms)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed'); setResults([])
    } finally { setLoading(false) }
  }

  return (
    <div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12, marginBottom: 16 }}>
        <div>
          <label style={labelStyle}>Query Vector</label>
          <textarea value={queryVector} onChange={e => setQueryVector(e.target.value)} placeholder="[0.1, 0.2, 0.3, ...]" rows={2} style={{ ...inputStyle, fontFamily: 'monospace', resize: 'vertical' }} />
        </div>

        {mode === 'hybrid' && (
          <>
            <div>
              <label style={labelStyle}>Text Query</label>
              <input value={textQuery} onChange={e => setTextQuery(e.target.value)} placeholder="running shoes" style={inputStyle} />
            </div>
            <div>
              <label style={labelStyle}>Vector Weight: {vectorWeight.toFixed(2)} / Text Weight: {(1 - vectorWeight).toFixed(2)}</label>
              <input type="range" min="0" max="1" step="0.05" value={vectorWeight} onChange={e => setVectorWeight(Number(e.target.value))} style={{ width: '100%', accentColor: 'var(--accent)' }} />
            </div>
          </>
        )}

        <div style={{ display: 'flex', alignItems: 'flex-end', gap: 12 }}>
          <div>
            <label style={labelStyle}>Top K</label>
            <input type="number" value={topK} onChange={e => setTopK(Number(e.target.value))} min={1} style={{ ...inputStyle, width: 80 }} />
          </div>
          <button onClick={handleSearch} disabled={loading} style={{ ...btnPrimaryStyle, opacity: loading ? 0.5 : 1 }}>
            {loading ? 'Searching...' : 'Search'}
          </button>
        </div>
      </div>

      {error && <p style={{ color: 'var(--danger)', fontSize: 13, marginBottom: 12 }}>{error}</p>}

      {queryTime !== null && (
        <p style={{ fontSize: 13, color: 'var(--text-tertiary)', marginBottom: 12 }}>{results.length} results in {queryTime.toFixed(2)}ms</p>
      )}

      {results.length > 0 && (
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border)' }}>
              <th style={thStyle}>#</th>
              <th style={thStyle}>ID</th>
              <th style={thStyle}>Score</th>
              <th style={thStyle}>Metadata</th>
            </tr>
          </thead>
          <tbody>
            {results.map((r, i) => (
              <tr key={r.id} style={{ borderBottom: '1px solid var(--border)' }}>
                <td style={{ ...tdStyle, color: 'var(--text-tertiary)' }}>{i + 1}</td>
                <td style={{ ...tdStyle, fontFamily: 'monospace' }}>{r.id}</td>
                <td style={{ ...tdStyle, fontFamily: 'monospace' }}>{r.distance.toFixed(6)}</td>
                <td style={{ ...tdStyle, color: 'var(--text-tertiary)', fontSize: 12, fontFamily: 'monospace', maxWidth: 400, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {r.metadata ? JSON.stringify(r.metadata) : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

const thStyle: React.CSSProperties = { padding: '8px 0', fontWeight: 500, fontSize: 12, color: 'var(--text-tertiary)', textAlign: 'left', textTransform: 'uppercase', letterSpacing: 0.5 }
const tdStyle: React.CSSProperties = { padding: '10px 0' }
const labelStyle: React.CSSProperties = { display: 'block', fontSize: 12, color: 'var(--text-tertiary)', marginBottom: 4, textTransform: 'uppercase', letterSpacing: 0.5 }
const inputStyle: React.CSSProperties = { width: '100%', padding: '8px 12px', fontSize: 13, border: '1px solid var(--border)', borderRadius: 6, background: 'var(--bg)', color: 'var(--text-primary)', boxSizing: 'border-box' as const }
const pageBtnStyle: React.CSSProperties = { padding: '4px 12px', fontSize: 12, border: '1px solid var(--border)', borderRadius: 6, background: 'var(--bg)', color: 'var(--text-secondary)', cursor: 'pointer' }
const btnPrimaryStyle: React.CSSProperties = { padding: '8px 20px', fontSize: 13, fontWeight: 500, background: 'var(--text-primary)', color: 'var(--bg)', border: 'none', borderRadius: 6, cursor: 'pointer' }
