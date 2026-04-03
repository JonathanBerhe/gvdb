import { useEffect, useState } from 'react'
import { api, type Collection } from '../lib/api'

export default function Collections({ onSelect }: { onSelect?: (name: string) => void }) {
  const [collections, setCollections] = useState<Collection[]>([])
  const [error, setError] = useState('')
  const [showCreate, setShowCreate] = useState(false)

  const load = () => {
    api.listCollections().then(setCollections).catch(e => setError(e.message))
  }
  useEffect(load, [])

  const handleDrop = async (name: string) => {
    if (!confirm(`Drop collection "${name}"?`)) return
    try {
      await api.dropCollection(name)
      load()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Error')
    }
  }

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 24 }}>
        <h1 style={{ fontSize: 24, fontWeight: 600, margin: 0, letterSpacing: -0.5 }}>Collections</h1>
        <button onClick={() => setShowCreate(!showCreate)} style={btnStyle}>
          {showCreate ? 'Cancel' : 'Create'}
        </button>
      </div>

      {error && <p style={{ color: 'var(--danger)', fontSize: 13, marginBottom: 16 }}>{error}</p>}
      {showCreate && <CreateForm onCreated={() => { setShowCreate(false); load() }} />}

      {collections.length === 0 ? (
        <p style={{ color: 'var(--text-tertiary)', fontSize: 14 }}>No collections yet.</p>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 14 }}>
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border)' }}>
              <th style={thStyle}>Name</th>
              <th style={thStyle}>Dimension</th>
              <th style={thStyle}>Vectors</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {collections.map(c => (
              <tr key={c.id} style={{ borderBottom: '1px solid var(--border)' }}>
                <td style={tdStyle}>
                  <button onClick={() => onSelect?.(c.name)} style={{ background: 'none', border: 'none', color: 'var(--accent)', cursor: 'pointer', padding: 0, fontSize: 14, fontWeight: 500 }}>
                    {c.name}
                  </button>
                </td>
                <td style={tdStyle}>{c.dimension}</td>
                <td style={tdStyle}>{c.vector_count.toLocaleString()}</td>
                <td style={{ ...tdStyle, textAlign: 'right' }}>
                  <button onClick={() => handleDrop(c.name)} style={{ background: 'none', border: 'none', color: 'var(--danger)', cursor: 'pointer', fontSize: 12, padding: 0 }}>
                    Drop
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
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
      setError(err instanceof Error ? err.message : 'Error')
    } finally {
      setLoading(false)
    }
  }

  return (
    <form onSubmit={handleSubmit} style={{ border: '1px solid var(--border)', borderRadius: 8, padding: 20, background: 'var(--bg-card)', marginBottom: 24 }}>
      {error && <p style={{ color: 'var(--danger)', fontSize: 13, margin: '0 0 12px' }}>{error}</p>}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 12 }}>
        <Field label="Name">
          <input value={name} onChange={e => setName(e.target.value)} required placeholder="my_collection" style={inputStyle} />
        </Field>
        <Field label="Dimension">
          <input type="number" value={dimension} onChange={e => setDimension(Number(e.target.value))} required min={1} style={inputStyle} />
        </Field>
        <Field label="Metric">
          <select value={metric} onChange={e => setMetric(e.target.value)} style={inputStyle}>
            <option value="l2">L2</option>
            <option value="cosine">Cosine</option>
            <option value="inner_product">Inner Product</option>
          </select>
        </Field>
        <Field label="Index">
          <select value={indexType} onChange={e => setIndexType(e.target.value)} style={inputStyle}>
            <option value="hnsw">HNSW</option>
            <option value="flat">FLAT</option>
            <option value="ivf_flat">IVF_FLAT</option>
            <option value="ivf_pq">IVF_PQ</option>
          </select>
        </Field>
      </div>
      <button type="submit" disabled={loading} style={{ ...btnStyle, marginTop: 16, opacity: loading ? 0.5 : 1 }}>
        {loading ? 'Creating...' : 'Create'}
      </button>
    </form>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label style={{ display: 'block', fontSize: 12, color: 'var(--text-tertiary)', marginBottom: 4, textTransform: 'uppercase', letterSpacing: 0.5 }}>{label}</label>
      {children}
    </div>
  )
}

const thStyle: React.CSSProperties = { padding: '10px 0', fontWeight: 500, fontSize: 12, color: 'var(--text-tertiary)', textAlign: 'left', textTransform: 'uppercase', letterSpacing: 0.5 }
const tdStyle: React.CSSProperties = { padding: '12px 0' }
const inputStyle: React.CSSProperties = { width: '100%', padding: '8px 12px', fontSize: 13, border: '1px solid var(--border)', borderRadius: 6, background: 'var(--bg)', color: 'var(--text-primary)', boxSizing: 'border-box' }
const btnStyle: React.CSSProperties = { padding: '8px 16px', fontSize: 13, fontWeight: 500, background: 'var(--text-primary)', color: 'var(--bg)', border: 'none', borderRadius: 6, cursor: 'pointer' }
