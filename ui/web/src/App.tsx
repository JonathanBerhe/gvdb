import { useState, useEffect } from 'react'
import Dashboard from './pages/Dashboard'
import Collections from './pages/Collections'
import CollectionDetail from './pages/CollectionDetail'

type Page =
  | { kind: 'dashboard' }
  | { kind: 'collections' }
  | { kind: 'collection-detail'; name: string }

function useTheme() {
  const [dark, setDark] = useState(() => {
    const stored = localStorage.getItem('theme')
    if (stored) return stored === 'dark'
    return window.matchMedia('(prefers-color-scheme: dark)').matches
  })

  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
    localStorage.setItem('theme', dark ? 'dark' : 'light')
  }, [dark])

  return { dark, toggle: () => setDark(d => !d) }
}

export default function App() {
  const [page, setPage] = useState<Page>({ kind: 'dashboard' })
  const theme = useTheme()
  const nav = (kind: 'dashboard' | 'collections') => setPage({ kind })

  return (
    <div style={{ background: 'var(--bg)', color: 'var(--text-primary)', minHeight: '100vh' }}>
      <nav style={{ borderBottom: '1px solid var(--border)', background: 'var(--bg)' }}>
        <div style={{ maxWidth: 1200, margin: '0 auto', padding: '0 24px', display: 'flex', alignItems: 'center', height: 48, gap: 24 }}>
          <span style={{ fontWeight: 700, fontSize: 15, letterSpacing: -0.3 }} onClick={() => nav('dashboard')} role="button">GVDB</span>
          <div style={{ display: 'flex', gap: 16, flex: 1 }}>
            <NavLink active={page.kind === 'dashboard'} onClick={() => nav('dashboard')}>Dashboard</NavLink>
            <NavLink active={page.kind === 'collections' || page.kind === 'collection-detail'} onClick={() => nav('collections')}>Collections</NavLink>
          </div>
          <button
            onClick={theme.toggle}
            style={{ background: 'none', border: '1px solid var(--border)', borderRadius: 6, padding: '4px 8px', cursor: 'pointer', color: 'var(--text-secondary)', fontSize: 13 }}
          >
            {theme.dark ? '☀ Light' : '● Dark'}
          </button>
        </div>
      </nav>
      <main style={{ maxWidth: 1200, margin: '0 auto', padding: '32px 24px' }}>
        {page.kind === 'dashboard' && <Dashboard />}
        {page.kind === 'collections' && (
          <Collections onSelect={(name) => setPage({ kind: 'collection-detail', name })} />
        )}
        {page.kind === 'collection-detail' && (
          <CollectionDetail name={page.name} onBack={() => nav('collections')} />
        )}
      </main>
    </div>
  )
}

function NavLink({ active, onClick, children }: { active: boolean; onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      style={{
        background: 'none',
        border: 'none',
        cursor: 'pointer',
        fontSize: 13,
        color: active ? 'var(--text-primary)' : 'var(--text-secondary)',
        fontWeight: active ? 500 : 400,
        padding: 0,
      }}
    >
      {children}
    </button>
  )
}
