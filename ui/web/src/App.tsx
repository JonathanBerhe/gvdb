import { useState } from 'react'
import Dashboard from './pages/Dashboard'
import Collections from './pages/Collections'
import CollectionDetail from './pages/CollectionDetail'

type Page =
  | { kind: 'dashboard' }
  | { kind: 'collections' }
  | { kind: 'collection-detail'; name: string }

export default function App() {
  const [page, setPage] = useState<Page>({ kind: 'dashboard' })

  const nav = (kind: 'dashboard' | 'collections') => setPage({ kind })

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-950 text-gray-900 dark:text-gray-100">
      <nav className="border-b border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900">
        <div className="max-w-7xl mx-auto px-4 flex items-center h-14 gap-6">
          <span className="font-bold text-lg cursor-pointer" onClick={() => nav('dashboard')}>GVDB</span>
          <button
            onClick={() => nav('dashboard')}
            className={`text-sm ${page.kind === 'dashboard' ? 'text-blue-600 dark:text-blue-400 font-medium' : 'text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'}`}
          >
            Dashboard
          </button>
          <button
            onClick={() => nav('collections')}
            className={`text-sm ${page.kind === 'collections' || page.kind === 'collection-detail' ? 'text-blue-600 dark:text-blue-400 font-medium' : 'text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'}`}
          >
            Collections
          </button>
        </div>
      </nav>
      <main className="max-w-7xl mx-auto px-4 py-6">
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
