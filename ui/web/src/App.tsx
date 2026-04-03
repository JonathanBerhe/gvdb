import { useState } from 'react'
import Dashboard from './pages/Dashboard'
import Collections from './pages/Collections'

type Page = 'dashboard' | 'collections'

export default function App() {
  const [page, setPage] = useState<Page>('dashboard')

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-950 text-gray-900 dark:text-gray-100">
      <nav className="border-b border-gray-200 dark:border-gray-800 bg-white dark:bg-gray-900">
        <div className="max-w-7xl mx-auto px-4 flex items-center h-14 gap-6">
          <span className="font-bold text-lg">GVDB</span>
          <button
            onClick={() => setPage('dashboard')}
            className={`text-sm ${page === 'dashboard' ? 'text-blue-600 dark:text-blue-400 font-medium' : 'text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'}`}
          >
            Dashboard
          </button>
          <button
            onClick={() => setPage('collections')}
            className={`text-sm ${page === 'collections' ? 'text-blue-600 dark:text-blue-400 font-medium' : 'text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'}`}
          >
            Collections
          </button>
        </div>
      </nav>
      <main className="max-w-7xl mx-auto px-4 py-6">
        {page === 'dashboard' && <Dashboard />}
        {page === 'collections' && <Collections />}
      </main>
    </div>
  )
}
