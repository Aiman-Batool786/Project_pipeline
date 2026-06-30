import React, { useState } from 'react'
import Sidebar      from './components/Sidebar'
import Dashboard    from './components/Dashboard'
import SearchScraper from './components/SearchScraper'
import GenerateProduct from './components/GenerateProduct'
import DatabaseViewer  from './components/DatabaseViewer'
import Translation  from './components/Translation'
import Variants     from './components/Variants'
import MerchantBulk from './components/MerchantBulk'
import ExportTools  from './components/ExportTools'

const PAGES = {
  dashboard:   Dashboard,
  search:      SearchScraper,
  generate:    GenerateProduct,
  database:    DatabaseViewer,
  translation: Translation,
  variants:    Variants,
  merchant:    MerchantBulk,
  export:      ExportTools,
}

export default function App() {
  const [page, setPage] = useState('dashboard')
  const Page = PAGES[page] || Dashboard

  return (
    <div style={{ display: 'flex', minHeight: '100vh' }}>
      <Sidebar active={page} onChange={setPage} />
      <main style={{ flex: 1, padding: 28, overflowY: 'auto', minWidth: 0 }}>
        <Page />
      </main>
    </div>
  )
}
