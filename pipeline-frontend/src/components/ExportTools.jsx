import React, { useState } from 'react'
import { FileSpreadsheet, Filter, RefreshCw } from 'lucide-react'
import { api } from '../api'
import { Card, Btn, Alert, Spinner, SectionTitle, JsonViewer } from './UI'

export default function ExportTools() {
  const [exportResult, setExportResult] = useState(null)
  const [filterResult, setFilterResult] = useState(null)
  const [loading,      setLoading]      = useState(null)
  const [error,        setError]        = useState(null)
  const [onlyNew,      setOnlyNew]      = useState(false)

  async function runExport() {
    setLoading('export'); setError(null); setExportResult(null)
    try {
      const data = await api.exportTemplates(onlyNew)
      setExportResult(data)
    } catch (e) { setError(e.message) }
    finally { setLoading(null) }
  }

  async function reloadFilters() {
    setLoading('filter'); setError(null); setFilterResult(null)
    try {
      const data = await api.reloadFilters()
      setFilterResult(data)
    } catch (e) { setError(e.message) }
    finally { setLoading(null) }
  }

  return (
    <div>
      <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 4 }}>Export & Tools</h1>
      <p style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 24 }}>
        Export templates to Excel and manage pipeline filters.
      </p>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        <Card>
          <SectionTitle sub="Batch-export all categorized products to per-category .xlsm files">
            Export Templates
          </SectionTitle>
          <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13,
            color: 'var(--text-muted)', marginBottom: 14, cursor: 'pointer' }}>
            <input type="checkbox" checked={onlyNew} onChange={e => setOnlyNew(e.target.checked)} />
            Incremental export — only export products not yet exported
          </label>
          <Btn onClick={runExport} disabled={loading === 'export'} variant="success">
            {loading === 'export' ? <Spinner size={13} /> : <FileSpreadsheet size={13} />}
            {loading === 'export' ? 'Exporting…' : 'Export All Templates'}
          </Btn>
          {exportResult && (
            <div style={{ marginTop: 14 }}>
              <Alert type="success">Export complete</Alert>
              <div style={{ marginTop: 10 }}><JsonViewer data={exportResult} /></div>
            </div>
          )}
        </Card>

        <Card>
          <SectionTitle sub="Reload category filter data from disk without restarting the server">
            Reload Filters
          </SectionTitle>
          <p style={{ fontSize: 13, color: 'var(--text-muted)', marginBottom: 14 }}>
            Use this after updating keyword lists, category embeddings, or restricted keywords.
          </p>
          <Btn onClick={reloadFilters} disabled={loading === 'filter'} variant="secondary">
            {loading === 'filter' ? <Spinner size={13} /> : <RefreshCw size={13} />}
            {loading === 'filter' ? 'Reloading…' : 'Reload Filter Data'}
          </Btn>
          {filterResult && (
            <div style={{ marginTop: 14 }}>
              <Alert type="success">Filters reloaded successfully</Alert>
              <div style={{ marginTop: 10 }}><JsonViewer data={filterResult} /></div>
            </div>
          )}
        </Card>

        {error && <Alert type="error">{error}</Alert>}
      </div>
    </div>
  )
}
