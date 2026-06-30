import React, { useState } from 'react'
import { Layers, Search, Trash2 } from 'lucide-react'
import { api } from '../api'
import { Card, Field, Input, Btn, Alert, Spinner, SectionTitle, JsonViewer, Badge, StatCard } from './UI'

export default function Variants() {
  const [id,       setId]      = useState('')
  const [force,    setForce]   = useState(false)
  const [result,   setResult]  = useState(null)
  const [summary,  setSummary] = useState(null)
  const [loading,  setLoading] = useState(false)
  const [error,    setError]   = useState(null)
  const [action,   setAction]  = useState(null)

  async function scrape() {
    if (!id.trim()) return
    setLoading(true); setError(null); setResult(null); setSummary(null); setAction('scrape')
    try {
      const data = await api.scrapeVariants({ product_id: id.trim(), force_rescrape: force })
      setResult(data)
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  async function fetchDb() {
    if (!id.trim()) return
    setLoading(true); setError(null); setResult(null); setSummary(null); setAction('fetch')
    try {
      const [d, s] = await Promise.all([api.dbVariants(id.trim()), api.variantSummary(id.trim())])
      setResult(d); setSummary(s)
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  async function deleteVariants() {
    if (!id.trim() || !confirm('Delete all variants for this product?')) return
    setLoading(true); setError(null); setAction('delete')
    try {
      const data = await api.deleteVariants(id.trim())
      setResult(data); setSummary(null)
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  return (
    <div>
      <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 4 }}>Variant Scraper</h1>
      <p style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 24 }}>
        Scrape and store product variants (color, size, country) from AliExpress.
      </p>

      <Card style={{ marginBottom: 20 }}>
        <SectionTitle>Variant Operations</SectionTitle>
        <Field label="Product ID">
          <Input value={id} onChange={setId} placeholder="e.g. 1005012117886583" />
        </Field>
        <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13,
          color: 'var(--text-muted)', marginBottom: 14, cursor: 'pointer' }}>
          <input type="checkbox" checked={force} onChange={e => setForce(e.target.checked)} />
          Force re-scrape (ignore cache)
        </label>
        <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
          <Btn onClick={scrape} disabled={loading || !id.trim()}>
            {loading && action === 'scrape' ? <Spinner size={13} /> : <Layers size={13} />}
            Scrape Variants
          </Btn>
          <Btn variant="secondary" onClick={fetchDb} disabled={loading || !id.trim()}>
            {loading && action === 'fetch' ? <Spinner size={13} /> : <Search size={13} />}
            View from DB
          </Btn>
          <Btn variant="danger" onClick={deleteVariants} disabled={loading || !id.trim()}>
            {loading && action === 'delete' ? <Spinner size={13} /> : <Trash2 size={13} />}
            Delete Variants
          </Btn>
        </div>

        {error && <div style={{ marginTop: 14 }}><Alert type="error">{error}</Alert></div>}
      </Card>

      {summary && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 12, marginBottom: 20 }}>
          <StatCard label="Colors"    value={summary.colors    ?? 0} color="accent"  />
          <StatCard label="Sizes"     value={summary.sizes     ?? 0} color="green"   />
          <StatCard label="Countries" value={summary.countries ?? 0} color="purple"  />
        </div>
      )}

      {result && (
        <Card>
          <SectionTitle>Result</SectionTitle>
          <JsonViewer data={result} />
        </Card>
      )}
    </div>
  )
}
