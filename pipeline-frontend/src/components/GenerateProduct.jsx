import React, { useState } from 'react'
import { Zap, List } from 'lucide-react'
import { api } from '../api'
import { Card, Field, Input, Textarea, Btn, Alert, Spinner, SectionTitle, JsonViewer, Badge } from './UI'

function SingleProduct() {
  const [url,     setUrl]     = useState('')
  const [comply,  setComply]  = useState(true)
  const [result,  setResult]  = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)

  async function run() {
    if (!url.trim()) return
    setLoading(true); setError(null); setResult(null)
    try {
      const data = await api.generateProduct({ url: url.trim(), extract_compliance: comply })
      setResult(data)
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  return (
    <Card>
      <SectionTitle sub="Scrape → Store → Enhance → Categorize → Map → Excel">Single Product</SectionTitle>
      <Field label="AliExpress Product URL">
        <Input value={url} onChange={setUrl}
          placeholder="https://www.aliexpress.com/item/1005010388288135.html" />
      </Field>
      <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13,
        color: 'var(--text-muted)', marginBottom: 14, cursor: 'pointer' }}>
        <input type="checkbox" checked={comply} onChange={e => setComply(e.target.checked)} />
        Extract compliance data
      </label>
      <Btn onClick={run} disabled={loading || !url.trim()}>
        {loading ? <Spinner size={13} /> : <Zap size={13} />}
        {loading ? 'Processing…' : 'Generate Product'}
      </Btn>

      {error && <div style={{ marginTop: 14 }}><Alert type="error">{error}</Alert></div>}

      {result && (
        <div style={{ marginTop: 16 }}>
          <div style={{ display: 'flex', gap: 8, marginBottom: 10 }}>
            <Badge color={result.success ? 'green' : 'red'}>{result.success ? 'Success' : 'Failed'}</Badge>
            {result.product_id && <Badge color="accent">ID: {result.product_id}</Badge>}
            {result.category && <Badge color="purple">{result.category}</Badge>}
          </div>
          {result.enhanced_title && (
            <div style={{ background: 'var(--bg-input)', borderRadius: 'var(--radius)',
              padding: 12, marginBottom: 10, fontSize: 13 }}>
              <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>Enhanced Title</span>
              <p style={{ color: 'var(--text)', marginTop: 4 }}>{result.enhanced_title}</p>
            </div>
          )}
          <JsonViewer data={result} />
        </div>
      )}
    </Card>
  )
}

function BulkProducts() {
  const [urls,    setUrls]    = useState('')
  const [comply,  setComply]  = useState(false)
  const [result,  setResult]  = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)

  async function run() {
    const list = urls.split('\n').map(u => u.trim()).filter(Boolean)
    if (!list.length) return
    setLoading(true); setError(null); setResult(null)
    try {
      const data = await api.generateProducts({ urls: list, extract_compliance: comply })
      setResult(data)
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  const urlCount = urls.split('\n').filter(u => u.trim()).length

  return (
    <Card>
      <SectionTitle sub="Process multiple products in one request">Bulk Products</SectionTitle>
      <Field label={`AliExpress URLs (one per line) — ${urlCount} entered`}>
        <Textarea value={urls} onChange={setUrls} rows={6}
          placeholder={'https://www.aliexpress.com/item/1005010388288135.html\nhttps://www.aliexpress.com/item/1005006395261235.html'} />
      </Field>
      <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13,
        color: 'var(--text-muted)', marginBottom: 14, cursor: 'pointer' }}>
        <input type="checkbox" checked={comply} onChange={e => setComply(e.target.checked)} />
        Extract compliance data
      </label>
      <Btn onClick={run} disabled={loading || !urlCount}>
        {loading ? <Spinner size={13} /> : <List size={13} />}
        {loading ? `Processing ${urlCount} products…` : `Generate ${urlCount} Products`}
      </Btn>

      {error && <div style={{ marginTop: 14 }}><Alert type="error">{error}</Alert></div>}
      {result && <div style={{ marginTop: 16 }}><JsonViewer data={result} /></div>}
    </Card>
  )
}

export default function GenerateProduct() {
  return (
    <div>
      <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 4 }}>Generate Products</h1>
      <p style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 24 }}>
        Run the full pipeline: scrape → enhance → categorize → map → export.
      </p>
      <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
        <SingleProduct />
        <BulkProducts />
      </div>
    </div>
  )
}
