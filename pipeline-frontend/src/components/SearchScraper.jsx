import React, { useState } from 'react'
import { Search, ExternalLink } from 'lucide-react'
import { api } from '../api'
import { Card, Field, Input, Btn, Alert, Spinner, SectionTitle, Badge, Table } from './UI'

export default function SearchScraper() {
  const [url,     setUrl]     = useState('')
  const [pages,   setPages]   = useState('5')
  const [delay,   setDelay]   = useState('1.0')
  const [results, setResults] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)

  async function run() {
    if (!url.trim()) return
    setLoading(true); setError(null); setResults(null)
    try {
      const data = await api.scrapeSearch({
        search_url: url.trim(),
        max_pages: parseInt(pages) || 5,
        delay_between_requests: parseFloat(delay) || 1.0,
      })
      setResults(Array.isArray(data) ? data : [])
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  const accepted = results?.filter(r => r.status === 'accepted').length ?? 0
  const rejected = results?.filter(r => r.status === 'rejected').length ?? 0

  const cols = [
    { key: 'product_id', label: 'Product ID', maxWidth: 160 },
    { key: 'title',      label: 'Title', maxWidth: 280, wrap: true },
    { key: 'rating',     label: 'Rating', maxWidth: 70 },
    { key: 'sold_count', label: 'Sold', maxWidth: 80 },
    { key: 'status',     label: 'Status', render: v =>
        <Badge color={v === 'accepted' ? 'green' : 'red'}>{v}</Badge> },
    { key: 'message',    label: 'Rejection Reason', maxWidth: 220, wrap: true },
    { key: 'product_url',label: 'Link', render: v =>
        v ? <a href={v} target="_blank" rel="noreferrer" style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
          Open <ExternalLink size={11} /></a> : '—' },
  ]

  return (
    <div>
      <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 4 }}>Search Scraper</h1>
      <p style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 24 }}>
        Scrape AliExpress search result pages. Products are auto-filtered by rating ≥ 4.0 and keyword rules.
      </p>

      <Card style={{ marginBottom: 20 }}>
        <SectionTitle>Search Parameters</SectionTitle>
        <Field label="AliExpress Search URL">
          <Input
            value={url}
            onChange={setUrl}
            placeholder="https://www.aliexpress.com/w/wholesale-bags.html?SearchText=bags&page=1"
          />
        </Field>
        <div style={{ display: 'flex', gap: 12 }}>
          <Field label="Max Pages">
            <Input value={pages} onChange={setPages} type="number" style={{ width: 100 }} />
          </Field>
          <Field label="Delay (seconds)">
            <Input value={delay} onChange={setDelay} type="number" style={{ width: 100 }} />
          </Field>
        </div>
        <Btn onClick={run} disabled={loading || !url.trim()}>
          {loading ? <Spinner size={13} /> : <Search size={13} />}
          {loading ? 'Scraping…' : 'Run Scraper'}
        </Btn>
      </Card>

      {error && <Alert type="error">{error}</Alert>}

      {results && (
        <Card>
          <div style={{ display: 'flex', gap: 16, marginBottom: 18 }}>
            <SectionTitle style={{ margin: 0 }}>Results — {results.length} products found</SectionTitle>
            <span style={{ marginLeft: 'auto', display: 'flex', gap: 8, alignItems: 'center' }}>
              <Badge color="green">✓ {accepted} accepted</Badge>
              <Badge color="red">✗ {rejected} rejected</Badge>
            </span>
          </div>
          <Table columns={cols} rows={results} emptyText="No products found" />
        </Card>
      )}
    </div>
  )
}
