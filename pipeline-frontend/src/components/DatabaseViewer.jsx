import React, { useState, useEffect } from 'react'
import { RefreshCw } from 'lucide-react'
import { api } from '../api'
import { Card, Btn, Alert, Spinner, SectionTitle, Table, Badge } from './UI'

const TABS = [
  { id: 'scraped',    label: 'Scraped Products' },
  { id: 'seller',     label: 'Seller Info'      },
  { id: 'compliance', label: 'Compliance'        },
  { id: 'logs',       label: 'Processing Logs'  },
]

const SCRAPED_COLS = [
  { key: 'product_id',      label: 'ID',       maxWidth: 80 },
  { key: 'title',           label: 'Title',    maxWidth: 260, wrap: true },
  { key: 'price',           label: 'Price',    maxWidth: 90 },
  { key: 'rating',          label: 'Rating',   maxWidth: 70 },
  { key: 'sold_count',      label: 'Sold',     maxWidth: 80 },
  { key: 'category',        label: 'Category', maxWidth: 140 },
  { key: 'scraped_at',      label: 'Scraped',  maxWidth: 160, render: v => v?.slice(0, 19) ?? '—' },
]

const SELLER_COLS = [
  { key: 'product_id',      label: 'Product',   maxWidth: 80 },
  { key: 'store_name',      label: 'Store',     maxWidth: 180 },
  { key: 'store_id',        label: 'Store ID',  maxWidth: 100 },
  { key: 'seller_rating',   label: 'Rating',    maxWidth: 80 },
  { key: 'seller_country',  label: 'Country',   maxWidth: 100 },
  { key: 'is_top_rated',    label: 'Top Rated', maxWidth: 90,
    render: v => <Badge color={v ? 'green' : 'muted'}>{v ? 'Yes' : 'No'}</Badge> },
  { key: 'scraped_at',      label: 'Scraped',   maxWidth: 160, render: v => v?.slice(0, 19) ?? '—' },
]

const COMPLIANCE_COLS = [
  { key: 'product_id',      label: 'Product',     maxWidth: 80  },
  { key: 'brand',           label: 'Brand',       maxWidth: 130 },
  { key: 'certifications',  label: 'Certs',       maxWidth: 180, wrap: true },
  { key: 'country_of_origin',label:'Origin',      maxWidth: 100 },
  { key: 'warranty',        label: 'Warranty',    maxWidth: 120 },
  { key: 'extracted_at',    label: 'Extracted',   maxWidth: 160, render: v => v?.slice(0, 19) ?? '—' },
]

const LOG_COLS = [
  { key: 'log_time',  label: 'Time',    maxWidth: 170, render: v => v?.slice(0, 19) ?? '—' },
  { key: 'level',     label: 'Level',   maxWidth: 90,
    render: v => {
      const c = v === 'ERROR' ? 'red' : v === 'WARNING' ? 'yellow' : v === 'SUCCESS' ? 'green' : 'muted'
      return <Badge color={c}>{v || 'INFO'}</Badge>
    }},
  { key: 'step',      label: 'Step',    maxWidth: 130 },
  { key: 'message',   label: 'Message', maxWidth: 340, wrap: true },
]

export default function DatabaseViewer() {
  const [tab,     setTab]     = useState('scraped')
  const [data,    setData]    = useState({})
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)
  const [limit,   setLimit]   = useState(100)

  async function load() {
    setLoading(true); setError(null)
    try {
      let rows
      if (tab === 'scraped')    rows = await api.scrapedProducts(limit)
      if (tab === 'seller')     rows = await api.sellerInfo(limit)
      if (tab === 'compliance') rows = await api.complianceInfo(limit)
      if (tab === 'logs')       rows = await api.processingLogs(limit)
      setData(prev => ({ ...prev, [tab]: Array.isArray(rows) ? rows : [] }))
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  useEffect(() => { load() }, [tab])

  const rows = data[tab] || []
  const cols = tab === 'scraped' ? SCRAPED_COLS
             : tab === 'seller'  ? SELLER_COLS
             : tab === 'compliance' ? COMPLIANCE_COLS
             : LOG_COLS

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <h1 style={{ fontSize: 20, fontWeight: 700 }}>Database Viewer</h1>
          <p style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 2 }}>Browse data stored in products.db</p>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <select value={limit} onChange={e => setLimit(Number(e.target.value))}
            style={{ background: 'var(--bg-input)', border: '1px solid var(--border)',
              color: 'var(--text)', borderRadius: 'var(--radius)', padding: '6px 10px', fontSize: 12 }}>
            {[50, 100, 250, 500].map(n => <option key={n} value={n}>{n} rows</option>)}
          </select>
          <Btn variant="secondary" onClick={load} disabled={loading} small>
            {loading ? <Spinner size={12} /> : <RefreshCw size={12} />} Refresh
          </Btn>
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: 'flex', gap: 2, marginBottom: 16, borderBottom: '1px solid var(--border)' }}>
        {TABS.map(t => (
          <button key={t.id} onClick={() => setTab(t.id)}
            style={{ background: 'none', border: 'none', padding: '8px 16px', fontSize: 13,
              color: tab === t.id ? 'var(--accent)' : 'var(--text-muted)',
              borderBottom: tab === t.id ? '2px solid var(--accent)' : '2px solid transparent',
              cursor: 'pointer', transition: 'color .15s', fontWeight: tab === t.id ? 600 : 400 }}>
            {t.label}
            {data[t.id] && (
              <span style={{ marginLeft: 6, fontSize: 11, background: 'var(--bg-input)',
                padding: '1px 6px', borderRadius: 10, color: 'var(--text-muted)' }}>
                {data[t.id].length}
              </span>
            )}
          </button>
        ))}
      </div>

      {error && <div style={{ marginBottom: 12 }}><Alert type="error">{error}</Alert></div>}

      {loading ? (
        <div style={{ textAlign: 'center', padding: 40, color: 'var(--text-muted)' }}>
          <Spinner size={20} /><p style={{ marginTop: 8 }}>Loading…</p>
        </div>
      ) : (
        <Card style={{ padding: 0, overflow: 'hidden' }}>
          <Table columns={cols} rows={rows} emptyText={`No ${tab} records found`} />
        </Card>
      )}
    </div>
  )
}
