import React, { useState } from 'react'
import { Languages, Search } from 'lucide-react'
import { api } from '../api'
import { Card, Field, Input, Btn, Alert, Spinner, SectionTitle, Badge } from './UI'

const LANGS = ['Romanian', 'German', 'Portuguese', 'Spanish', 'French']
const LANG_FLAGS = { Romanian: '🇷🇴', German: '🇩🇪', Portuguese: '🇵🇹', Spanish: '🇪🇸', French: '🇫🇷' }

export default function Translation() {
  const [id,        setId]        = useState('')
  const [result,    setResult]    = useState(null)
  const [viewMode,  setViewMode]  = useState('translate')
  const [loading,   setLoading]   = useState(false)
  const [error,     setError]     = useState(null)
  const [activeLang,setActiveLang]= useState(null)

  async function translate() {
    if (!id.trim()) return
    setLoading(true); setError(null); setResult(null)
    try {
      const data = await api.translateProduct(id.trim())
      setResult(data)
      setActiveLang(Object.keys(data.translations || {})[0] || null)
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  async function fetchExisting() {
    if (!id.trim()) return
    setLoading(true); setError(null); setResult(null)
    try {
      const data = await api.translations(id.trim())
      setResult(data)
      setActiveLang(Object.keys(data || {})[0] || null)
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  const translations = result?.translations || result || {}
  const langs = Object.keys(translations)

  return (
    <div>
      <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 4 }}>Translation</h1>
      <p style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 24 }}>
        Translate product title, description, and specs into {LANGS.join(', ')}.
      </p>

      <Card style={{ marginBottom: 20 }}>
        <SectionTitle>Translate Product</SectionTitle>

        <div style={{ display: 'flex', gap: 8, marginBottom: 14 }}>
          {['translate', 'view'].map(m => (
            <button key={m} onClick={() => setViewMode(m)}
              style={{ background: viewMode === m ? 'var(--accent-dim)' : 'var(--bg-input)',
                border: `1px solid ${viewMode === m ? 'var(--accent)' : 'var(--border)'}`,
                color: viewMode === m ? 'var(--accent)' : 'var(--text-muted)',
                borderRadius: 'var(--radius)', padding: '5px 14px', fontSize: 12, cursor: 'pointer' }}>
              {m === 'translate' ? '🔄 Translate Now' : '📂 View Existing'}
            </button>
          ))}
        </div>

        <Field label="AliExpress Product ID (numeric)">
          <Input value={id} onChange={setId} placeholder="e.g. 1005010388288135" />
        </Field>

        {viewMode === 'translate' ? (
          <>
            <p style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 12 }}>
              This will scrape the product (or reuse cached data) and translate into all 5 languages.
            </p>
            <Btn onClick={translate} disabled={loading || !id.trim()}>
              {loading ? <Spinner size={13} /> : <Languages size={13} />}
              {loading ? 'Translating…' : 'Translate Product'}
            </Btn>
          </>
        ) : (
          <Btn onClick={fetchExisting} disabled={loading || !id.trim()} variant="secondary">
            {loading ? <Spinner size={13} /> : <Search size={13} />}
            {loading ? 'Loading…' : 'Fetch Translations'}
          </Btn>
        )}

        {error && <div style={{ marginTop: 14 }}><Alert type="error">{error}</Alert></div>}
      </Card>

      {langs.length > 0 && (
        <Card>
          <div style={{ display: 'flex', gap: 6, marginBottom: 16, flexWrap: 'wrap' }}>
            {langs.map(lang => (
              <button key={lang} onClick={() => setActiveLang(lang)}
                style={{ background: activeLang === lang ? 'var(--accent-dim)' : 'var(--bg-input)',
                  border: `1px solid ${activeLang === lang ? 'var(--accent)' : 'var(--border)'}`,
                  color: activeLang === lang ? 'var(--accent)' : 'var(--text)',
                  borderRadius: 'var(--radius)', padding: '6px 14px', fontSize: 13,
                  cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 6 }}>
                <span>{LANG_FLAGS[lang] || '🌐'}</span> {lang}
              </button>
            ))}
          </div>

          {activeLang && translations[activeLang] && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
              {['title', 'description', 'specification'].map(field => {
                const val = translations[activeLang][field]
                if (!val) return null
                return (
                  <div key={field} style={{ background: 'var(--bg-input)',
                    borderRadius: 'var(--radius)', padding: 14 }}>
                    <div style={{ fontSize: 11, color: 'var(--text-muted)', fontWeight: 600,
                      textTransform: 'uppercase', letterSpacing: '.4px', marginBottom: 6 }}>
                      {field}
                    </div>
                    <p style={{ fontSize: 13, color: 'var(--text)', lineHeight: 1.7 }}>{val}</p>
                  </div>
                )
              })}
            </div>
          )}
        </Card>
      )}
    </div>
  )
}
