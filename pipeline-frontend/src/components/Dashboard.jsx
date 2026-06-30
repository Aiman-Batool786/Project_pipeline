import React, { useEffect, useState } from 'react'
import { Activity, Database, RefreshCw, Wifi, WifiOff } from 'lucide-react'
import { api } from '../api'
import { StatCard, Card, Alert, Btn, Spinner, SectionTitle, Badge } from './UI'

export default function Dashboard() {
  const [stats,   setStats]   = useState(null)
  const [health,  setHealth]  = useState(null)
  const [logs,    setLogs]    = useState([])
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)

  async function load() {
    setLoading(true); setError(null)
    try {
      const [s, h, l] = await Promise.all([api.stats(), api.health(), api.processingLogs(10)])
      setStats(s); setHealth(h)
      setLogs(Array.isArray(l) ? l : [])
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  const statDefs = [
    { key: 'scraped_products',    label: 'Scraped Products',  color: 'accent'  },
    { key: 'mapped_products',     label: 'Mapped Products',   color: 'green'   },
    { key: 'template_outputs',    label: 'Template Outputs',  color: 'purple'  },
    { key: 'enhanced_content',    label: 'Enhanced Content',  color: 'yellow'  },
    { key: 'translation',         label: 'Translations',      color: 'accent'  },
    { key: 'varient',             label: 'Variants',          color: 'green'   },
    { key: 'seller_info',         label: 'Seller Records',    color: 'purple'  },
    { key: 'compliance_info',     label: 'Compliance Records',color: 'yellow'  },
  ]

  const levelColor = l => l === 'ERROR' ? 'red' : l === 'WARNING' ? 'yellow' : l === 'SUCCESS' ? 'green' : 'muted'

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24 }}>
        <div>
          <h1 style={{ fontSize: 20, fontWeight: 700 }}>Dashboard</h1>
          <p style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 2 }}>Octopia Template Pipeline · v3.4</p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          {health && (
            <span style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 12,
              color: health.status === 'healthy' ? 'var(--green)' : 'var(--red)' }}>
              {health.status === 'healthy' ? <Wifi size={13} /> : <WifiOff size={13} />}
              {health.status === 'healthy' ? 'API Online' : 'API Error'}
            </span>
          )}
          <Btn variant="secondary" onClick={load} disabled={loading} small>
            {loading ? <Spinner size={12} /> : <RefreshCw size={12} />} Refresh
          </Btn>
        </div>
      </div>

      {error && <Alert type="error" style={{ marginBottom: 16 }}>{error}</Alert>}

      {loading && !stats ? (
        <div style={{ textAlign: 'center', padding: 60, color: 'var(--text-muted)' }}>
          <Spinner size={24} /><p style={{ marginTop: 12 }}>Loading dashboard…</p>
        </div>
      ) : stats ? (
        <>
          {/* Stats grid */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: 12, marginBottom: 24 }}>
            {statDefs.map(s => (
              <StatCard key={s.key} label={s.label} value={stats[s.key] ?? 0} color={s.color} />
            ))}
          </div>

          {/* DB coverage */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 }}>
            <Card>
              <SectionTitle>Pipeline Coverage</SectionTitle>
              {statDefs.map(s => {
                const val = stats[s.key] ?? 0
                const pct = stats.scraped_products > 0 ? Math.min(100, Math.round(val / stats.scraped_products * 100)) : 0
                return (
                  <div key={s.key} style={{ marginBottom: 10 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12,
                      color: 'var(--text-muted)', marginBottom: 4 }}>
                      <span>{s.label}</span>
                      <span>{pct}%</span>
                    </div>
                    <div style={{ background: 'var(--bg-input)', borderRadius: 4, height: 6 }}>
                      <div style={{ width: `${pct}%`, height: '100%', borderRadius: 4,
                        background: `var(--${s.color === 'accent' ? 'accent' : s.color})`,
                        transition: 'width .4s ease' }} />
                    </div>
                  </div>
                )
              })}
            </Card>

            <Card>
              <SectionTitle>Recent Activity</SectionTitle>
              {logs.length === 0 ? (
                <p style={{ fontSize: 12, color: 'var(--text-muted)' }}>No recent logs</p>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {logs.slice(0, 8).map((log, i) => (
                    <div key={i} style={{ display: 'flex', gap: 8, alignItems: 'flex-start' }}>
                      <Badge color={levelColor(log.level)}>{log.level || 'INFO'}</Badge>
                      <div>
                        <div style={{ fontSize: 12, color: 'var(--text)' }}>{log.message}</div>
                        <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>{log.log_time}</div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </Card>
          </div>

          <Card>
            <SectionTitle sub="All tables in the products.db SQLite database">Database Tables</SectionTitle>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: 8 }}>
              {Object.entries(stats).map(([k, v]) => (
                <div key={k} style={{ display: 'flex', justifyContent: 'space-between',
                  background: 'var(--bg-input)', padding: '8px 12px', borderRadius: 'var(--radius)',
                  fontSize: 12 }}>
                  <span style={{ color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>{k}</span>
                  <span style={{ color: 'var(--accent)', fontFamily: 'var(--font-mono)', fontWeight: 600 }}>{v}</span>
                </div>
              ))}
            </div>
          </Card>
        </>
      ) : null}
    </div>
  )
}
