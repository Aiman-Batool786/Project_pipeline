import React, { useState, useEffect, useRef } from 'react'
import { Store, Play, Square, Download, RefreshCw } from 'lucide-react'
import { api } from '../api'
import { Card, Field, Textarea, Btn, Alert, Spinner, SectionTitle, Badge, Table } from './UI'

function JobCard({ job, onStop, onRefresh }) {
  const statusColor = s =>
    s === 'completed' ? 'green' : s === 'running' ? 'accent' :
    s === 'failed' ? 'red' : s === 'stopped' ? 'yellow' : 'muted'

  return (
    <div style={{ background: 'var(--bg-input)', border: '1px solid var(--border)',
      borderRadius: 'var(--radius)', padding: 14, marginBottom: 10 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <span style={{ fontFamily: 'var(--font-mono)', fontSize: 12, color: 'var(--text-muted)' }}>
            {job.job_id?.slice(0, 8)}…
          </span>
          <Badge color={statusColor(job.status)}>{job.status}</Badge>
        </div>
        <div style={{ display: 'flex', gap: 6 }}>
          {job.status === 'running' && (
            <Btn variant="danger" small onClick={() => onStop(job.job_id)}>
              <Square size={11} /> Stop
            </Btn>
          )}
          {job.status === 'completed' && (
            <a href={api.merchantDownload(job.job_id)} download>
              <Btn variant="success" small><Download size={11} /> Download</Btn>
            </a>
          )}
          <Btn variant="ghost" small onClick={() => onRefresh(job.job_id)}>
            <RefreshCw size={11} />
          </Btn>
        </div>
      </div>
      <div style={{ display: 'flex', gap: 16, fontSize: 12, color: 'var(--text-muted)' }}>
        <span>Progress: <b style={{ color: 'var(--text)' }}>{job.progress ?? 0}%</b></span>
        <span>Merchants: <b style={{ color: 'var(--text)' }}>{job.total_merchants ?? '?'}</b></span>
        {job.created_at && <span>Started: {job.created_at.slice(0, 19)}</span>}
      </div>
      {job.status === 'running' && (
        <div style={{ marginTop: 8, background: 'var(--border)', borderRadius: 4, height: 4 }}>
          <div style={{ width: `${job.progress || 0}%`, height: '100%', borderRadius: 4,
            background: 'var(--accent)', transition: 'width .3s' }} />
        </div>
      )}
    </div>
  )
}

export default function MerchantBulk() {
  const [ids,     setIds]     = useState('')
  const [jobs,    setJobs]    = useState([])
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)
  const [success, setSuccess] = useState(null)
  const pollRef = useRef(null)

  async function loadJobs() {
    try {
      const data = await api.merchantJobs()
      setJobs(Array.isArray(data) ? data : Object.values(data || {}))
    } catch {}
  }

  useEffect(() => {
    loadJobs()
    pollRef.current = setInterval(loadJobs, 5000)
    return () => clearInterval(pollRef.current)
  }, [])

  async function submitIds() {
    const list = ids.split('\n').map(s => s.trim()).filter(Boolean)
    if (!list.length) return
    setLoading(true); setError(null); setSuccess(null)
    try {
      const data = await api.submitMerchantIds(list)
      setSuccess(`Job started: ${data.job_id}`)
      setIds('')
      loadJobs()
    } catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  async function stopJob(jobId) {
    try { await api.merchantStop(jobId); loadJobs() } catch {}
  }

  async function refreshJob(jobId) {
    try {
      const data = await api.merchantJobStatus(jobId)
      setJobs(prev => prev.map(j => j.job_id === jobId ? { ...j, ...data } : j))
    } catch {}
  }

  const idCount = ids.split('\n').filter(s => s.trim()).length

  return (
    <div>
      <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 4 }}>Merchant Bulk Scraper</h1>
      <p style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 24 }}>
        Get item counts for multiple AliExpress merchant stores simultaneously.
      </p>

      <Card style={{ marginBottom: 20 }}>
        <SectionTitle>Submit Merchant IDs</SectionTitle>
        <Field label={`Merchant IDs (one per line) — ${idCount} entered`}>
          <Textarea value={ids} onChange={setIds} rows={5}
            placeholder={'1103833861\n912519001\n567839201'} />
        </Field>
        <Btn onClick={submitIds} disabled={loading || !idCount}>
          {loading ? <Spinner size={13} /> : <Play size={13} />}
          {loading ? 'Starting job…' : `Start Job (${idCount} merchants)`}
        </Btn>

        {error   && <div style={{ marginTop: 12 }}><Alert type="error">{error}</Alert></div>}
        {success && <div style={{ marginTop: 12 }}><Alert type="success">{success}</Alert></div>}
      </Card>

      <Card>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <SectionTitle style={{ margin: 0 }}>
            Jobs {jobs.length > 0 && <span style={{ fontWeight: 400, color: 'var(--text-muted)', fontSize: 13 }}>
              ({jobs.length})
            </span>}
          </SectionTitle>
          <Btn variant="ghost" small onClick={loadJobs}>
            <RefreshCw size={12} /> Refresh
          </Btn>
        </div>

        {jobs.length === 0 ? (
          <p style={{ fontSize: 13, color: 'var(--text-muted)', textAlign: 'center', padding: 24 }}>
            No jobs yet. Submit merchant IDs above to start.
          </p>
        ) : (
          jobs.map(job => (
            <JobCard key={job.job_id} job={job} onStop={stopJob} onRefresh={refreshJob} />
          ))
        )}
      </Card>
    </div>
  )
}
