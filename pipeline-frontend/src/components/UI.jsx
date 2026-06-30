import React from 'react'
import { Loader2, CheckCircle2, XCircle, AlertCircle, Info } from 'lucide-react'

/* ── Card ─────────────────────────────────────────────────────── */
export function Card({ children, style, className = '' }) {
  return (
    <div className={`card ${className}`} style={style}>
      {children}
      <style>{`
        .card {
          background: var(--bg-card);
          border: 1px solid var(--border);
          border-radius: var(--radius-lg);
          padding: 20px;
        }
      `}</style>
    </div>
  )
}

/* ── Button ───────────────────────────────────────────────────── */
export function Btn({ children, onClick, variant = 'primary', disabled, small, style }) {
  const map = {
    primary:  { bg: 'var(--accent)',  color: '#0d1117', border: 'var(--accent)' },
    success:  { bg: 'var(--green)',   color: '#0d1117', border: 'var(--green)'  },
    danger:   { bg: 'var(--red)',     color: '#fff',    border: 'var(--red)'    },
    ghost:    { bg: 'transparent',    color: 'var(--text)', border: 'var(--border)' },
    secondary:{ bg: 'var(--bg-input)',color: 'var(--text)', border: 'var(--border)' },
  }
  const v = map[variant] || map.primary
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        background: v.bg, color: v.color, border: `1px solid ${v.border}`,
        borderRadius: 'var(--radius)', padding: small ? '4px 10px' : '8px 16px',
        fontSize: small ? '12px' : '13px', fontWeight: 500,
        opacity: disabled ? 0.5 : 1, cursor: disabled ? 'not-allowed' : 'pointer',
        display: 'inline-flex', alignItems: 'center', gap: 6, transition: 'opacity .15s',
        whiteSpace: 'nowrap', ...style
      }}
    >
      {children}
    </button>
  )
}

/* ── Input ────────────────────────────────────────────────────── */
export function Input({ value, onChange, placeholder, type = 'text', style }) {
  return (
    <input
      type={type}
      value={value}
      onChange={e => onChange(e.target.value)}
      placeholder={placeholder}
      style={{
        background: 'var(--bg-input)', border: '1px solid var(--border)',
        borderRadius: 'var(--radius)', color: 'var(--text)',
        padding: '8px 12px', fontSize: 13, width: '100%', outline: 'none',
        transition: 'border-color .15s', ...style
      }}
      onFocus={e => e.target.style.borderColor = 'var(--border-focus)'}
      onBlur={e  => e.target.style.borderColor = 'var(--border)'}
    />
  )
}

/* ── Textarea ─────────────────────────────────────────────────── */
export function Textarea({ value, onChange, placeholder, rows = 4, style }) {
  return (
    <textarea
      value={value}
      onChange={e => onChange(e.target.value)}
      placeholder={placeholder}
      rows={rows}
      style={{
        background: 'var(--bg-input)', border: '1px solid var(--border)',
        borderRadius: 'var(--radius)', color: 'var(--text)',
        padding: '8px 12px', fontSize: 13, width: '100%', outline: 'none',
        resize: 'vertical', transition: 'border-color .15s', ...style
      }}
      onFocus={e => e.target.style.borderColor = 'var(--border-focus)'}
      onBlur={e  => e.target.style.borderColor = 'var(--border)'}
    />
  )
}

/* ── Label ────────────────────────────────────────────────────── */
export function Label({ children }) {
  return (
    <label style={{ fontSize: 12, color: 'var(--text-muted)', fontWeight: 500,
      textTransform: 'uppercase', letterSpacing: '.5px', display: 'block', marginBottom: 6 }}>
      {children}
    </label>
  )
}

/* ── Field ────────────────────────────────────────────────────── */
export function Field({ label, children }) {
  return (
    <div style={{ marginBottom: 14 }}>
      {label && <Label>{label}</Label>}
      {children}
    </div>
  )
}

/* ── Spinner ──────────────────────────────────────────────────── */
export function Spinner({ size = 16 }) {
  return <Loader2 size={size} style={{ animation: 'spin 1s linear infinite' }} />
}

/* ── Alert ────────────────────────────────────────────────────── */
export function Alert({ type = 'info', children }) {
  const map = {
    info:    { icon: Info,         bg: 'var(--accent-dim)',  border: 'var(--accent)',  color: 'var(--accent)' },
    success: { icon: CheckCircle2, bg: 'var(--green-dim)',   border: 'var(--green)',   color: 'var(--green)'  },
    error:   { icon: XCircle,      bg: 'var(--red-dim)',     border: 'var(--red)',     color: 'var(--red)'    },
    warning: { icon: AlertCircle,  bg: 'var(--yellow-dim)',  border: 'var(--yellow)',  color: 'var(--yellow)' },
  }
  const m = map[type]
  const Icon = m.icon
  return (
    <div style={{ background: m.bg, border: `1px solid ${m.border}`, borderRadius: 'var(--radius)',
      padding: '10px 14px', display: 'flex', alignItems: 'flex-start', gap: 8, fontSize: 13 }}>
      <Icon size={15} color={m.color} style={{ flexShrink: 0, marginTop: 1 }} />
      <span style={{ color: 'var(--text)' }}>{children}</span>
    </div>
  )
}

/* ── Badge ────────────────────────────────────────────────────── */
export function Badge({ children, color = 'accent' }) {
  const map = {
    accent:  { bg: 'var(--accent-dim)',  text: 'var(--accent)'  },
    green:   { bg: 'var(--green-dim)',   text: 'var(--green)'   },
    red:     { bg: 'var(--red-dim)',     text: 'var(--red)'     },
    yellow:  { bg: 'var(--yellow-dim)',  text: 'var(--yellow)'  },
    purple:  { bg: 'var(--purple-dim)',  text: 'var(--purple)'  },
    muted:   { bg: 'var(--bg-input)',    text: 'var(--text-muted)' },
  }
  const c = map[color] || map.accent
  return (
    <span style={{ background: c.bg, color: c.text, padding: '2px 8px',
      borderRadius: 20, fontSize: 11, fontWeight: 600, letterSpacing: '.3px' }}>
      {children}
    </span>
  )
}

/* ── Table ────────────────────────────────────────────────────── */
export function Table({ columns, rows, emptyText = 'No data' }) {
  if (!rows || rows.length === 0)
    return <p style={{ color: 'var(--text-muted)', fontSize: 13, textAlign: 'center', padding: 24 }}>{emptyText}</p>
  return (
    <div style={{ overflowX: 'auto', borderRadius: 'var(--radius)', border: '1px solid var(--border)' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
        <thead>
          <tr style={{ background: 'var(--bg-input)' }}>
            {columns.map(c => (
              <th key={c.key} style={{ padding: '10px 14px', textAlign: 'left',
                color: 'var(--text-muted)', fontWeight: 600, fontSize: 11,
                textTransform: 'uppercase', letterSpacing: '.5px',
                whiteSpace: 'nowrap', borderBottom: '1px solid var(--border)' }}>
                {c.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} style={{ borderBottom: i < rows.length - 1 ? '1px solid var(--border)' : 'none' }}
              onMouseEnter={e => e.currentTarget.style.background = 'var(--bg-input)'}
              onMouseLeave={e => e.currentTarget.style.background = 'transparent'}>
              {columns.map(c => (
                <td key={c.key} style={{ padding: '10px 14px', color: 'var(--text)',
                  maxWidth: c.maxWidth || 300, overflow: 'hidden',
                  textOverflow: 'ellipsis', whiteSpace: c.wrap ? 'normal' : 'nowrap' }}>
                  {c.render ? c.render(row[c.key], row) : (row[c.key] ?? '—')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

/* ── JsonViewer ───────────────────────────────────────────────── */
export function JsonViewer({ data }) {
  return (
    <pre style={{ background: 'var(--bg-input)', border: '1px solid var(--border)',
      borderRadius: 'var(--radius)', padding: 14, fontSize: 12, overflowX: 'auto',
      color: 'var(--text)', maxHeight: 400, overflowY: 'auto', lineHeight: 1.5 }}>
      {JSON.stringify(data, null, 2)}
    </pre>
  )
}

/* ── Section heading ──────────────────────────────────────────── */
export function SectionTitle({ children, sub }) {
  return (
    <div style={{ marginBottom: 18 }}>
      <h2 style={{ fontSize: 16, fontWeight: 600, color: 'var(--text)' }}>{children}</h2>
      {sub && <p style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 2 }}>{sub}</p>}
    </div>
  )
}

/* ── Stat card ────────────────────────────────────────────────── */
export function StatCard({ label, value, color = 'accent' }) {
  const colorMap = {
    accent: 'var(--accent)', green: 'var(--green)', yellow: 'var(--yellow)',
    red: 'var(--red)', purple: 'var(--purple)',
  }
  return (
    <div style={{ background: 'var(--bg-card)', border: '1px solid var(--border)',
      borderRadius: 'var(--radius-lg)', padding: '16px 20px' }}>
      <div style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase',
        letterSpacing: '.5px', fontWeight: 600, marginBottom: 6 }}>{label}</div>
      <div style={{ fontSize: 26, fontWeight: 700, color: colorMap[color] || colorMap.accent,
        fontFamily: 'var(--font-mono)' }}>{value ?? '—'}</div>
    </div>
  )
}

/* ── Global keyframe ──────────────────────────────────────────── */
const styleEl = document.createElement('style')
styleEl.textContent = `@keyframes spin { to { transform: rotate(360deg); } }`
document.head.appendChild(styleEl)
