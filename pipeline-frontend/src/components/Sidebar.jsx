import React from 'react'
import {
  LayoutDashboard, Search, Zap, Database,
  Languages, Layers, Store, FileSpreadsheet
} from 'lucide-react'

const NAV = [
  { id: 'dashboard',  label: 'Dashboard',        icon: LayoutDashboard },
  { id: 'search',     label: 'Search Scraper',   icon: Search          },
  { id: 'generate',   label: 'Generate Products',icon: Zap             },
  { id: 'variants',   label: 'Variants',          icon: Layers          },
  { id: 'translation',label: 'Translation',       icon: Languages       },
  { id: 'merchant',   label: 'Merchant Bulk',     icon: Store           },
  { id: 'database',   label: 'Database Viewer',   icon: Database        },
  { id: 'export',     label: 'Export & Tools',    icon: FileSpreadsheet },
]

export default function Sidebar({ active, onChange }) {
  return (
    <aside style={{
      width: 220, background: 'var(--bg-card)', borderRight: '1px solid var(--border)',
      display: 'flex', flexDirection: 'column', flexShrink: 0, height: '100vh',
      position: 'sticky', top: 0,
    }}>
      {/* Logo */}
      <div style={{ padding: '20px 16px 16px', borderBottom: '1px solid var(--border)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <div style={{ width: 32, height: 32, background: 'var(--accent)', borderRadius: 8,
            display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 16 }}>
            ⚙️
          </div>
          <div>
            <div style={{ fontSize: 13, fontWeight: 700, color: 'var(--text)' }}>Octopia</div>
            <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Pipeline v3.4</div>
          </div>
        </div>
      </div>

      {/* Nav */}
      <nav style={{ flex: 1, padding: '10px 8px', overflowY: 'auto' }}>
        {NAV.map(item => {
          const Icon = item.icon
          const isActive = active === item.id
          return (
            <button key={item.id} onClick={() => onChange(item.id)}
              style={{
                width: '100%', display: 'flex', alignItems: 'center', gap: 10,
                padding: '8px 10px', borderRadius: 'var(--radius)', border: 'none',
                background: isActive ? 'var(--accent-dim)' : 'transparent',
                color: isActive ? 'var(--accent)' : 'var(--text-muted)',
                fontSize: 13, fontWeight: isActive ? 600 : 400,
                cursor: 'pointer', marginBottom: 2, textAlign: 'left',
                transition: 'background .15s, color .15s',
              }}
              onMouseEnter={e => { if (!isActive) { e.currentTarget.style.background = 'var(--bg-input)'; e.currentTarget.style.color = 'var(--text)' } }}
              onMouseLeave={e => { if (!isActive) { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--text-muted)' } }}
            >
              <Icon size={15} />
              {item.label}
            </button>
          )
        })}
      </nav>

      {/* Footer */}
      <div style={{ padding: '12px 16px', borderTop: '1px solid var(--border)', fontSize: 11, color: 'var(--text-dim)' }}>
        AliExpress → Octopia Pipeline
      </div>
    </aside>
  )
}
