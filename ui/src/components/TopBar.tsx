import { useAccount, useAccounts } from '../context/AccountContext'
import { useMeta } from '../hooks/useMeta'
import { useEventStream } from '../hooks/useEventStream'

interface Props {
  sidebarOpen: boolean
  onToggleSidebar: () => void
}

export function TopBar({ sidebarOpen, onToggleSidebar }: Props) {
  const { data: meta } = useMeta()
  const { accountId, setAccountId } = useAccount()
  const { data: accountsData, refetch: refetchAccounts, isFetching: accountsFetching } = useAccounts()
  const { connected } = useEventStream()

  const accounts = accountsData?.accounts ?? (accountId ? [accountId] : [])

  return (
    <header style={headerStyle}>
      <button
        onClick={onToggleSidebar}
        title={sidebarOpen ? 'Collapse sidebar' : 'Expand sidebar'}
        style={toggleBtnStyle}
      >
        {sidebarOpen ? '◀' : '▶'}
      </button>

      <strong style={{ fontSize: '1rem', letterSpacing: '0.01em', color: '#fff' }}>
        JaisCloud
      </strong>

      <span style={badgeStyle}>
        {meta?.cloud?.toUpperCase() ?? 'AWS'}
      </span>

      <span style={{ ...badgeStyle, background: 'rgba(255,255,255,0.08)' }}>
        {meta?.region ?? '—'}
      </span>

      <div style={{ marginLeft: '0.5rem', display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
        <span style={{ fontSize: '0.78em', color: 'rgba(255,255,255,0.5)' }}>Account</span>
        <select
          value={accountId}
          onChange={(e) => setAccountId(e.target.value)}
          style={accountSelectStyle}
        >
          {accounts.map((a) => (
            <option key={a} value={a}>{a}</option>
          ))}
        </select>
        <button
          onClick={() => void refetchAccounts()}
          disabled={accountsFetching}
          title="Refresh account list"
          style={{
            background: 'none', border: 'none', color: 'rgba(255,255,255,0.5)',
            cursor: 'pointer', fontSize: '0.9rem', padding: '0 0.2rem', lineHeight: 1,
            opacity: accountsFetching ? 0.4 : 1,
            transition: 'color 0.15s',
          }}
          onMouseEnter={(e) => (e.currentTarget.style.color = '#fff')}
          onMouseLeave={(e) => (e.currentTarget.style.color = 'rgba(255,255,255,0.5)')}
        >
          {accountsFetching ? '…' : '↺'}
        </button>
      </div>

      <span style={{ marginLeft: 'auto', fontSize: '0.75em', color: connected ? '#6ee7b7' : 'rgba(255,255,255,0.35)' }}>
        {connected ? '● live' : '○ polling'}
      </span>

      <span style={{ fontSize: '0.7em', color: 'rgba(255,255,255,0.3)', marginLeft: '1rem' }}>
        {meta?.version ? `v${meta.version}` : ''} · {meta?.mode ?? ''}
      </span>
    </header>
  )
}

const headerStyle: React.CSSProperties = {
  background: '#232f3e',
  color: '#fff',
  padding: '0 1.25rem',
  display: 'flex',
  alignItems: 'center',
  gap: '0.75rem',
  height: 48,
  flexShrink: 0,
  zIndex: 100,
  boxShadow: '0 1px 4px rgba(0,0,0,0.3)',
}

const toggleBtnStyle: React.CSSProperties = {
  background: 'none',
  border: 'none',
  color: 'rgba(255,255,255,0.65)',
  cursor: 'pointer',
  fontSize: '0.85rem',
  padding: '0.25rem 0.4rem',
  borderRadius: 4,
  lineHeight: 1,
  flexShrink: 0,
}

const badgeStyle: React.CSSProperties = {
  background: 'rgba(255,255,255,0.12)',
  color: 'rgba(255,255,255,0.7)',
  fontSize: '0.75em',
  padding: '0.2em 0.55em',
  borderRadius: 4,
  fontFamily: 'monospace',
}

const accountSelectStyle: React.CSSProperties = {
  background: 'rgba(255,255,255,0.1)',
  color: '#fff',
  border: '1px solid rgba(255,255,255,0.2)',
  borderRadius: 4,
  padding: '0.25rem 0.5rem',
  fontSize: '0.82em',
  fontFamily: 'monospace',
  cursor: 'pointer',
  outline: 'none',
}
