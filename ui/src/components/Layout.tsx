import { useState } from 'react'
import { TopBar } from './TopBar'
import { Sidebar } from './Sidebar'
import { AccountProvider } from '../context/AccountContext'

interface Props {
  children: React.ReactNode
}

function Shell({ children }: Props) {
  const [sidebarOpen, setSidebarOpen] = useState(true)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', overflow: 'hidden' }}>
      <TopBar
        sidebarOpen={sidebarOpen}
        onToggleSidebar={() => setSidebarOpen((o) => !o)}
      />
      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        <Sidebar open={sidebarOpen} />
        <main style={mainStyle}>
          {children}
        </main>
      </div>
    </div>
  )
}

export function Layout({ children }: Props) {
  return (
    <AccountProvider>
      <Shell>{children}</Shell>
    </AccountProvider>
  )
}

const mainStyle: React.CSSProperties = {
  flex: 1,
  overflowY: 'auto',
  padding: '1.5rem 2rem',
  background: '#f8f9fa',
  boxSizing: 'border-box',
}
