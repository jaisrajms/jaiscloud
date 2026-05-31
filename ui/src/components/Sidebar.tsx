import { NavLink, useLocation } from 'react-router-dom'

interface NavChild {
  label: string
  path: string
}

interface NavSection {
  id: string
  label: string
  basePath: string
  rootPath: string  // navigate here when section header is clicked
  children: NavChild[]
}

const navTree: NavSection[] = [
  {
    id: 'sqs',
    label: 'SQS',
    basePath: '/aws/sqs',
    rootPath: '/aws/sqs',
    children: [
      { label: 'Queues', path: '/aws/sqs' },
    ],
  },
  {
    id: 'lambda',
    label: 'Lambda',
    basePath: '/aws/lambda',
    rootPath: '/aws/lambda',
    children: [
      { label: 'Functions', path: '/aws/lambda' },
    ],
  },
  {
    id: 'logs',
    label: 'CloudWatch Logs',
    basePath: '/aws/logs',
    rootPath: '/aws/logs/groups',
    children: [
      { label: 'Log Groups', path: '/aws/logs/groups' },
    ],
  },
]

interface Props {
  open: boolean
}

export function Sidebar({ open }: Props) {
  const { pathname } = useLocation()

  return (
    <aside style={{
      ...sidebarStyle,
      width: open ? SIDEBAR_WIDTH : 0,
      minWidth: open ? SIDEBAR_WIDTH : 0,
      overflowY: open ? 'auto' : 'hidden',
      overflowX: 'hidden',
      position: 'relative',
      zIndex: 10,
    }}>
      <nav style={{ width: SIDEBAR_WIDTH, paddingTop: '0.5rem' }}>
        {navTree.map((section) => {
          const isActive = pathname.startsWith(section.basePath)
          return (
            <div key={section.id} style={{ marginBottom: '0.15rem' }}>
              {/* Section header — NavLink to the service root */}
              <NavLink
                to={section.rootPath}
                style={{
                  ...sectionHeaderStyle,
                  background: isActive ? 'rgba(232,118,0,0.12)' : 'none',
                  color: isActive ? '#e87600' : '#c9cdd4',
                  textDecoration: 'none',
                  display: 'flex',
                }}
              >
                <span style={{ fontSize: '0.6em', opacity: 0.7, width: 16, textAlign: 'center', flexShrink: 0, paddingTop: 1 }}>
                  {isActive ? '▼' : '▶'}
                </span>
                <span style={{ fontWeight: isActive ? 600 : 400, fontSize: '0.83em' }}>
                  {section.label}
                </span>
              </NavLink>

              {/* Children — shown when this service is active */}
              {isActive && (
                <div>
                  {section.children.map((child) => (
                    <NavLink
                      key={child.path}
                      to={child.path}
                      end
                      style={({ isActive: childActive }) => ({
                        ...childLinkStyle,
                        background: childActive ? 'rgba(9,114,211,0.12)' : 'none',
                        color: childActive ? '#0972d3' : '#8d9daa',
                        fontWeight: childActive ? 500 : 400,
                      })}
                    >
                      {child.label}
                    </NavLink>
                  ))}
                </div>
              )}
            </div>
          )
        })}
      </nav>
    </aside>
  )
}

const SIDEBAR_WIDTH = 210

const sidebarStyle: React.CSSProperties = {
  background: '#1b2530',
  borderRight: '1px solid #2d3748',
  flexShrink: 0,
  transition: 'width 0.18s ease, min-width 0.18s ease',
}

const sectionHeaderStyle: React.CSSProperties = {
  alignItems: 'center',
  gap: '0.5rem',
  padding: '0.55rem 1rem',
  cursor: 'pointer',
  userSelect: 'none',
}

const childLinkStyle: React.CSSProperties = {
  display: 'block',
  padding: '0.4rem 1rem 0.4rem 2.25rem',
  textDecoration: 'none',
  fontSize: '0.82em',
  transition: 'background 0.1s',
}
