import { useEffect, useMemo, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { applyDensity, applyMode, Density, Mode } from '@cloudscape-design/global-styles'
import {
  Alert,
  AppLayout,
  BreadcrumbGroup,
  Flashbar,
  HelpPanel,
  Icon,
  Input,
  Link,
  SideNavigation,
  SpaceBetween,
  TopNavigation,
} from '@cloudscape-design/components'
import type {
  SideNavigationProps,
  TopNavigationProps,
} from '@cloudscape-design/components'
import { AccountProvider, useAccount, useAccounts } from '../context/AccountContext'
import { useMeta } from '../hooks/useMeta'
import { useEventStream } from '../hooks/useEventStream'
import { useServices } from '../hooks/useServices'
import { useActiveTerminology } from '../hooks/useTerminology'
import { docsLink } from '../lib/cloudLinks'
import { buildBreadcrumbItems, groupByCategory, serviceForPath, type NavSection } from './nav'
import { NotificationsProvider, useNotifications } from './notifications'
import { ServiceTierBadge } from './ServiceTierBadge'
import { serviceIconName } from './serviceIcons'
import { GlobalSearch } from './GlobalSearch'
import { tierDescription, tierLabel } from '../lib/tier'

/** Router basename; links must include it so they also work without JS. */
const BASE = '/ui'

const href = (path: string) => `${BASE}${path}`

interface Props {
  children: React.ReactNode
}

type NavItem = SideNavigationProps.Link | SideNavigationProps.ExpandableLinkGroup

function serviceItem(service: NavSection, expand: boolean): NavItem {
  if (service.children.length > 1) {
    return {
      type: 'expandable-link-group',
      text: service.label,
      href: href(service.rootPath),
      icon: <Icon name={serviceIconName(service.id)} />,
      defaultExpanded: expand,
      items: service.children.map((child) => ({
        type: 'link',
        text: child.label,
        href: href(child.path),
      })),
    }
  }
  return {
    type: 'link',
    text: service.label,
    href: href(service.rootPath),
    icon: <Icon name={serviceIconName(service.id)} />,
    info: <ServiceTierBadge service={service} />,
  }
}

function useConsoleNav(services: NavSection[]) {
  const { pathname } = useLocation()
  const navigate = useNavigate()

  const onFollow = (event: { preventDefault: () => void; detail: { href: string } }) => {
    event.preventDefault()
    navigate(event.detail.href.replace(BASE, '') || '/')
  }

  const breadcrumbItems = useMemo(
    () => buildBreadcrumbItems(services, pathname, href),
    [pathname, services],
  )

  return { breadcrumbItems, onFollow }
}

function Shell({ children }: Props) {
  const [navOpen, setNavOpen] = useState(true)
  const [search, setSearch] = useState('')
  const [toolsOpen, setToolsOpen] = useState(false)
  const [mode, setMode] = useState<Mode>(() =>
    localStorage.getItem('jaiscloud-mode') === 'dark' ? Mode.Dark : Mode.Light,
  )
  const [density, setDensity] = useState<Density>(() =>
    localStorage.getItem('jaiscloud-density') === 'compact'
      ? Density.Compact
      : Density.Comfortable,
  )
  const navigate = useNavigate()
  const { pathname } = useLocation()
  const { data: meta } = useMeta()
  const { accountId, setAccountId } = useAccount()
  const { data: accountsData, refetch: refetchAccounts } = useAccounts()
  const { connected } = useEventStream()
  const { data: servicesData } = useServices()
  const services = useMemo(() => servicesData?.services ?? [], [servicesData])
  const currentService = serviceForPath(services, pathname)
  const { breadcrumbItems, onFollow } = useConsoleNav(services)
  const { items: notifications } = useNotifications()
  const terminology = useActiveTerminology(meta?.cloud ?? 'aws')
  const docs = docsLink(meta?.cloud)

  useEffect(() => {
    applyMode(mode)
    localStorage.setItem('jaiscloud-mode', mode)
  }, [mode])

  useEffect(() => {
    applyDensity(density)
    localStorage.setItem(
      'jaiscloud-density',
      density === Density.Compact ? 'compact' : 'comfortable',
    )
  }, [density])

  useEffect(() => {
    if (!currentService) return
    try {
      const ids: string[] = JSON.parse(localStorage.getItem('jaiscloud-recent') ?? '[]')
      const next = [currentService.id, ...ids.filter((id) => id !== currentService.id)].slice(0, 6)
      localStorage.setItem('jaiscloud-recent', JSON.stringify(next))
    } catch {
      /* ignore malformed storage */
    }
  }, [currentService])

  const accounts = useMemo(
    () => accountsData?.accounts ?? (accountId ? [accountId] : []),
    [accountsData, accountId],
  )

  const sideItems = useMemo<SideNavigationProps.Item[]>(() => {
    const query = search.trim().toLowerCase()
    const groups: SideNavigationProps.Item[] = groupByCategory(services)
      .map((group) => {
        const items = group.services
          .filter((service) => !query || service.label.toLowerCase().includes(query))
          .map((service) =>
            serviceItem(
              service,
              query.length > 0 || serviceForPath(services, pathname)?.id === service.id,
            ),
          )
        return { type: 'section-group', title: group.category, items } as SideNavigationProps.SectionGroup
      })
      .filter((group) => group.items.length > 0)

    const items: SideNavigationProps.Item[] = [
      { type: 'link', text: 'Console home', href: href('/'), icon: <Icon name="view-full" /> },
      { type: 'link', text: 'Favorites', href: href('/favorites'), icon: <Icon name="star" /> },
      { type: 'divider' },
      ...groups,
      { type: 'divider' },
      { type: 'link', text: 'Admin', href: href('/admin'), icon: <Icon name="security" /> },
    ]
    return items
  }, [search, pathname, services])

  const utilities = useMemo<TopNavigationProps.Utility[]>(() => {
    const items: TopNavigationProps.Utility[] = [
      {
        type: 'menu-dropdown',
        text: accountId || terminology['account-id-label'] || 'Account',
        iconName: 'user-profile',
        items: accounts.map((account) => ({ id: account, text: account })),
        onItemClick: (event) => {
          if (event.detail.id) setAccountId(event.detail.id)
        },
      },
      {
        type: 'button',
        iconName: 'refresh',
        ariaLabel: 'Refresh account list',
        onClick: () => void refetchAccounts(),
      },
      {
        type: 'button',
        text: connected ? 'Live' : 'Polling',
        iconName: connected ? 'status-positive' : 'status-pending',
        disableUtilityCollapse: true,
      },
      {
        type: 'menu-dropdown',
        text: mode === Mode.Dark ? 'Dark' : 'Light',
        iconName: 'light-dark',
        ariaLabel: 'Appearance',
        disableUtilityCollapse: true,
        items: [
          { id: 'light', text: 'Light' },
          { id: 'dark', text: 'Dark' },
        ],
        onItemClick: (event) => {
          setMode(event.detail.id === 'dark' ? Mode.Dark : Mode.Light)
        },
      },
      {
        type: 'menu-dropdown',
        text: density === Density.Compact ? 'Compact' : 'Comfortable',
        iconName: density === Density.Compact ? 'shrink' : 'expand',
        ariaLabel: 'Density',
        disableUtilityCollapse: true,
        items: [
          { id: 'comfortable', text: 'Comfortable' },
          { id: 'compact', text: 'Compact' },
        ],
        onItemClick: (event) => {
          setDensity(event.detail.id === 'compact' ? Density.Compact : Density.Comfortable)
        },
      },
      {
        type: 'button',
        text: 'Admin',
        iconName: 'settings',
        ariaLabel: 'Admin panel',
        disableUtilityCollapse: true,
        onClick: () => navigate('/admin'),
      },
    ]

    const version = [meta?.version ? `v${meta.version}` : '', meta?.mode ?? '']
      .filter(Boolean)
      .join(' · ')
    if (version) {
      items.push({ type: 'button', text: version, disableUtilityCollapse: true })
    }
    return items
  }, [
    meta,
    accountId,
    accounts,
    connected,
    refetchAccounts,
    setAccountId,
    mode,
    density,
    navigate,
    terminology,
  ])

  return (
    <>
      <div className="console-topnav">
        <TopNavigation
          identity={{
            href: href('/'),
            title: 'JaisCloud',
            onFollow: (event) => {
              event.preventDefault()
              navigate('/')
            },
          }}
          search={<GlobalSearch />}
          utilities={utilities}
        />
      </div>
      <AppLayout
        navigation={
          <div className="console-sidenav">
            <div className="console-sidenav__search">
              <Input
                type="search"
                value={search}
                onChange={({ detail }) => setSearch(detail.value)}
                placeholder="Find services"
                ariaLabel="Find services"
              />
            </div>
            <SideNavigation
              key={search}
              header={{ href: href('/'), text: 'All services' }}
              items={sideItems}
              onFollow={onFollow}
              activeHref={href(pathname)}
            />
          </div>
        }
        navigationOpen={navOpen}
        onNavigationChange={({ detail }) => setNavOpen(detail.open)}
        breadcrumbs={
          <BreadcrumbGroup items={breadcrumbItems} onFollow={onFollow} ariaLabel="Breadcrumbs" />
        }
        content={children}
        notifications={
          <SpaceBetween size="xs">
            {currentService && currentService.tier !== 'full' && (
              <Alert type="info" header={tierLabel(currentService)}>
                {tierDescription(currentService)}
              </Alert>
            )}
            <Flashbar items={notifications} />
          </SpaceBetween>
        }
        stickyNotifications
        tools={
          currentService ? (
            <HelpPanel header={<h2>{currentService.label}</h2>}>
              <SpaceBetween size="m">
                <p>
                  JaisCloud emulates {currentService.label}. Manage its resources in{' '}
                  {terminology['account-id-label'] ?? 'account'} {accountId || '—'} (
                  {meta?.region ?? '—'}).
                </p>
                <Link external href={docs.href}>
                  {docs.label}
                </Link>
              </SpaceBetween>
            </HelpPanel>
          ) : undefined
        }
        toolsOpen={toolsOpen}
        onToolsChange={({ detail }) => setToolsOpen(detail.open)}
        toolsHide={!currentService}
        contentType="default"
      />
    </>
  )
}

export function Layout({ children }: Props) {
  return (
    <AccountProvider>
      <NotificationsProvider>
        <Shell>{children}</Shell>
      </NotificationsProvider>
    </AccountProvider>
  )
}
