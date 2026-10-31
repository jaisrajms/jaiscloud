import type { ServiceChild, ServiceDescriptor } from '../../api/services'
import { groupByCategory, serviceForPath } from '../../components/nav'

/** Minimal Storage surface so the recent-list helpers are testable in Node. */
export interface StorageLike {
  getItem(key: string): string | null
  setItem(key: string, value: string): void
}

/** Recent services, shared with the Cloudscape shell so both consoles agree. */
export const RECENT_SERVICES_KEY = 'jaiscloud-recent'
/** Recent GCP projects for the "Select a project" dialog. */
export const RECENT_PROJECTS_KEY = 'jaiscloud-recent-projects'

function defaultStorage(): StorageLike | undefined {
  if (typeof window === 'undefined') return undefined
  try {
    return window.localStorage
  } catch {
    return undefined
  }
}

/** Read a JSON string-array from storage, tolerating malformed or absent data. */
export function readRecent(key: string, storage?: StorageLike): string[] {
  const store = storage ?? defaultStorage()
  if (!store) return []
  try {
    const raw: unknown = JSON.parse(store.getItem(key) ?? '[]')
    return Array.isArray(raw) ? raw.filter((id): id is string => typeof id === 'string') : []
  } catch {
    return []
  }
}

/** Push an id to the front of a recent list, de-duplicated and capped. */
export function pushRecent(
  key: string,
  id: string,
  storage?: StorageLike,
  limit = 6,
): string[] {
  const store = storage ?? defaultStorage()
  if (!id) return readRecent(key, store)
  const next = [id, ...readRecent(key, store).filter((item) => item !== id)].slice(0, limit)
  if (store) {
    try {
      store.setItem(key, JSON.stringify(next))
    } catch {
      /* ignore storage errors */
    }
  }
  return next
}

export interface SearchOption {
  id: string
  label: string
  description: string
  path: string
}

export interface SearchGroup {
  label: string
  options: SearchOption[]
}

/** A service plus the presentation state the navigation menu needs. */
export interface NavEntry {
  id: string
  label: string
  category: string
  path: string
  children: ServiceChild[]
  pinned: boolean
}

export interface NavGroup {
  label: string
  kind: 'pinned' | 'recent' | 'category'
  entries: NavEntry[]
}

export type NavGroupKind = NavGroup['kind']

function matchesQuery(service: ServiceDescriptor, query: string): boolean {
  const q = query.trim().toLowerCase()
  if (!q) return true
  return (
    service.label.toLowerCase().includes(q) || service.category.toLowerCase().includes(q)
  )
}

function toEntry(service: ServiceDescriptor, favorites: Set<string>): NavEntry {
  return {
    id: service.id,
    label: service.label,
    category: service.category,
    path: service.rootPath,
    children: service.children,
    pinned: favorites.has(service.id),
  }
}

function pickEntries(
  services: ServiceDescriptor[],
  ids: string[],
  matchingIds: Set<string>,
  favorites: Set<string>,
): NavEntry[] {
  const byId = new Map(services.map((service) => [service.id, service]))
  return ids
    .map((id) => byId.get(id))
    .filter((service): service is ServiceDescriptor => service != null)
    .filter((service) => matchingIds.has(service.id))
    .map((service) => toEntry(service, favorites))
}

/**
 * Build the navigation-menu groups: pinned favorites and recents first, then
 * every matching service grouped by category. A service's own `children` drive
 * its collapsible sub-nav. The global-search options are derived from this so
 * the two surfaces never diverge.
 */
export function buildNavGroups(
  services: ServiceDescriptor[],
  favorites: string[],
  recent: string[],
  query: string,
): NavGroup[] {
  const matching = services.filter((service) => matchesQuery(service, query))
  const matchingIds = new Set(matching.map((service) => service.id))
  const favoriteSet = new Set(favorites)
  const groups: NavGroup[] = []

  const pinned = pickEntries(services, favorites, matchingIds, favoriteSet)
  if (pinned.length > 0) groups.push({ label: 'Favorites', kind: 'pinned', entries: pinned })

  const recentEntries = pickEntries(services, recent, matchingIds, favoriteSet)
  if (recentEntries.length > 0) groups.push({ label: 'Recent', kind: 'recent', entries: recentEntries })

  for (const group of groupByCategory(matching)) {
    groups.push({
      label: group.category,
      kind: 'category',
      entries: group.services.map((service) => toEntry(service, favoriteSet)),
    })
  }
  return groups
}

/**
 * Build the grouped options for the GCP global search: matching favorites and
 * recents first, then every matching service grouped by category.
 */
export function buildSearchGroups(
  services: ServiceDescriptor[],
  favorites: string[],
  recent: string[],
  query: string,
): SearchGroup[] {
  return buildNavGroups(services, favorites, recent, query).map((group) => ({
    label: group.label,
    options: group.entries.map((entry) => ({
      id: entry.id,
      label: entry.label,
      description: entry.category,
      path: entry.path,
    })),
  }))
}

/**
 * Slot resource results into the service groups: after the Favorites and Recent
 * shortcuts, before the category groups. An empty resource list leaves the
 * groups untouched, so a search that matches no resource looks exactly as it
 * did before resource indexing existed.
 */
export function withResourceGroup(
  groups: SearchGroup[],
  resources: SearchOption[],
): SearchGroup[] {
  if (resources.length === 0) return groups
  const shortcut = (group: SearchGroup) => group.label === 'Favorites' || group.label === 'Recent'
  return [
    ...groups.filter(shortcut),
    { label: 'Resources', options: resources },
    ...groups.filter((group) => !shortcut(group)),
  ]
}

export interface Crumb {
  label: string
  to?: string
}

/** Breadcrumb trail for a GCP router pathname (root, then service or Admin). */
export function breadcrumbsFor(services: ServiceDescriptor[], pathname: string): Crumb[] {
  const root: Crumb = { label: 'JaisCloud', to: '/gcp' }
  const rest = pathname.replace(/^\/gcp/, '').split('/').filter(Boolean)
  if (rest[0] === 'admin') return [root, { label: 'Admin', to: '/gcp/admin' }]
  const service = serviceForPath(services, pathname)
  if (service) return [root, { label: service.label, to: service.rootPath }]
  return [root]
}

/** `document.title` for a GCP router pathname. */
export function pageTitleFor(services: ServiceDescriptor[], pathname: string): string {
  const rest = pathname.replace(/^\/gcp/, '').split('/').filter(Boolean)
  if (rest[0] === 'admin') return 'Admin · JaisCloud'
  const service = serviceForPath(services, pathname)
  return service ? `${service.label} · JaisCloud` : 'JaisCloud Console'
}

/** A project row as the picker and home card render it. */
export interface ProjectRowInfo {
  id: string
  kind: 'configured' | 'registered' | 'observed' | 'unknown'
  firstSeen?: string
  lastSeen?: string
  services?: string[]
  requests?: number
}

export interface ProjectGroups {
  /** Observed on the wire but never registered, newest activity first. */
  active: ProjectRowInfo[]
  /** Recently used ids, including ones no longer in the server list. */
  recent: ProjectRowInfo[]
  /** Configured and registered projects not already shown under Recent. */
  all: ProjectRowInfo[]
}

function lastSeenMs(row: ProjectRowInfo): number {
  const t = row.lastSeen ? Date.parse(row.lastSeen) : NaN
  return Number.isNaN(t) ? -Infinity : t
}

/**
 * Split project rows into the picker's three groups. `accounts` is the
 * configured ∪ created list the console already had and acts as a fallback when
 * discovery is unavailable; `recent` is the localStorage list, which keeps an
 * id reachable after its observed entry is evicted (the client-side backstop).
 */
export function groupProjects(
  accounts: string[],
  projects: ProjectRowInfo[],
  recent: string[],
  query = '',
): ProjectGroups {
  const q = query.trim().toLowerCase()
  const matches = (id: string) => !q || id.toLowerCase().includes(q)

  const observed = projects.filter((project) => project.kind === 'observed')
  const known = projects.filter((project) => project.kind !== 'observed')
  const knownIds = new Set(known.map((project) => project.id))
  const observedIds = new Set(observed.map((project) => project.id))
  for (const id of accounts) {
    if (!knownIds.has(id) && !observedIds.has(id)) {
      known.push({ id, kind: 'configured' })
      knownIds.add(id)
    }
  }
  known.sort((a, b) => a.id.localeCompare(b.id))

  const active = observed
    .filter((project) => matches(project.id))
    .sort((a, b) => lastSeenMs(b) - lastSeenMs(a) || a.id.localeCompare(b.id))

  const byId = new Map<string, ProjectRowInfo>()
  for (const project of [...observed, ...known]) byId.set(project.id, project)

  const recentRows: ProjectRowInfo[] = []
  const recentIds = new Set<string>()
  for (const id of recent) {
    if (!id || recentIds.has(id) || observedIds.has(id) || !matches(id)) continue
    recentIds.add(id)
    recentRows.push(byId.get(id) ?? { id, kind: 'unknown' })
  }

  const all = known.filter((project) => !recentIds.has(project.id) && matches(project.id))

  return { active, recent: recentRows, all }
}

/** Up to `limit` service ids for a project's chips. */
export function projectServiceChips(services: string[] | undefined, limit = 3): string[] {
  return (services ?? []).slice(0, limit)
}

/** Relative last-activity label ("18s ago"), empty for an absent timestamp. */
export function relativeProjectAge(iso: string | undefined, now = Date.now()): string {
  if (!iso) return ''
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return ''
  const d = Math.max(0, now - t)
  if (d < 60_000) return `${Math.floor(d / 1000)}s ago`
  if (d < 3_600_000) return `${Math.floor(d / 60_000)}m ago`
  if (d < 86_400_000) return `${Math.floor(d / 3_600_000)}h ago`
  return `${Math.floor(d / 86_400_000)}d ago`
}
