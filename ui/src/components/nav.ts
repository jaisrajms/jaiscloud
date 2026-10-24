import type { ServiceDescriptor } from '../api/services'

export type NavSection = ServiceDescriptor
export type NavChild = ServiceDescriptor['children'][number]

export interface ServiceGroup {
  category: string
  services: NavSection[]
}

/** Group services by category, preserving the server's category order. */
export function groupByCategory(services: NavSection[]): ServiceGroup[] {
  const groups: ServiceGroup[] = []
  for (const service of services) {
    let group = groups.find((g) => g.category === service.category)
    if (!group) {
      group = { category: service.category, services: [] }
      groups.push(group)
    }
    group.services.push(service)
  }
  return groups
}

/** Find the service that owns a router pathname. */
export function serviceForPath(
  services: NavSection[],
  pathname: string,
): NavSection | undefined {
  return services.find(
    (service) =>
      pathname === service.rootPath ||
      pathname.startsWith(`${service.rootPath}/`) ||
      service.children.some(
        (child) => pathname === child.path || pathname.startsWith(`${child.path}/`),
      ),
  )
}

export interface Crumb {
  text: string
  href: string
}

/**
 * Build the breadcrumb trail for a router pathname: the console root, then the
 * owning service (any cloud) or the Admin section. `href` maps a router path to
 * a full link.
 */
export function buildBreadcrumbItems(
  services: NavSection[],
  pathname: string,
  href: (path: string) => string,
): Crumb[] {
  const items: Crumb[] = [{ text: 'JaisCloud', href: href('/') }]
  const parts = pathname.split('/').filter(Boolean)
  if (parts[0] === 'admin') {
    items.push({ text: 'Admin', href: href('/admin') })
    return items
  }
  const service = serviceForPath(services, pathname)
  if (service) items.push({ text: service.label, href: href(service.rootPath) })
  return items
}
