import {
  Box,
  Button,
  ContentLayout,
  Header,
  Icon,
  Link,
  SpaceBetween,
  Table,
} from '@cloudscape-design/components'
import type { TableProps } from '@cloudscape-design/components'
import { useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { useFavorites } from '../hooks/useFavorites'
import { useResourceFavorites, type ResourceFavorite } from '../hooks/useResourceFavorites'
import { useServices } from '../hooks/useServices'
import { lookupResourceIds } from '../lib/resourceLookup'
import { serviceIconName } from './serviceIcons'
import { useNotifications } from './notifications'
import type { NavSection } from './nav'

/** Starred services and resources, with deep links and remove actions. */
export function ResourceFavorites() {
  const navigate = useNavigate()
  const { data } = useServices()
  const services = data?.services ?? []
  const { favorites: serviceIds, toggle: toggleService } = useFavorites()
  const { favorites: resources, remove } = useResourceFavorites()
  const { notify } = useNotifications()

  // Prune favourites whose resource no longer exists. Runs once on mount; only
  // services whose current ids can be read are checked, others are left alone.
  const resourcesRef = useRef(resources)
  resourcesRef.current = resources
  const removeRef = useRef(remove)
  removeRef.current = remove
  const notifyRef = useRef(notify)
  notifyRef.current = notify

  useEffect(() => {
    let cancelled = false
    const favorites = resourcesRef.current
    const servicesToCheck = [...new Set(favorites.map((favorite) => favorite.service))]
    void Promise.all(
      servicesToCheck.map(async (service) => [service, await lookupResourceIds(service)] as const),
    ).then((entries) => {
      if (cancelled) return
      const idMaps = new Map(entries)
      const missing = favorites.filter((favorite) => {
        const idMap = idMaps.get(favorite.service)
        if (!idMap) return false
        const live = idMap.get(favorite.type ?? '')
        if (!live) return false
        return !live.has(favorite.id)
      })
      if (missing.length === 0) return
      missing.forEach((favorite) => removeRef.current(favorite.service, favorite.id))
      notifyRef.current({
        type: 'info',
        header: 'Favorites updated',
        content: `Removed ${missing.length} favorite${missing.length === 1 ? '' : 's'} that no longer exist${missing.length === 1 ? 's' : ''}.`,
      })
    })
    return () => {
      cancelled = true
    }
  }, [])

  const serviceLabel = (id: string) => services.find((service) => service.id === id)?.label ?? id
  const favoriteServices = serviceIds
    .map((id) => services.find((service) => service.id === id))
    .filter((service): service is NavSection => service != null)

  const resourceColumns: TableProps.ColumnDefinition<ResourceFavorite>[] = [
    { id: 'service', header: 'Service', cell: (f) => serviceLabel(f.service) },
    {
      id: 'label',
      header: 'Resource',
      cell: (f) => (
        <Link
          href={`/ui${f.href}`}
          onFollow={(event) => {
            event.preventDefault()
            navigate(f.href)
          }}
        >
          {f.label}
        </Link>
      ),
    },
    { id: 'type', header: 'Type', cell: (f) => f.type || '—' },
    {
      id: 'actions',
      header: '',
      cell: (f) => (
        <Button
          variant="icon"
          iconName="star-filled"
          ariaLabel={`Remove ${f.label} from favorites`}
          onClick={() => remove(f.service, f.id)}
        />
      ),
    },
  ]

  return (
    <ContentLayout
      header={
        <Header variant="h1" description="Starred services and resources">
          Favorites
        </Header>
      }
    >
      <SpaceBetween size="l">
        <Table
          items={favoriteServices}
          columnDefinitions={[
            {
              id: 'label',
              header: 'Service',
              cell: (service) => (
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: '0.5rem' }}>
                  <Icon name={serviceIconName(service.id)} />
                  <Link
                    href={`/ui${service.rootPath}`}
                    onFollow={(event) => {
                      event.preventDefault()
                      navigate(service.rootPath)
                    }}
                  >
                    {service.label}
                  </Link>
                </span>
              ),
            },
            { id: 'category', header: 'Category', cell: (service) => service.category },
            {
              id: 'actions',
              header: '',
              cell: (service) => (
                <Button
                  variant="icon"
                  iconName="star-filled"
                  ariaLabel={`Remove ${service.label} from favorites`}
                  onClick={() => toggleService(service.id)}
                />
              ),
            },
          ]}
          trackBy={(service) => service.id}
          header={<Header counter={`(${favoriteServices.length})`}>Favorite services</Header>}
          empty={<Box color="inherit">Star a service from Console Home.</Box>}
        />

        <Table
          items={resources}
          columnDefinitions={resourceColumns}
          trackBy={(f) => `${f.service}:${f.id}`}
          header={<Header counter={`(${resources.length})`}>Favorite resources</Header>}
          empty={<Box color="inherit">Star a resource from any list or detail page.</Box>}
        />
      </SpaceBetween>
    </ContentLayout>
  )
}
