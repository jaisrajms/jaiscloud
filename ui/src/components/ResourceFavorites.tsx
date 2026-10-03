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
import { useNavigate } from 'react-router-dom'
import { useFavorites } from '../hooks/useFavorites'
import { useResourceFavorites, type ResourceFavorite } from '../hooks/useResourceFavorites'
import { useServices } from '../hooks/useServices'
import { serviceIconName } from './serviceIcons'
import type { NavSection } from './nav'

/** Starred services and resources, with deep links and remove actions. */
export function ResourceFavorites() {
  const navigate = useNavigate()
  const { data } = useServices()
  const services = data?.services ?? []
  const { favorites: serviceIds, toggle: toggleService } = useFavorites()
  const { favorites: resources, remove } = useResourceFavorites()

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
