import {
  Box,
  Button,
  Cards,
  Container,
  ContentLayout,
  Header,
  Link,
  SpaceBetween,
} from '@cloudscape-design/components'
import { useNavigate } from 'react-router-dom'
import { useFavorites } from '../hooks/useFavorites'
import { useServices } from '../hooks/useServices'
import { groupByCategory, type NavSection } from './nav'
import { ServiceTierBadge } from './ServiceTierBadge'

function recentIds(): string[] {
  try {
    return JSON.parse(localStorage.getItem('jaiscloud-recent') ?? '[]') as string[]
  } catch {
    return []
  }
}

/**
 * Console Home landing page: favourites and recently visited services plus all
 * supported services grouped by AWS category. The service list is provided by
 * the backend, so only services this build supports are shown.
 */
export function ConsoleHome() {
  const navigate = useNavigate()
  const { data } = useServices()
  const services = data?.services ?? []
  const { isFavorite, toggle } = useFavorites()

  const recent = recentIds()
    .map((id) => services.find((service) => service.id === id))
    .filter((service): service is NavSection => service != null)

  const favorites = services.filter((service) => isFavorite(service.id))
  const groups = groupByCategory(services)

  const renderLink = (service: NavSection) => (
    <Link
      href={`/ui${service.rootPath}`}
      onFollow={(event) => {
        event.preventDefault()
        navigate(service.rootPath)
      }}
    >
      {service.label}
    </Link>
  )

  const renderStar = (service: NavSection) => {
    const favorited = isFavorite(service.id)
    return (
      <Button
        variant="icon"
        iconName={favorited ? 'star-filled' : 'star'}
        ariaLabel={favorited ? `Remove ${service.label} from favorites` : `Add ${service.label} to favorites`}
        onClick={() => toggle(service.id)}
      />
    )
  }

  const renderService = (service: NavSection) => (
    <span
      key={service.id}
      style={{ display: 'inline-flex', alignItems: 'center', gap: '0.5rem' }}
    >
      {renderLink(service)}
      <ServiceTierBadge service={service} />
      {renderStar(service)}
    </span>
  )

  return (
    <ContentLayout
      header={
        <Header variant="h1" description="JaisCloud local AWS emulator">
          Console Home
        </Header>
      }
    >
      <SpaceBetween size="l">
        {favorites.length > 0 && (
          <Container
            key="favorites"
            header={<Header variant="h2">Favorites</Header>}
          >
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
                gap: '0.75rem',
              }}
            >
              {favorites.map(renderService)}
            </div>
          </Container>
        )}

        {recent.length > 0 && (
          <Cards
            items={recent}
            cardDefinition={{
              header: (item) => (
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: '0.5rem' }}>
                  {renderLink(item)}
                  <ServiceTierBadge service={item} />
                  {renderStar(item)}
                </span>
              ),
              sections: [
                {
                  id: 'category',
                  header: 'Category',
                  content: (item) => item.category,
                },
              ],
            }}
            cardsPerRow={[
              { cards: 1 },
              { minWidth: 300, cards: 2 },
              { minWidth: 600, cards: 3 },
              { minWidth: 900, cards: 4 },
            ]}
            header={<Header variant="h2">Recently visited</Header>}
            empty={<Box color="inherit">Services you open appear here.</Box>}
          />
        )}

        {groups.map((group) => (
          <Container key={group.category} header={<Header variant="h2">{group.category}</Header>}>
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
                gap: '0.75rem',
              }}
            >
              {group.services.map(renderService)}
            </div>
          </Container>
        ))}
      </SpaceBetween>
    </ContentLayout>
  )
}
