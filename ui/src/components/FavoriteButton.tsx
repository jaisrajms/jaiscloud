import { Button } from '@cloudscape-design/components'
import { useResourceFavorites, type ResourceFavorite } from '../hooks/useResourceFavorites'

/**
 * Star toggle for a resource. Wraps in a stopPropagation span so it can live in
 * clickable table rows without triggering row navigation.
 */
export function FavoriteButton(favorite: ResourceFavorite) {
  const { isFavorite, toggle } = useResourceFavorites()
  const active = isFavorite(favorite.service, favorite.id)
  return (
    <span onClick={(event) => event.stopPropagation()}>
      <Button
        variant="icon"
        iconName={active ? 'star-filled' : 'star'}
        ariaLabel={
          active
            ? `Remove ${favorite.label} from favorites`
            : `Add ${favorite.label} to favorites`
        }
        onClick={() => toggle(favorite)}
      />
    </span>
  )
}
