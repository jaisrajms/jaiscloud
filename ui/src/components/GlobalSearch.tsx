import { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Autosuggest } from '@cloudscape-design/components'
import type { AutosuggestProps } from '@cloudscape-design/components'
import { useFavorites } from '../hooks/useFavorites'
import { useServices } from '../hooks/useServices'
import { groupByCategory, type NavSection } from './nav'

function recentIds(): string[] {
  try {
    return JSON.parse(localStorage.getItem('jaiscloud-recent') ?? '[]') as string[]
  } catch {
    return []
  }
}

function toOption(service: NavSection): AutosuggestProps.Option {
  return { value: service.id, label: service.label, description: service.category }
}

function buildOptions(services: NavSection[], favorites: string[], recent: string[]): AutosuggestProps.Options {
  const byId = new Map(services.map((service) => [service.id, service]))
  const pick = (ids: string[]) =>
    ids.map((id) => byId.get(id)).filter((service): service is NavSection => service != null)

  const groups: AutosuggestProps.OptionGroup[] = []
  const favoriteServices = pick(favorites)
  if (favoriteServices.length > 0) {
    groups.push({ label: 'Favorites', options: favoriteServices.map(toOption) })
  }
  const recentServices = pick(recent)
  if (recentServices.length > 0) {
    groups.push({ label: 'Recent', options: recentServices.map(toOption) })
  }
  for (const group of groupByCategory(services)) {
    groups.push({ label: group.category, options: group.services.map(toOption) })
  }
  return groups
}

/**
 * Client-side (Phase 1) global search over the service catalog. Matches service
 * labels/categories, grouped like the sidebar; selecting a result navigates to
 * the service root. Cmd/Ctrl+K focuses the field.
 */
export function GlobalSearch() {
  const navigate = useNavigate()
  const ref = useRef<AutosuggestProps.Ref>(null)
  const { data } = useServices()
  const { favorites } = useFavorites()
  const [value, setValue] = useState('')

  const services = useMemo(() => data?.services ?? [], [data])
  const options = useMemo(
    () => buildOptions(services, favorites, recentIds()),
    [services, favorites],
  )

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        ref.current?.focus()
        ref.current?.select()
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])

  return (
    <Autosuggest
      ref={ref}
      value={value}
      onChange={({ detail }) => setValue(detail.value)}
      onSelect={({ detail }) => {
        const service = services.find((item) => item.id === detail.value)
        if (service) {
          setValue('')
          ref.current?.select()
          navigate(service.rootPath)
        }
      }}
      options={options}
      statusType="finished"
      placeholder="Search services (⌘K)"
      ariaLabel="Search services"
      empty="No services match"
    />
  )
}
