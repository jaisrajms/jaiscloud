import { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Autocomplete, Box, InputAdornment, TextField, Typography } from '@mui/material'
import SearchIcon from '@mui/icons-material/Search'
import { useFavorites } from '../../hooks/useFavorites'
import { useServices } from '../../hooks/useServices'
import {
  filterResources,
  RESOURCE_SEARCH_MIN_LENGTH,
  type SearchableResource,
} from '../common/resourceSearch'
import {
  buildSearchGroups,
  withResourceGroup,
  type SearchOption,
} from './navModel'
import { rememberRecentService, useRecentServices } from './recentServices'
import { useResourceIndex } from './useResourceIndex'

/** Search result caption for a resource: type, service, then its location or
 * parent when the loader supplied one. */
function resourceDescription(
  resource: SearchableResource,
  serviceLabels: Map<string, string>,
): string {
  const service = serviceLabels.get(resource.service) ?? resource.service
  const detail = resource.detail ? ` · ${resource.detail}` : ''
  return `${resource.type} · ${service}${detail}`
}

function isTypingTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false
  const tag = target.tagName.toLowerCase()
  return tag === 'input' || tag === 'textarea' || tag === 'select' || target.isContentEditable
}

/**
 * GCP console global search: the service catalog plus the project's resources.
 * Favorites and recents lead, then matching resources, then every service
 * grouped by category. `/` (or Cmd/Ctrl+K) focuses it.
 */
export function GcpGlobalSearch() {
  const navigate = useNavigate()
  const inputRef = useRef<HTMLInputElement>(null)
  const { data } = useServices()
  const { favorites } = useFavorites()
  const [query, setQuery] = useState('')
  const recent = useRecentServices()
  const resourceIndex = useResourceIndex(query.trim().length >= RESOURCE_SEARCH_MIN_LENGTH)

  const services = useMemo(() => data?.services ?? [], [data])
  const serviceGroups = useMemo(
    () => buildSearchGroups(services, favorites, recent, query),
    [services, favorites, recent, query],
  )
  const resourceOptions = useMemo(() => {
    const labels = new Map(services.map((service) => [service.id, service.label]))
    return filterResources(resourceIndex.data ?? [], query).map((resource) => ({
      id: resource.id,
      label: resource.label,
      description: resourceDescription(resource, labels),
      path: resource.path,
    }))
  }, [resourceIndex.data, query, services])
  const groups = useMemo(
    () => withResourceGroup(serviceGroups, resourceOptions),
    [serviceGroups, resourceOptions],
  )

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.defaultPrevented || isTypingTarget(event.target)) return
      const slash = event.key === '/' && !event.metaKey && !event.ctrlKey && !event.altKey
      const chord = (event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k'
      if (slash || chord) {
        event.preventDefault()
        inputRef.current?.focus()
      }
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [])

  return (
    <Autocomplete<SearchOption, false, false, false>
      options={groups.flatMap((group) => group.options)}
      groupBy={(option) => {
        const group = groups.find((candidate) => candidate.options.includes(option))
        return group?.label ?? ''
      }}
      filterOptions={(options) => options}
      getOptionLabel={(option) => option.label}
      // Option ids are stable and unique (resource ids carry their scope), so a
      // topic and a subscription sharing a display name keep distinct rows.
      getOptionKey={(option) => option.id}
      inputValue={query}
      onInputChange={(_, value, reason) => {
        if (reason !== 'reset') setQuery(value)
      }}
      onChange={(_, option) => {
        if (!option) return
        // Only services belong in the recents list; resources are transient.
        if (services.some((service) => service.id === option.id)) {
          rememberRecentService(option.id)
        }
        setQuery('')
        navigate(option.path)
      }}
      size="small"
      loading={resourceIndex.isFetching}
      loadingText="Searching resources…"
      noOptionsText="No matching services or resources"
      sx={{ width: '100%', maxWidth: 600 }}
      renderGroup={(params) => (
        <li key={params.key}>
          <Typography
            variant="overline"
            sx={{ display: 'block', px: 2, pt: 1, color: 'text.secondary' }}
          >
            {params.group}
          </Typography>
          <ul style={{ padding: 0 }}>{params.children}</ul>
        </li>
      )}
      renderOption={(props, option) => (
        <Box component="li" {...props}>
          <Box sx={{ minWidth: 0 }}>
            <Typography variant="body2" noWrap>
              {option.label}
            </Typography>
            <Typography variant="caption" color="text.secondary" noWrap>
              {option.description}
            </Typography>
          </Box>
        </Box>
      )}
      renderInput={(params) => (
        <TextField
          {...params}
          size="small"
          inputRef={inputRef}
          placeholder="Search services and resources"
          slotProps={{
            ...params.slotProps,
            input: {
              ...params.slotProps.input,
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon fontSize="small" sx={{ color: 'text.secondary' }} />
                </InputAdornment>
              ),
              // The AppBar sets color: inherit, so an explicit theme-aware
              // background + text colour is required for the pill to be legible
              // in both appearances (a hardcoded light pill rendered near-white
              // text on light gray in dark mode).
              sx: (theme) => ({
                backgroundColor:
                  theme.palette.mode === 'dark' ? 'rgba(255, 255, 255, 0.08)' : '#f1f3f4',
                borderRadius: 1,
                color: theme.palette.text.primary,
                '& input': { color: theme.palette.text.primary, fontSize: 14 },
                '& input::placeholder': { color: theme.palette.text.secondary, opacity: 1 },
                '& fieldset': { border: 'none' },
                '&:hover': {
                  backgroundColor:
                    theme.palette.mode === 'dark' ? 'rgba(255, 255, 255, 0.14)' : '#e8eaed',
                },
              }),
            },
            htmlInput: {
              ...params.slotProps.htmlInput,
              'aria-label': 'Search services and resources',
            },
          }}
        />
      )}
    />
  )
}
