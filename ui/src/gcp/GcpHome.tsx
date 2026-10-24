import { useMemo, useState } from 'react'
import { useQueries } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Card,
  CardActionArea,
  CardContent,
  Chip,
  IconButton,
  Skeleton,
  Stack,
  Tooltip,
  Typography,
} from '@mui/material'
import StarIcon from '@mui/icons-material/Star'
import StarBorderIcon from '@mui/icons-material/StarBorder'
import SwapHorizIcon from '@mui/icons-material/SwapHoriz'
import { Link as RouterLink } from 'react-router-dom'
import { useAccount } from '../context/AccountContext'
import { useMeta } from '../hooks/useMeta'
import { useServices } from '../hooks/useServices'
import { useFavorites } from '../hooks/useFavorites'
import { cloudName } from '../lib/cloudNames'
import { tierLabel } from '../lib/tier'
import { GcpServiceIcon } from './icons/GcpServiceIcon'
import { serviceAccent } from './icons/serviceIcons'
import { ProjectPickerDialog } from './chrome/ProjectPicker'
import { buildNavGroups, type NavEntry } from './chrome/navModel'
import { rememberRecentService, useRecentServices } from './chrome/recentServices'
import { resourceCount, summarySourcesFor } from './home/resourceSummary'

const TILE_GRID = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))',
  gap: 2,
} as const

interface HomeTileProps {
  id: string
  title: string
  subtitle?: string
  to: string
  onVisit?: () => void
  badge?: React.ReactNode
  action?: React.ReactNode
}

/** A console-home tile: product glyph, label and an optional count/badge. */
function HomeTile({ id, title, subtitle, to, onVisit, badge, action }: HomeTileProps) {
  return (
    <Card variant="outlined" sx={{ position: 'relative' }}>
      <CardActionArea
        component={RouterLink}
        to={to}
        onClick={onVisit}
        sx={{ height: '100%' }}
      >
        <CardContent
          sx={{ display: 'flex', alignItems: 'center', gap: 1.5, pr: action ? 6 : undefined }}
        >
          <Box
            sx={{
              width: 40,
              height: 40,
              borderRadius: '50%',
              flexShrink: 0,
              bgcolor: `${serviceAccent(id)}1f`,
              color: serviceAccent(id),
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
            }}
          >
            <GcpServiceIcon id={id} size={24} />
          </Box>
          <Box sx={{ minWidth: 0, flexGrow: 1 }}>
            <Typography variant="subtitle1" noWrap>
              {title}
            </Typography>
            {subtitle && (
              <Typography variant="body2" color="text.secondary" noWrap>
                {subtitle}
              </Typography>
            )}
          </Box>
          {badge}
        </CardContent>
      </CardActionArea>
      {action && (
        <Box sx={{ position: 'absolute', top: 6, right: 6 }}>{action}</Box>
      )}
    </Card>
  )
}

/** Google Cloud console overview: welcome, project, resources and pinned/recent. */
export function GcpHome() {
  const { data: meta } = useMeta()
  const { accountId } = useAccount()
  const { data: servicesData } = useServices()
  const services = useMemo(() => servicesData?.services ?? [], [servicesData])
  const { favorites, isFavorite, toggle } = useFavorites()
  const recent = useRecentServices()
  const [projectOpen, setProjectOpen] = useState(false)

  const project = accountId || meta?.accountId || '—'

  const groups = useMemo(
    () => buildNavGroups(services, favorites, recent, ''),
    [services, favorites, recent],
  )
  const pinned = groups.find((group) => group.kind === 'pinned')?.entries ?? []
  const recentEntries = groups.find((group) => group.kind === 'recent')?.entries ?? []

  const byId = useMemo(() => new Map(services.map((service) => [service.id, service])), [services])
  const sources = useMemo(
    () => summarySourcesFor(services.map((service) => service.id)),
    [services],
  )
  const counts = useQueries({
    queries: sources.map((source) => ({
      queryKey: ['gcp', 'home', source.service, project],
      queryFn: () => source.list(),
      staleTime: 60_000,
      retry: false,
    })),
  })

  const pinAction = (id: string, label: string) => (
    <Tooltip title={isFavorite(id) ? `Unpin ${label}` : `Pin ${label}`}>
      <IconButton
        size="small"
        aria-label={isFavorite(id) ? `Unpin ${label}` : `Pin ${label}`}
        onClick={() => toggle(id)}
      >
        {isFavorite(id) ? (
          <StarIcon fontSize="small" color="primary" />
        ) : (
          <StarBorderIcon fontSize="small" />
        )}
      </IconButton>
    </Tooltip>
  )

  const serviceTile = (entry: NavEntry) => {
    const service = byId.get(entry.id)
    const tier = service ? tierLabel(service) : undefined

    return (
      <HomeTile
        key={entry.id}
        id={entry.id}
        title={entry.label}
        subtitle={entry.category}
        to={entry.path}
        onVisit={() => rememberRecentService(entry.id)}
        badge={
          tier ? <Chip size="small" label={tier} variant="outlined" /> : undefined
        }
        action={pinAction(entry.id, entry.label)}
      />
    )
  }

  return (
    <Box>
      <Typography variant="h4" sx={{ fontWeight: 400 }}>
        Welcome
      </Typography>
      <Typography variant="body1" color="text.secondary" sx={{ mt: 0.5 }}>
        {cloudName(meta?.cloud)} console · project {project} · {meta?.region ?? 'global'}
      </Typography>

      <Card variant="outlined" sx={{ mt: 3 }}>
        <CardContent>
          <Stack
            direction={{ xs: 'column', sm: 'row' }}
            spacing={2}
            sx={{ alignItems: { sm: 'center' } }}
          >
            <Box sx={{ flexGrow: 1, minWidth: 0 }}>
              <Typography variant="overline" color="text.secondary">
                Project
              </Typography>
              <Typography variant="h6" noWrap>
                {project}
              </Typography>
              <Stack direction="row" spacing={1} sx={{ mt: 0.5, flexWrap: 'wrap', rowGap: 0.5 }}>
                <Chip size="small" label={`Region: ${meta?.region ?? 'global'}`} variant="outlined" />
                <Chip
                  size="small"
                  label={`Instance: ${meta?.instanceId || '—'}`}
                  variant="outlined"
                />
                {meta?.mode && <Chip size="small" label={meta.mode} variant="outlined" />}
              </Stack>
            </Box>
            <Button
              variant="outlined"
              startIcon={<SwapHorizIcon />}
              onClick={() => setProjectOpen(true)}
              sx={{ alignSelf: { xs: 'flex-start', sm: 'center' } }}
            >
              Switch project
            </Button>
          </Stack>
        </CardContent>
      </Card>

      {pinned.length > 0 && (
        <>
          <Typography variant="h6" sx={{ mt: 4, mb: 1.5 }}>
            Pinned services
          </Typography>
          <Box sx={TILE_GRID}>{pinned.map(serviceTile)}</Box>
        </>
      )}

      {recentEntries.length > 0 && (
        <>
          <Typography variant="h6" sx={{ mt: 4, mb: 1.5 }}>
            Recent services
          </Typography>
          <Box sx={TILE_GRID}>{recentEntries.map(serviceTile)}</Box>
        </>
      )}

      {pinned.length === 0 && recentEntries.length === 0 && services.length > 0 && (
        <Alert severity="info" sx={{ mt: 3 }}>
          Pin a service from the navigation menu, or open one, to see it here.
        </Alert>
      )}

      <Typography variant="h6" sx={{ mt: 4, mb: 0.5 }}>
        Resource types
      </Typography>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 1.5 }}>
        Resource types the console can summarize, with live counts.
      </Typography>

      {services.length === 0 ? (
        <Alert severity="info">
          No service UIs are wired into this build yet. Use the gcloud CLI or the GCP SDKs
          against the emulator endpoints.
        </Alert>
      ) : (
        <Box sx={TILE_GRID}>
          {sources.map((source, index) => {
            const query = counts[index]
            const count = query?.data == null ? undefined : resourceCount(query.data, source.arrayKey)
            return (
              <HomeTile
                key={source.service}
                id={source.service}
                title={source.label}
                subtitle={byId.get(source.service)?.label}
                to={source.path}
                onVisit={() => rememberRecentService(source.service)}
                badge={
                  query?.isPending ? (
                    <Skeleton variant="text" width={24} />
                  ) : (
                    <Typography variant="h6" color={count == null ? 'text.disabled' : undefined}>
                      {count ?? '—'}
                    </Typography>
                  )
                }
              />
            )
          })}
        </Box>
      )}

      <ProjectPickerDialog open={projectOpen} onClose={() => setProjectOpen(false)} />
    </Box>
  )
}
