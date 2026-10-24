import { useEffect, useMemo, useRef, useState } from 'react'
import { Link as RouterLink, useLocation } from 'react-router-dom'
import {
  Box,
  Collapse,
  Divider,
  IconButton,
  InputAdornment,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Stack,
  TextField,
  Toolbar,
  Tooltip,
  Typography,
} from '@mui/material'
import SearchIcon from '@mui/icons-material/Search'
import HomeOutlinedIcon from '@mui/icons-material/HomeOutlined'
import StarIcon from '@mui/icons-material/Star'
import StarBorderIcon from '@mui/icons-material/StarBorder'
import ExpandLessIcon from '@mui/icons-material/ExpandLess'
import ExpandMoreIcon from '@mui/icons-material/ExpandMore'
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined'
import CloseIcon from '@mui/icons-material/Close'
import type { ServiceDescriptor } from '../../api/services'
import { GcpServiceIcon } from '../icons/GcpServiceIcon'
import { useFavorites } from '../../hooks/useFavorites'
import { buildNavGroups, type NavEntry } from './navModel'
import { rememberRecentService, useRecentServices } from './recentServices'

/** Children that add a real sub-nav (a lone child equal to the root is noise). */
function subNav(entry: NavEntry) {
  return entry.children.filter((child) => child.path !== entry.path)
}

export interface GcpNavProps {
  services: ServiceDescriptor[]
  /**
   * Whether the surface is currently visible. Used only to refocus the search
   * box when the mobile overlay reopens (the rail never shows search).
   */
  open?: boolean
  /** Render the service search field. The mobile overlay sets this; the rail does not. */
  showSearch?: boolean
  /**
   * Render a close button and call after every navigation. The mobile overlay
   * sets this so a tap closes it; the permanent rail leaves it unset.
   */
  onClose?: () => void
  /** Header subtitle ("Console" on the rail, "Navigation menu" in the overlay). */
  title?: string
  /**
   * Presentation of the desktop rail. `mini` is the icons-only rail (product
   * glyphs with tooltips, no category headers, labels, pin/expand or search);
   * `full` is the labelled list. Ignored by the mobile overlay.
   */
  variant?: 'full' | 'mini'
}

/**
 * The single GCP console navigation. The exact same list renders inside the
 * desktop permanent rail and the mobile navigation overlay — one source of
 * truth for the service list, favorites, recents, sub-nav, selection and the
 * Admin link. It is deliberately navigational only: it carries no tier or
 * status text.
 */
export function GcpNav({
  services,
  open = false,
  showSearch = false,
  onClose,
  title = 'Navigation menu',
  variant = 'full',
}: GcpNavProps) {
  const location = useLocation()
  const { favorites, isFavorite, toggle } = useFavorites()
  const recent = useRecentServices()
  const [query, setQuery] = useState('')
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set())
  const searchRef = useRef<HTMLInputElement>(null)

  const groups = useMemo(
    () => buildNavGroups(services, favorites, recent, query),
    [services, favorites, recent, query],
  )

  useEffect(() => {
    if (open && showSearch) searchRef.current?.focus()
  }, [open, showSearch])

  const isSelected = (path: string) =>
    location.pathname === path || location.pathname.startsWith(`${path}/`)

  const select = (serviceId: string) => {
    rememberRecentService(serviceId)
    onClose?.()
  }

  const toggleExpanded = (entryId: string) => {
    setExpanded((current) => {
      const next = new Set(current)
      if (next.has(entryId)) next.delete(entryId)
      else next.add(entryId)
      return next
    })
  }

  // Icons-only rail: product glyphs with tooltips, no labels/headers/search.
  if (variant === 'mini') {
    return (
      <Box
        role="navigation"
        aria-label="Service navigation"
        sx={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}
      >
        <Toolbar />
        <List sx={{ py: 1 }}>
          <Tooltip title="Console home" placement="right">
            <ListItemButton
              component={RouterLink}
              to="/gcp"
              selected={location.pathname === '/gcp'}
              onClick={() => onClose?.()}
              sx={{ justifyContent: 'center', px: 0, minHeight: 44 }}
            >
              <HomeOutlinedIcon fontSize="small" />
            </ListItemButton>
          </Tooltip>
        </List>
        <Divider />
        <List
          component="nav"
          sx={{
            flexGrow: 1,
            minHeight: 0,
            overflowY: 'auto',
            py: 1,
            scrollbarWidth: 'none',
            '&::-webkit-scrollbar': { display: 'none' },
          }}
        >
          {services.map((service) => (
            <Tooltip key={service.id} title={service.label} placement="right">
              <ListItemButton
                component={RouterLink}
                to={service.rootPath}
                selected={isSelected(service.rootPath)}
                onClick={() => select(service.id)}
                sx={{ justifyContent: 'center', px: 0, minHeight: 44 }}
              >
                <GcpServiceIcon id={service.id} size={22} />
              </ListItemButton>
            </Tooltip>
          ))}
        </List>
        <Divider />
        <List sx={{ py: 1 }}>
          <Tooltip title="Admin" placement="right">
            <ListItemButton
              component={RouterLink}
              to="/gcp/admin"
              selected={isSelected('/gcp/admin')}
              onClick={() => onClose?.()}
              sx={{ justifyContent: 'center', px: 0, minHeight: 44 }}
            >
              <SettingsOutlinedIcon fontSize="small" />
            </ListItemButton>
          </Tooltip>
        </List>
      </Box>
    )
  }

  return (
    <Box
      role="navigation"
      sx={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0 }}
    >
      {/* Reserve space for the fixed AppBar that overlays the top of both surfaces. */}
      <Toolbar />
      <Box sx={{ px: 2, py: 1.5, display: 'flex', alignItems: 'center', gap: 1 }}>
        <Box sx={{ minWidth: 0, flexGrow: 1 }}>
          <Typography variant="overline" color="text.secondary">
            JaisCloud
          </Typography>
          <Typography variant="subtitle1" sx={{ lineHeight: 1.1 }} noWrap>
            {title}
          </Typography>
        </Box>
        {onClose && (
          <IconButton edge="end" aria-label="Close navigation menu" onClick={onClose}>
            <CloseIcon />
          </IconButton>
        )}
      </Box>
      <Divider />
      {showSearch && (
        <>
          <Box sx={{ px: 2, py: 1.5 }}>
            <TextField
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search for products and services"
              size="small"
              fullWidth
              inputRef={searchRef}
              slotProps={{
                input: {
                  startAdornment: (
                    <InputAdornment position="start">
                      <SearchIcon fontSize="small" sx={{ color: 'text.secondary' }} />
                    </InputAdornment>
                  ),
                },
                htmlInput: { 'aria-label': 'Search navigation' },
              }}
            />
          </Box>
          <Divider />
        </>
      )}
      <List
        component="nav"
        sx={{
          flexGrow: 1,
          minHeight: 0,
          overflowY: 'auto',
          pb: 2,
          // Keep the list scrollable but hide the scrollbar on both surfaces.
          scrollbarWidth: 'none',
          '&::-webkit-scrollbar': { display: 'none' },
        }}
      >
        <ListItem disablePadding>
          <ListItemButton
            component={RouterLink}
            to="/gcp"
            selected={location.pathname === '/gcp'}
            onClick={() => onClose?.()}
          >
            <ListItemText primary="Console home" />
          </ListItemButton>
        </ListItem>
        {showSearch && groups.length === 0 && (
          <Box sx={{ px: 2, py: 3 }}>
            <Typography variant="body2" color="text.secondary">
              No matching services
            </Typography>
          </Box>
        )}
        {groups.map((group) => (
          <Box key={`${group.kind}:${group.label}`}>
            <Typography
              variant="overline"
              sx={{ display: 'block', px: 2, pt: 1.5, color: 'text.secondary' }}
            >
              {group.label}
            </Typography>
            {group.entries.map((entry) => {
              const children = subNav(entry)
              const isOpen = expanded.has(entry.id)
              return (
                <Box key={entry.id}>
                  <ListItem
                    disablePadding
                    secondaryAction={
                      <Stack direction="row" spacing={0.25}>
                        {children.length > 0 && (
                          <IconButton
                            size="small"
                            aria-label={`Toggle ${entry.label} sections`}
                            aria-expanded={isOpen}
                            onClick={() => toggleExpanded(entry.id)}
                          >
                            {isOpen ? (
                              <ExpandLessIcon fontSize="small" />
                            ) : (
                              <ExpandMoreIcon fontSize="small" />
                            )}
                          </IconButton>
                        )}
                        <IconButton
                          size="small"
                          aria-label={
                            isFavorite(entry.id)
                              ? `Unpin ${entry.label}`
                              : `Pin ${entry.label}`
                          }
                          onClick={() => toggle(entry.id)}
                        >
                          {isFavorite(entry.id) ? (
                            <StarIcon fontSize="small" color="primary" />
                          ) : (
                            <StarBorderIcon fontSize="small" />
                          )}
                        </IconButton>
                      </Stack>
                    }
                  >
                    <ListItemButton
                      component={RouterLink}
                      to={entry.path}
                      selected={isSelected(entry.path)}
                      onClick={() => select(entry.id)}
                      sx={{ pr: children.length > 0 ? 10 : 7 }}
                    >
                      <ListItemIcon sx={{ minWidth: 36 }}>
                        <GcpServiceIcon id={entry.id} size={20} />
                      </ListItemIcon>
                      <ListItemText primary={entry.label} secondary={entry.category} />
                    </ListItemButton>
                  </ListItem>
                  {children.length > 0 && (
                    <Collapse in={isOpen} timeout="auto" unmountOnExit>
                      <List disablePadding>
                        {children.map((child) => (
                          <ListItem key={child.path} disablePadding>
                            <ListItemButton
                              component={RouterLink}
                              to={child.path}
                              selected={isSelected(child.path)}
                              onClick={() => select(entry.id)}
                              // Align the child label under the parent's label
                              // (past the 36px icon column), not the icon.
                              sx={{ pl: 6.5 }}
                            >
                              <ListItemText primary={child.label} />
                            </ListItemButton>
                          </ListItem>
                        ))}
                      </List>
                    </Collapse>
                  )}
                </Box>
              )
            })}
          </Box>
        ))}
      </List>
      <Divider />
      <List sx={{ py: 1 }}>
        <ListItem disablePadding>
          <ListItemButton
            component={RouterLink}
            to="/gcp/admin"
            selected={isSelected('/gcp/admin')}
            onClick={() => onClose?.()}
          >
            <ListItemIcon>
              <SettingsOutlinedIcon fontSize="small" />
            </ListItemIcon>
            <ListItemText primary="Admin" />
          </ListItemButton>
        </ListItem>
      </List>
    </Box>
  )
}
