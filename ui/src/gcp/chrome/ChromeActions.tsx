import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Chip, Divider, IconButton, ListItemIcon, Menu, MenuItem, Tooltip, Typography } from '@mui/material'
import TerminalIcon from '@mui/icons-material/Terminal'
import NotificationsNoneIcon from '@mui/icons-material/NotificationsNone'
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined'
import HelpOutlineOutlinedIcon from '@mui/icons-material/HelpOutlineOutlined'
import ShieldOutlinedIcon from '@mui/icons-material/ShieldOutlined'
import CheckOutlinedIcon from '@mui/icons-material/CheckOutlined'
import { docsLink } from '../../lib/cloudLinks'
import type { GcpAppearance, GcpModePreference } from '../appearance'
import type { GcpDensity } from '../theme'

interface MenuState {
  anchor: HTMLElement | null
  kind: 'notifications' | 'settings' | null
}

const MODE_OPTIONS: { value: GcpModePreference; label: string }[] = [
  { value: 'light', label: 'Light' },
  { value: 'dark', label: 'Dark' },
  { value: 'system', label: 'System default' },
]

const DENSITY_OPTIONS: { value: GcpDensity; label: string }[] = [
  { value: 'comfortable', label: 'Comfortable' },
  { value: 'compact', label: 'Compact' },
]

/** Right-hand console action row: Cloud Shell, notifications, settings, help. */
export function ChromeActions({
  connected,
  appearance,
}: {
  connected: boolean
  appearance: GcpAppearance
}) {
  const navigate = useNavigate()
  const docs = docsLink('gcp')
  const [menu, setMenu] = useState<MenuState>({ anchor: null, kind: null })
  const close = () => setMenu({ anchor: null, kind: null })

  return (
    <>
      <Tooltip
        title={connected ? 'Live updates active' : 'Stream disconnected — falling back to polling'}
      >
        <Chip
          size="small"
          label={connected ? 'Live' : 'Polling'}
          color={connected ? 'success' : 'default'}
          variant="outlined"
          sx={{ mr: 1, display: { xs: 'none', sm: 'inline-flex' } }}
        />
      </Tooltip>
      <Tooltip title="Cloud Shell is not available in the emulator">
        <span>
          <IconButton disabled aria-label="Cloud Shell" sx={{ color: 'text.secondary' }}>
            <TerminalIcon />
          </IconButton>
        </span>
      </Tooltip>
      <Tooltip title="Notifications">
        <IconButton
          aria-label="Notifications"
          sx={{ color: 'text.secondary' }}
          onClick={(event) => setMenu({ anchor: event.currentTarget, kind: 'notifications' })}
        >
          <NotificationsNoneIcon />
        </IconButton>
      </Tooltip>
      <Tooltip title="Settings">
        <IconButton
          aria-label="Settings"
          sx={{ color: 'text.secondary' }}
          onClick={(event) => setMenu({ anchor: event.currentTarget, kind: 'settings' })}
        >
          <SettingsOutlinedIcon />
        </IconButton>
      </Tooltip>
      <Tooltip title={docs.label}>
        <IconButton
          aria-label={docs.label}
          component="a"
          href={docs.href}
          target="_blank"
          rel="noreferrer"
          sx={{ color: 'text.secondary' }}
        >
          <HelpOutlineOutlinedIcon />
        </IconButton>
      </Tooltip>

      <Menu anchorEl={menu.anchor} open={Boolean(menu.anchor)} onClose={close}>
        {menu.kind === 'notifications' && <MenuItem disabled>No new notifications</MenuItem>}
        {menu.kind === 'settings' && [
          <MenuItem
            key="admin"
            onClick={() => {
              close()
              navigate('/gcp/admin')
            }}
          >
            <ListItemIcon>
              <ShieldOutlinedIcon fontSize="small" />
            </ListItemIcon>
            Admin panel
          </MenuItem>,
          <Divider key="divider-appearance" />,
          <Typography
            key="appearance-label"
            variant="overline"
            sx={{ display: 'block', px: 2, pt: 1, color: 'text.secondary' }}
          >
            Appearance
          </Typography>,
          ...MODE_OPTIONS.map((option) => (
            <MenuItem
              key={`mode-${option.value}`}
              selected={appearance.preference === option.value}
              onClick={() => {
                appearance.setPreference(option.value)
                close()
              }}
            >
              <ListItemIcon sx={{ minWidth: 32 }}>
                {appearance.preference === option.value ? (
                  <CheckOutlinedIcon fontSize="small" />
                ) : null}
              </ListItemIcon>
              {option.label}
            </MenuItem>
          )),
          <Divider key="divider-density" />,
          <Typography
            key="density-label"
            variant="overline"
            sx={{ display: 'block', px: 2, pt: 1, color: 'text.secondary' }}
          >
            Density
          </Typography>,
          ...DENSITY_OPTIONS.map((option) => (
            <MenuItem
              key={`density-${option.value}`}
              selected={appearance.density === option.value}
              onClick={() => {
                appearance.setDensity(option.value)
                close()
              }}
            >
              <ListItemIcon sx={{ minWidth: 32 }}>
                {appearance.density === option.value ? (
                  <CheckOutlinedIcon fontSize="small" />
                ) : null}
              </ListItemIcon>
              {option.label}
            </MenuItem>
          )),
        ]}
      </Menu>
    </>
  )
}
