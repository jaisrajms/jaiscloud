import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Avatar, Divider, IconButton, ListItemIcon, Menu, MenuItem, Tooltip, Typography } from '@mui/material'
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined'
import OpenInNewIcon from '@mui/icons-material/OpenInNew'
import SwapHorizIcon from '@mui/icons-material/SwapHoriz'
import { useAccount } from '../../context/AccountContext'
import { useMeta } from '../../hooks/useMeta'
import { docsLink } from '../../lib/cloudLinks'

interface Props {
  onSelectProject: () => void
}

/** Project avatar button with the account/project menu (mirrors the GCP console). */
export function AccountMenu({ onSelectProject }: Props) {
  const navigate = useNavigate()
  const { accountId } = useAccount()
  const { data: meta } = useMeta()
  const [anchor, setAnchor] = useState<HTMLElement | null>(null)
  const current = accountId || meta?.accountId || ''
  const docs = docsLink('gcp')

  return (
    <>
      <Tooltip title={current ? `Project: ${current}` : 'Project'}>
        <IconButton
          onClick={(event) => setAnchor(event.currentTarget)}
          aria-label="Account menu"
          sx={{ ml: 0.5 }}
        >
          <Avatar sx={{ width: 32, height: 32, bgcolor: 'primary.main', fontSize: 14 }}>
            {current ? current.charAt(0).toUpperCase() : 'J'}
          </Avatar>
        </IconButton>
      </Tooltip>
      <Menu anchorEl={anchor} open={Boolean(anchor)} onClose={() => setAnchor(null)}>
        <MenuItem disabled>
          <Typography variant="body2" color="text.secondary" noWrap>
            {current || 'No project selected'}
          </Typography>
        </MenuItem>
        <Divider />
        <MenuItem
          onClick={() => {
            setAnchor(null)
            onSelectProject()
          }}
        >
          <ListItemIcon>
            <SwapHorizIcon fontSize="small" />
          </ListItemIcon>
          Select a project…
        </MenuItem>
        <MenuItem
          onClick={() => {
            setAnchor(null)
            navigate('/gcp/admin')
          }}
        >
          <ListItemIcon>
            <SettingsOutlinedIcon fontSize="small" />
          </ListItemIcon>
          Admin
        </MenuItem>
        <Divider />
        <MenuItem
          component="a"
          href={docs.href}
          target="_blank"
          rel="noreferrer"
          onClick={() => setAnchor(null)}
        >
          <ListItemIcon>
            <OpenInNewIcon fontSize="small" />
          </ListItemIcon>
          {docs.label}
        </MenuItem>
      </Menu>
    </>
  )
}
