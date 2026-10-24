import { useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  List,
  ListItemButton,
  ListItemText,
  TextField,
  Typography,
} from '@mui/material'
import CheckIcon from '@mui/icons-material/Check'
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined'
import { useAccount, useAccounts } from '../../context/AccountContext'
import { useMeta } from '../../hooks/useMeta'
import { useServices } from '../../hooks/useServices'
import { pushRecent, readRecent, RECENT_PROJECTS_KEY } from './navModel'

interface Props {
  open: boolean
  onClose: () => void
}

/**
 * "Select a project" dialog: a searchable list of every project the emulator
 * knows, with recently used projects pinned to the top.
 */
export function ProjectPickerDialog({ open, onClose }: Props) {
  const navigate = useNavigate()
  const { accountId, setAccountId } = useAccount()
  const { data } = useAccounts()
  const { data: meta } = useMeta()
  const { data: servicesData } = useServices()
  const [query, setQuery] = useState('')
  const [recent, setRecent] = useState<string[]>(() => readRecent(RECENT_PROJECTS_KEY))

  // Only offer the project manager when the server advertises Resource Manager,
  // so the picker never links to a page whose service is disabled.
  const canManage = (servicesData?.services ?? []).some((s) => s.id === 'resourcemanager')

  const current = accountId || meta?.accountId || ''
  const accounts = useMemo(
    () => data?.accounts ?? (current ? [current] : []),
    [data, current],
  )
  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    return q ? accounts.filter((account) => account.toLowerCase().includes(q)) : accounts
  }, [accounts, query])
  const recentShown = recent.filter((id) => filtered.includes(id))
  const others = filtered.filter((id) => !recentShown.includes(id))

  const choose = (id: string) => {
    setAccountId(id)
    setRecent(pushRecent(RECENT_PROJECTS_KEY, id))
    setQuery('')
    onClose()
  }

  const manage = () => {
    onClose()
    navigate('/gcp/resourcemanager/projects')
  }

  const projectList = (ids: string[]) => (
    <List dense disablePadding>
      {ids.map((id) => (
        <ListItemButton key={id} selected={id === current} onClick={() => choose(id)}>
          <ListItemText primary={id} />
          {id === current && <CheckIcon fontSize="small" />}
        </ListItemButton>
      ))}
    </List>
  )

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="xs">
      <DialogTitle>Select a project</DialogTitle>
      <DialogContent>
        <TextField
          autoFocus
          fullWidth
          size="small"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search projects"
          slotProps={{ htmlInput: { 'aria-label': 'Search projects' } }}
          sx={{ my: 1 }}
        />
        {filtered.length === 0 ? (
          <Typography variant="body2" color="text.secondary" sx={{ py: 2 }}>
            No projects match.
          </Typography>
        ) : (
          <>
            {recentShown.length > 0 && (
              <>
                <Typography variant="overline" color="text.secondary">
                  Recent
                </Typography>
                {projectList(recentShown)}
              </>
            )}
            {others.length > 0 && (
              <>
                <Typography variant="overline" color="text.secondary">
                  All projects
                </Typography>
                {projectList(others)}
              </>
            )}
          </>
        )}
      </DialogContent>
      <DialogActions>
        {canManage && (
          <Button onClick={manage} startIcon={<SettingsOutlinedIcon />} size="small">
            Manage projects
          </Button>
        )}
      </DialogActions>
    </Dialog>
  )
}
