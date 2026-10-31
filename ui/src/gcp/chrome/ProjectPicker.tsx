import { Fragment, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQueryClient } from '@tanstack/react-query'
import {
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  List,
  ListItemButton,
  ListItemText,
  Stack,
  TextField,
  Typography,
} from '@mui/material'
import CheckIcon from '@mui/icons-material/Check'
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined'
import { createProject } from '../../api/gcp/resourcemanager'
import { useAccount, useAccounts } from '../../context/AccountContext'
import { useMeta } from '../../hooks/useMeta'
import { useServices } from '../../hooks/useServices'
import {
  groupProjects,
  projectServiceChips,
  pushRecent,
  readRecent,
  relativeProjectAge,
  RECENT_PROJECTS_KEY,
  type ProjectRowInfo,
} from './navModel'
import { useProjectDiscovery } from './useProjectDiscovery'

interface Props {
  open: boolean
  onClose: () => void
}

/**
 * "Select a project" dialog: an "Active (not registered)" group for projects a
 * workload wrote to, recently used projects, then every configured/registered
 * project. The active rows are selectable — the wire API serves any project id —
 * and can be registered in place.
 */
export function ProjectPickerDialog({ open, onClose }: Props) {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { accountId, setAccountId } = useAccount()
  const { data } = useAccounts()
  const { data: meta } = useMeta()
  const { data: servicesData } = useServices()
  const { data: discovery } = useProjectDiscovery()
  const [query, setQuery] = useState('')
  const [recent, setRecent] = useState<string[]>(() => readRecent(RECENT_PROJECTS_KEY))
  const [registering, setRegistering] = useState<string | null>(null)
  const [errors, setErrors] = useState<Record<string, string>>({})

  // Only offer the project manager when the server advertises Resource Manager,
  // so the picker never links to a page whose service is disabled.
  const canManage = (servicesData?.services ?? []).some((s) => s.id === 'resourcemanager')

  const current = accountId || meta?.accountId || ''
  const accounts = useMemo(
    () => data?.accounts ?? (current ? [current] : []),
    [data, current],
  )
  const projects = useMemo(
    () => (discovery?.projects ?? []) as ProjectRowInfo[],
    [discovery],
  )
  const groups = useMemo(
    () => groupProjects(accounts, projects, recent, query),
    [accounts, projects, recent, query],
  )
  const empty = groups.active.length === 0 && groups.recent.length === 0 && groups.all.length === 0

  const choose = (id: string) => {
    setAccountId(id)
    setRecent(pushRecent(RECENT_PROJECTS_KEY, id))
    setQuery('')
    onClose()
  }

  const register = async (id: string) => {
    setRegistering(id)
    setErrors((prev) => ({ ...prev, [id]: '' }))
    try {
      await createProject({ projectId: id })
      await queryClient.invalidateQueries({ queryKey: ['gcp', 'project-discovery'] })
      await queryClient.invalidateQueries({ queryKey: ['meta', 'accounts'] })
    } catch (error) {
      setErrors((prev) => ({
        ...prev,
        [id]: error instanceof Error ? error.message : String(error),
      }))
    } finally {
      setRegistering(null)
    }
  }

  const manage = () => {
    onClose()
    navigate('/gcp/resourcemanager/projects')
  }

  const row = (project: ProjectRowInfo) => {
    const observed = project.kind === 'observed'
    const services = projectServiceChips(project.services)
    const age = relativeProjectAge(project.lastSeen)
    return (
      <Fragment key={project.id}>
        <ListItemButton selected={project.id === current} onClick={() => choose(project.id)}>
          <ListItemText
            primary={project.id}
            secondary={
              observed && (age || services.length > 0) ? (
                <Stack
                  direction="row"
                  spacing={0.5}
                  sx={{ mt: 0.5, flexWrap: 'wrap', rowGap: 0.5, alignItems: 'center' }}
                >
                  {age && (
                    <Typography variant="caption" color="text.secondary">
                      {age}
                    </Typography>
                  )}
                  {services.map((service) => (
                    <Chip key={service} size="small" label={service} variant="outlined" />
                  ))}
                </Stack>
              ) : undefined
            }
          />
          {observed && (
            <Chip size="small" label="not registered" sx={{ mr: 1 }} variant="outlined" />
          )}
          {observed && (
            <Button
              size="small"
              disabled={registering === project.id}
              onClick={(event) => {
                event.stopPropagation()
                void register(project.id)
              }}
            >
              Register
            </Button>
          )}
          {project.id === current && <CheckIcon fontSize="small" />}
        </ListItemButton>
        {errors[project.id] && (
          <Typography variant="caption" color="error" sx={{ display: 'block', pl: 2, pb: 0.5 }}>
            {errors[project.id]}
          </Typography>
        )}
      </Fragment>
    )
  }

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
        {empty ? (
          <Typography variant="body2" color="text.secondary" sx={{ py: 2 }}>
            No projects match.
          </Typography>
        ) : (
          <>
            {groups.active.length > 0 && (
              <>
                <Typography variant="overline" color="text.secondary">
                  Active (not registered)
                </Typography>
                <List dense disablePadding>
                  {groups.active.map(row)}
                </List>
              </>
            )}
            {groups.recent.length > 0 && (
              <>
                <Typography variant="overline" color="text.secondary">
                  Recent
                </Typography>
                <List dense disablePadding>
                  {groups.recent.map(row)}
                </List>
              </>
            )}
            {groups.all.length > 0 && (
              <>
                <Typography variant="overline" color="text.secondary">
                  All projects
                </Typography>
                <List dense disablePadding>
                  {groups.all.map(row)}
                </List>
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
