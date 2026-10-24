import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  MenuItem,
  Stack,
  TextField,
  Typography,
} from '@mui/material'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import {
  createAcl,
  updateAcl,
  type AclWriteInput,
  type ManagedKafkaAcl,
  type ManagedKafkaAclEntry,
} from '../../api/gcp/managedkafka'
import { ACL_OPERATIONS, ACL_RESOURCES, composeAclID, splitAclID } from './util'

export interface AclDialogProps {
  open: boolean
  onClose: () => void
  location: string
  cluster: string
  /** The ACL to edit; omitted for a create. */
  acl?: ManagedKafkaAcl
  onSaved?: () => void
}

const DEFAULT_ENTRY: ManagedKafkaAclEntry = {
  principal: 'User:',
  permissionType: 'ALLOW',
  operation: 'READ',
  host: '*',
}

/** Create or update a Managed Kafka ACL and its entries. */
export function AclDialog({ open, onClose, location, cluster, acl, onSaved }: AclDialogProps) {
  const queryClient = useQueryClient()
  const editing = Boolean(acl)
  const [prefix, setPrefix] = useState('cluster')
  const [name, setName] = useState('')
  const [entries, setEntries] = useState<ManagedKafkaAclEntry[]>([])

  useEffect(() => {
    if (!open) return
    if (acl) {
      const parts = splitAclID(acl.id)
      setPrefix(parts.prefix)
      setName(parts.name)
      setEntries(acl.aclEntries.length > 0 ? acl.aclEntries : [{ ...DEFAULT_ENTRY }])
    } else {
      setPrefix('cluster')
      setName('')
      setEntries([{ ...DEFAULT_ENTRY }])
    }
  }, [open, acl])

  const save = useMutation({
    mutationFn: (input: AclWriteInput) =>
      editing ? updateAcl(location, cluster, acl!.id, input) : createAcl(location, cluster, input),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'managedkafka'] })
      onSaved?.()
      onClose()
    },
  })

  const setEntry = (index: number, patch: Partial<ManagedKafkaAclEntry>) =>
    setEntries((cur) => cur.map((entry, i) => (i === index ? { ...entry, ...patch } : entry)))

  const requiresName = prefix.endsWith('/')
  const aclID = composeAclID(prefix, name)
  const entriesValid =
    entries.length > 0 &&
    entries.every((entry) => entry.principal?.trim() && entry.permissionType && entry.operation)
  const canSave = Boolean((editing || (aclID && (!requiresName || name.trim()))) && entriesValid) && !save.isPending

  const handleSave = () => {
    if (editing) {
      save.mutate({ etag: acl!.etag, aclEntries: entries })
      return
    }
    save.mutate({ id: aclID, aclEntries: entries })
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>{editing ? `Edit ACL ${acl?.id}` : 'Create ACL'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">
              Could not {editing ? 'update' : 'create'} the ACL: {(save.error as Error).message}
            </Alert>
          )}
          <Stack direction="row" spacing={2}>
            <TextField
              select
              label="Resource"
              value={prefix}
              onChange={(e) => setPrefix(e.target.value)}
              disabled={editing}
              sx={{ minWidth: 220 }}
            >
              {ACL_RESOURCES.map((resource) => (
                <MenuItem key={resource.prefix} value={resource.prefix}>
                  {resource.label}
                </MenuItem>
              ))}
            </TextField>
            {!editing && requiresName && (
              <TextField
                label="Resource name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="orders"
                fullWidth
              />
            )}
          </Stack>
          {!editing && (
            <Typography variant="caption" color="text.secondary">
              ACL id: <code>{aclID || '—'}</code>
            </Typography>
          )}

          <Stack spacing={1}>
            <Typography variant="subtitle2">Entries</Typography>
            {entries.map((entry, index) => (
              <Box
                key={index}
                sx={{ display: 'grid', gridTemplateColumns: '2fr 1fr 1fr 1fr auto', gap: 1, alignItems: 'center' }}
              >
                <TextField
                  size="small"
                  label="Principal"
                  value={entry.principal ?? ''}
                  onChange={(e) => setEntry(index, { principal: e.target.value })}
                  placeholder="User:alice"
                />
                <TextField
                  size="small"
                  select
                  label="Permission"
                  value={entry.permissionType ?? 'ALLOW'}
                  onChange={(e) => setEntry(index, { permissionType: e.target.value })}
                >
                  <MenuItem value="ALLOW">ALLOW</MenuItem>
                  <MenuItem value="DENY">DENY</MenuItem>
                </TextField>
                <TextField
                  size="small"
                  select
                  label="Operation"
                  value={entry.operation ?? 'READ'}
                  onChange={(e) => setEntry(index, { operation: e.target.value })}
                >
                  {ACL_OPERATIONS.map((op) => (
                    <MenuItem key={op} value={op}>
                      {op}
                    </MenuItem>
                  ))}
                </TextField>
                <TextField
                  size="small"
                  label="Host"
                  value={entry.host ?? '*'}
                  onChange={(e) => setEntry(index, { host: e.target.value })}
                  placeholder="*"
                />
                <IconButton
                  size="small"
                  onClick={() => setEntries((cur) => cur.filter((_, i) => i !== index))}
                  disabled={entries.length === 1}
                  aria-label="Remove entry"
                >
                  <DeleteOutlineIcon fontSize="small" />
                </IconButton>
              </Box>
            ))}
            <Box>
              <Button size="small" onClick={() => setEntries((cur) => [...cur, { ...DEFAULT_ENTRY }])}>
                Add entry
              </Button>
            </Box>
          </Stack>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={!canSave} onClick={handleSave}>
          {editing ? 'Save' : 'Create'}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
