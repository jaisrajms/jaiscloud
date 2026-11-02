import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import {
  accessSecretVersion,
  deleteSecret,
  destroySecretVersion,
  disableSecretVersion,
  enableManagedRotation,
  enableSecretVersion,
  getSecret,
  getSecretIam,
  listSecretVersions,
  putSecretIam,
  rotateSecret,
  type SecretVersion,
} from '../../api/gcp/secretmanager'
import { useAccount } from '../../context/AccountContext'
import { IamPolicyPanel } from '../common/IamPolicyPanel'
import { AddVersionDialog } from './AddVersionDialog'
import { EditSecretDialog } from './EditSecretDialog'
import { fromBase64 } from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** One Secret Manager secret: metadata, versions and IAM policy. */
export function SecretDetailPage() {
  const { secret = '' } = useParams()
  const { accountId } = useAccount()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const invalidate = () =>
    void queryClient.invalidateQueries({ queryKey: ['gcp', 'secretmanager'] })
  const [addOpen, setAddOpen] = useState(false)
  const [editOpen, setEditOpen] = useState(false)
  const [revealed, setRevealed] = useState<{ version: string; value: string } | null>(null)
  const [managedOpen, setManagedOpen] = useState(false)
  const [instanceID, setInstanceID] = useState('')
  const [dbUser, setDBUser] = useState('')
  const [dbPassword, setDBPassword] = useState('')

  const detail = useQuery({
    queryKey: ['gcp', 'secretmanager', 'secret', secret, accountId],
    queryFn: () => getSecret(secret),
    enabled: Boolean(secret),
  })

  const versions = useQuery({
    queryKey: ['gcp', 'secretmanager', 'versions', secret, accountId],
    queryFn: () => listSecretVersions(secret),
    enabled: Boolean(secret),
  })

  const remove = useMutation({
    mutationFn: () => deleteSecret(secret),
    onSuccess: () => {
      invalidate()
      navigate('/gcp/secretmanager/secrets')
    },
  })

  const toggle = useMutation({
    mutationFn: (version: SecretVersion) =>
      version.state === 'ENABLED'
        ? disableSecretVersion(secret, version.versionId || '')
        : enableSecretVersion(secret, version.versionId || ''),
    onSuccess: invalidate,
  })

  const destroy = useMutation({
    mutationFn: (version: SecretVersion) =>
      destroySecretVersion(secret, version.versionId || ''),
    onSuccess: invalidate,
  })

  const rotate = useMutation({
    mutationFn: () => rotateSecret(secret),
    onSuccess: invalidate,
  })

  const enableManaged = useMutation({
    mutationFn: () =>
      enableManagedRotation(secret, {
        instanceId: instanceID.trim(),
        username: dbUser.trim(),
        password: dbPassword.trim() || undefined,
      }),
    onSuccess: () => {
      invalidate()
      setManagedOpen(false)
      setInstanceID('')
      setDBUser('')
      setDBPassword('')
    },
  })

  const reveal = useMutation({
    mutationFn: (version: SecretVersion) =>
      accessSecretVersion(secret, version.versionId || ''),
    onSuccess: (resp, version) =>
      setRevealed({ version: version.versionId || '', value: fromBase64(resp.data || '') }),
  })

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton
          component={RouterLink}
          to="/gcp/secretmanager/secrets"
          aria-label="Back to secrets"
        >
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="secretmanager">{secret}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Secret · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="outlined" disabled={!detail.data} onClick={() => setEditOpen(true)}>
          Edit
        </Button>
        <Button variant="outlined" color="error" disabled={remove.isPending} onClick={() => remove.mutate()}>
          Delete
        </Button>
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the secret.</Alert>}
      {detail.data && (
        <Box sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2, p: 2, mb: 3 }}>
          <Stack spacing={0.5}>
            <Typography variant="body2" color="text.secondary">
              Labels: {detail.data.labels ? Object.entries(detail.data.labels).map(([k, v]) => `${k}=${v}`).join(', ') : '—'}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Rotation: {detail.data.rotationPeriod || '—'}
              {detail.data.nextRotationTime ? ` (next ${detail.data.nextRotationTime})` : ''}
              {detail.data.managedRotationStatus
                ? ` · managed rotation ${detail.data.managedRotationStatus}`
                : ''}
            </Typography>
            <Stack direction="row" spacing={1} sx={{ pt: 1, flexWrap: 'wrap', rowGap: 1 }}>
              <Button
                size="small"
                variant="outlined"
                disabled={rotate.isPending || detail.data.managedRotationStatus !== 'ACTIVE'}
                onClick={() => rotate.mutate()}
              >
                Rotate now
              </Button>
              <Button
                size="small"
                variant="outlined"
                disabled={Boolean(detail.data.managedRotationStatus)}
                onClick={() => setManagedOpen(true)}
              >
                Enable managed rotation
              </Button>
            </Stack>
            {(rotate.isError || enableManaged.isError) && (
              <Alert severity="error">
                {((rotate.error ?? enableManaged.error) as Error).message}
              </Alert>
            )}
            {rotate.data && (
              <Alert severity="success">
                Rotated to version {rotate.data.versionId} ({rotate.data.state}).
              </Alert>
            )}
            <Typography variant="body2" color="text.secondary">
              Event topics: {detail.data.topics && detail.data.topics.length > 0 ? detail.data.topics.join(', ') : '—'}
            </Typography>
            {detail.data.kmsKeyName && (
              <Typography variant="body2" color="text.secondary">
                CMEK: {detail.data.kmsKeyName}
              </Typography>
            )}
            <Typography variant="body2" color="text.secondary">
              Created: {detail.data.createTime || '—'}
            </Typography>
          </Stack>
        </Box>
      )}

      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 1, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Typography variant="h6">Versions</Typography>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setAddOpen(true)}>
          Add version
        </Button>
      </Stack>

      {versions.isError && <Alert severity="error">Failed to load versions.</Alert>}
      {(toggle.isError || destroy.isError || reveal.isError) && (
        <Alert severity="error" sx={{ mb: 2 }}>
          The version operation failed.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Version</TableCell>
              <TableCell>State</TableCell>
              <TableCell>Created</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {versions.isLoading && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!versions.isLoading && (versions.data?.versions.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No versions. Add one to store a value.
                </TableCell>
              </TableRow>
            )}
            {versions.data?.versions.map((version) => {
              const mutable = version.state === 'ENABLED' || version.state === 'DISABLED'
              return (
                <TableRow key={version.versionId} hover>
                  <TableCell>v{version.versionId}</TableCell>
                  <TableCell>
                    <Chip
                      size="small"
                      label={version.state || '—'}
                      color={version.state === 'ENABLED' ? 'success' : 'default'}
                    />
                  </TableCell>
                  <TableCell>{version.createTime || '—'}</TableCell>
                  <TableCell align="right">
                    {version.state === 'ENABLED' && (
                      <Button size="small" onClick={() => reveal.mutate(version)}>
                        Reveal
                      </Button>
                    )}
                    {mutable && (
                      <Button size="small" onClick={() => toggle.mutate(version)}>
                        {version.state === 'ENABLED' ? 'Disable' : 'Enable'}
                      </Button>
                    )}
                    {mutable && (
                      <Tooltip title="Destroy version">
                        <span>
                          <IconButton
                            size="small"
                            disabled={destroy.isPending}
                            onClick={() => destroy.mutate(version)}
                            aria-label={`Destroy version ${version.versionId}`}
                          >
                            <DeleteOutlineIcon fontSize="small" />
                          </IconButton>
                        </span>
                      </Tooltip>
                    )}
                  </TableCell>
                </TableRow>
              )
            })}
          </TableBody>
        </Table>
      </TableContainer>

      <Box sx={{ mt: 4 }}>
        <IamPolicyPanel
          title="Secret IAM policy"
          queryKey={['gcp', 'secretmanager', 'secret', secret, 'iam']}
          load={() => getSecretIam(secret)}
          save={(policy) => putSecretIam(secret, policy)}
          defaultRole="roles/secretmanager.secretAccessor"
        />
      </Box>

      <AddVersionDialog open={addOpen} secret={secret} onClose={() => setAddOpen(false)} />
      <EditSecretDialog
        open={editOpen}
        secret={secret}
        labels={detail.data?.labels}
        topics={detail.data?.topics}
        rotationPeriod={detail.data?.rotationPeriod}
        nextRotationTime={detail.data?.nextRotationTime}
        onClose={() => setEditOpen(false)}
      />

      <Dialog open={managedOpen} onClose={() => setManagedOpen(false)} fullWidth maxWidth="sm">
        <DialogTitle>Enable Cloud SQL managed rotation</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            {enableManaged.isError && (
              <Alert severity="error">{(enableManaged.error as Error).message}</Alert>
            )}
            <TextField
              autoFocus
              label="Cloud SQL instance"
              value={instanceID}
              onChange={(e) => setInstanceID(e.target.value)}
              fullWidth
            />
            <TextField
              label="Database user"
              value={dbUser}
              onChange={(e) => setDBUser(e.target.value)}
              fullWidth
            />
            <TextField
              label="Password (leave empty to generate)"
              type="password"
              value={dbPassword}
              onChange={(e) => setDBPassword(e.target.value)}
              fullWidth
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setManagedOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={enableManaged.isPending || !instanceID.trim() || !dbUser.trim()}
            onClick={() => enableManaged.mutate()}
          >
            Enable
          </Button>
        </DialogActions>
      </Dialog>

      <Dialog open={Boolean(revealed)} onClose={() => setRevealed(null)} fullWidth maxWidth="sm">
        <DialogTitle>Version {revealed?.version} payload</DialogTitle>
        <DialogContent>
          <TextField
            value={revealed?.value ?? ''}
            multiline
            minRows={4}
            fullWidth
            slotProps={{ input: { readOnly: true, sx: { fontFamily: 'monospace' } } }}
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setRevealed(null)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  )
}
