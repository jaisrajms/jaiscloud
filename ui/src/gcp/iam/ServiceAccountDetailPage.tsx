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
  Tooltip,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import ContentCopyIcon from '@mui/icons-material/ContentCopy'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import {
  createServiceAccountKey,
  deleteServiceAccount,
  deleteServiceAccountKey,
  disableServiceAccount,
  disableServiceAccountKey,
  enableServiceAccount,
  enableServiceAccountKey,
  getServiceAccount,
  getServiceAccountIam,
  listServiceAccountKeys,
  putServiceAccountIam,
  type ServiceAccountKey,
} from '../../api/gcp/iam'
import { useAccount } from '../../context/AccountContext'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { IamPolicyPanel } from '../common/IamPolicyPanel'
import { EditServiceAccountDialog } from './EditServiceAccountDialog'
import { SignBlobDialog, SignJwtDialog } from './SignDialog'
import { GcpPageTitle } from '../common/PageTitle'

/** One IAM service account: metadata, keys and IAM policy. */
export function ServiceAccountDetailPage() {
  const { email = '' } = useParams()
  const { accountId } = useAccount()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'iam'] })
  const [newKey, setNewKey] = useState<ServiceAccountKey | null>(null)
  const [editOpen, setEditOpen] = useState(false)
  const [signBlobOpen, setSignBlobOpen] = useState(false)
  const [signJwtOpen, setSignJwtOpen] = useState(false)

  const detail = useQuery({
    queryKey: ['gcp', 'iam', 'serviceAccount', email, accountId],
    queryFn: () => getServiceAccount(email),
    enabled: Boolean(email),
  })

  const keys = useQuery({
    queryKey: ['gcp', 'iam', 'keys', email, accountId],
    queryFn: () => listServiceAccountKeys(email),
    enabled: Boolean(email),
  })

  const toggle = useMutation({
    mutationFn: (disabled: boolean) =>
      disabled ? enableServiceAccount(email) : disableServiceAccount(email),
    onSuccess: invalidate,
  })

  const remove = useMutation({
    mutationFn: () => deleteServiceAccount(email),
    onSuccess: () => {
      invalidate()
      navigate('/gcp/iam/service-accounts')
    },
  })

  const createKey = useMutation({
    mutationFn: () => createServiceAccountKey(email),
    onSuccess: (key) => {
      setNewKey(key)
      invalidate()
    },
  })

  const removeKey = useMutation({
    mutationFn: (key: ServiceAccountKey) => deleteServiceAccountKey(email, key.keyId),
    onSuccess: invalidate,
  })

  const toggleKey = useMutation({
    mutationFn: (key: ServiceAccountKey) =>
      key.disabled ? enableServiceAccountKey(email, key.keyId) : disableServiceAccountKey(email, key.keyId),
    onSuccess: invalidate,
  })

  const disabled = detail.data?.disabled ?? false

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton
          component={RouterLink}
          to="/gcp/iam/service-accounts"
          aria-label="Back to service accounts"
        >
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="iam">{email}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Service account · project {accountId || '—'}
          </Typography>
        </Box>
        {detail.data && (
          <Chip
            size="small"
            label={disabled ? 'Disabled' : 'Enabled'}
            color={disabled ? 'default' : 'success'}
          />
        )}
        <Button
          variant="outlined"
          disabled={!detail.data || toggle.isPending}
          onClick={() => toggle.mutate(disabled)}
        >
          {disabled ? 'Enable' : 'Disable'}
        </Button>
        <Button variant="outlined" disabled={!detail.data} onClick={() => setEditOpen(true)}>
          Edit
        </Button>
        <Button variant="outlined" disabled={!detail.data} onClick={() => setSignBlobOpen(true)}>
          Sign blob
        </Button>
        <Button variant="outlined" disabled={!detail.data} onClick={() => setSignJwtOpen(true)}>
          Sign JWT
        </Button>
        <Button
          variant="outlined"
          color="error"
          disabled={remove.isPending}
          onClick={() => remove.mutate()}
        >
          Delete
        </Button>
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the service account.</Alert>}
      {detail.data && (
        <Box sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2, p: 2, mb: 3 }}>
          <Stack spacing={0.5}>
            <Typography variant="body2">Display name: {detail.data.displayName || '—'}</Typography>
            <Typography variant="body2">Description: {detail.data.description || '—'}</Typography>
            <Typography variant="body2" color="text.secondary">
              Unique ID: {detail.data.uniqueId || '—'}
            </Typography>
          </Stack>
        </Box>
      )}

      <Stack
        direction="row"
        sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 1, flexWrap: 'wrap', rowGap: 1 }}
      >
        <Typography variant="h6">Keys</Typography>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          disabled={createKey.isPending}
          onClick={() => createKey.mutate()}
        >
          Create key
        </Button>
      </Stack>

      {keys.isError && <Alert severity="error">Failed to load keys.</Alert>}
      {removeKey.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Key ID</TableCell>
              <TableCell>Algorithm</TableCell>
              <TableCell>Status</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {keys.isLoading && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!keys.isLoading && (keys.data?.keys.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No keys.
                </TableCell>
              </TableRow>
            )}
            {keys.data?.keys.map((key) => (
              <TableRow key={key.keyId} hover>
                <TableCell sx={{ fontFamily: 'monospace' }}>{key.keyId}</TableCell>
                <TableCell>{key.keyAlgorithm || '—'}</TableCell>
                <TableCell>
                  <Chip
                    size="small"
                    label={key.disabled ? 'Disabled' : 'Active'}
                    color={key.disabled ? 'default' : 'success'}
                  />
                </TableCell>
                <TableCell align="right">
                  <Button size="small" onClick={() => toggleKey.mutate(key)}>
                    {key.disabled ? 'Enable' : 'Disable'}
                  </Button>
                  <Tooltip title="Delete key">
                    <span>
                      <IconButton
                        size="small"
                        disabled={removeKey.isPending}
                        onClick={() => removeKey.mutate(key)}
                        aria-label={`Delete ${key.keyId}`}
                      >
                        <DeleteOutlineIcon fontSize="small" />
                      </IconButton>
                    </span>
                  </Tooltip>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <Box sx={{ mt: 4 }}>
        <IamPolicyPanel
          title="Service account IAM policy"
          queryKey={['gcp', 'iam', 'serviceAccount', email, 'iam']}
          load={() => getServiceAccountIam(email)}
          save={(policy) => putServiceAccountIam(email, policy)}
          defaultRole="roles/iam.serviceAccountUser"
        />
      </Box>

      <NewKeyDialog keyData={newKey} onClose={() => setNewKey(null)} />
      <EditServiceAccountDialog
        open={editOpen}
        email={email}
        displayName={detail.data?.displayName ?? ''}
        description={detail.data?.description ?? ''}
        etag={detail.data?.etag}
        onClose={() => setEditOpen(false)}
      />
      <SignBlobDialog open={signBlobOpen} email={email} onClose={() => setSignBlobOpen(false)} />
      <SignJwtDialog open={signJwtOpen} email={email} onClose={() => setSignJwtOpen(false)} />
    </Box>
  )
}

/** Shows the one-time private key material returned by keys.create. */
function NewKeyDialog({
  keyData,
  onClose,
}: {
  keyData: ServiceAccountKey | null
  onClose: () => void
}) {
  let credentials = ''
  if (keyData?.privateKeyData) {
    try {
      credentials = JSON.stringify(JSON.parse(atob(keyData.privateKeyData)), null, 2)
    } catch {
      credentials = keyData.privateKeyData
    }
  }
  return (
    <Dialog open={Boolean(keyData)} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Key created</DialogTitle>
      <DialogContent>
        <Alert severity="warning" sx={{ mb: 2 }}>
          This is the only time the private key is shown. Copy it now.
        </Alert>
        <GcpCodeEditor
          value={credentials}
          readOnly
          language="json"
          minRows={8}
          ariaLabel="Service account key JSON"
        />
      </DialogContent>
      <DialogActions>
        <Button
          startIcon={<ContentCopyIcon />}
          onClick={() => void navigator.clipboard.writeText(credentials)}
        >
          Copy
        </Button>
        <Button variant="contained" onClick={onClose}>
          Done
        </Button>
      </DialogActions>
    </Dialog>
  )
}
