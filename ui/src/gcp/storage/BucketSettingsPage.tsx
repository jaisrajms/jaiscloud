import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  FormControlLabel,
  IconButton,
  Link,
  Stack,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink, useParams } from 'react-router-dom'
import {
  getBucket,
  getBucketIam,
  getBucketLifecycle,
  getBucketRetention,
  getBucketVersioning,
  insertBucketAcl,
  listBucketAcl,
  lockBucketRetention,
  putBucketIam,
  putBucketLifecycle,
  putBucketRetention,
  putBucketVersioning,
  putDefaultEventBasedHold,
  type IamBinding,
  type IamPolicy,
} from '../../api/gcp/storage'
import { useAccount } from '../../context/AccountContext'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpTabs } from '../common/GcpTabs'

/** Bucket configuration: versioning, lifecycle, retention/holds, permissions. */
export function BucketSettingsPage() {
  const { bucket = '' } = useParams()
  const [tab, setTab] = useState(0)

  const objectsPath = `/gcp/storage/buckets/${encodeURIComponent(bucket)}`

  return (
    <Box>
      <GcpPageHeader
        id="storage"
        title={bucket}
        backTo={objectsPath}
        backAriaLabel="Back to objects"
        subtitle={
          <Link component={RouterLink} to={objectsPath}>
            Objects
          </Link>
        }
      />

      <GcpTabs
        aria-label="Bucket settings"
        value={tab}
        onChange={(value) => setTab(Number(value))}
        tabs={[
          { label: 'Versioning', value: 0 },
          { label: 'Lifecycle', value: 1 },
          { label: 'Retention & holds', value: 2 },
          { label: 'Permissions', value: 3 },
        ]}
      />

      {tab === 0 && <VersioningTab bucket={bucket} />}
      {tab === 1 && <LifecycleTab bucket={bucket} />}
      {tab === 2 && <RetentionTab bucket={bucket} />}
      {tab === 3 && <PermissionsTab bucket={bucket} />}
    </Box>
  )
}

function useBucketMutation<T>(fn: (arg: T) => Promise<unknown>, bucket: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: fn,
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'bucket', bucket] }),
  })
}

function VersioningTab({ bucket }: { bucket: string }) {
  const { accountId } = useAccount()
  const query = useQuery({
    queryKey: ['gcp', 'storage', 'bucket', bucket, 'versioning', accountId],
    queryFn: () => getBucketVersioning(bucket),
  })
  const save = useBucketMutation(
    (enabled: boolean) => putBucketVersioning(bucket, enabled),
    bucket,
  )
  const enabled = Boolean(query.data?.versioning?.enabled)

  return (
    <Stack spacing={2} sx={{ maxWidth: 560 }}>
      {query.isLoading && <CircularProgress size={20} />}
      {query.isError && <Alert severity="error">Failed to load versioning.</Alert>}
      {save.isError && <Alert severity="error">Failed to update versioning.</Alert>}
      {save.isSuccess && <Alert severity="success">Versioning updated.</Alert>}
      <FormControlLabel
        control={
          <Switch
            checked={enabled}
            disabled={query.isLoading || save.isPending}
            onChange={(e) => save.mutate(e.target.checked)}
          />
        }
        label="Object versioning"
      />
      <Typography variant="body2" color="text.secondary">
        When enabled, overwriting or deleting an object keeps the previous generation as a
        noncurrent version that can be restored.
      </Typography>
    </Stack>
  )
}

function LifecycleTab({ bucket }: { bucket: string }) {
  const { accountId } = useAccount()
  const query = useQuery({
    queryKey: ['gcp', 'storage', 'bucket', bucket, 'lifecycle', accountId],
    queryFn: () => getBucketLifecycle(bucket),
  })
  const [text, setText] = useState('')
  const [error, setError] = useState('')

  useEffect(() => {
    if (query.data) setText(JSON.stringify(query.data.lifecycle ?? {}, null, 2))
  }, [query.data])

  const save = useBucketMutation(
    (lifecycle: Record<string, unknown> | null) => putBucketLifecycle(bucket, lifecycle),
    bucket,
  )

  const onSave = () => {
    let value: Record<string, unknown> | null
    try {
      const parsed = JSON.parse(text)
      value = parsed && Object.keys(parsed).length > 0 ? parsed : null
    } catch {
      setError('Lifecycle must be valid JSON.')
      return
    }
    setError('')
    save.mutate(value)
  }

  return (
    <Stack spacing={2} sx={{ maxWidth: 720 }}>
      {query.isError && <Alert severity="error">Failed to load lifecycle.</Alert>}
      {save.isError && <Alert severity="error">Failed to save lifecycle.</Alert>}
      {save.isSuccess && <Alert severity="success">Lifecycle saved.</Alert>}
      {error && <Alert severity="error">{error}</Alert>}
      <Typography variant="body2" color="text.secondary">
        Lifecycle configuration as JSON. Example: {`{"rule":[{"action":{"type":"Delete"},"condition":{"age":30}}]}`}.
        Save an empty object to clear all rules.
      </Typography>
      <GcpCodeEditor
        value={text}
        onChange={(value) => {
          setText(value)
          setError('')
        }}
        error={error || null}
        language="json"
        minRows={10}
        ariaLabel="Lifecycle configuration"
      />
      <Stack direction="row" spacing={1}>
        <Button variant="contained" disabled={save.isPending} onClick={onSave}>
          Save
        </Button>
        <Button
          disabled={save.isPending}
          onClick={() => {
            setText('{}')
            setError('')
            save.mutate(null)
          }}
        >
          Clear
        </Button>
      </Stack>
    </Stack>
  )
}

function RetentionTab({ bucket }: { bucket: string }) {
  const { accountId } = useAccount()
  const retention = useQuery({
    queryKey: ['gcp', 'storage', 'bucket', bucket, 'retention', accountId],
    queryFn: () => getBucketRetention(bucket),
  })
  const bucketMeta = useQuery({
    queryKey: ['gcp', 'storage', 'bucket', bucket, 'meta', accountId],
    queryFn: () => getBucket(bucket),
  })
  const [period, setPeriod] = useState('')

  useEffect(() => {
    if (retention.data) setPeriod(retention.data.retentionPolicy?.retentionPeriod ?? '')
  }, [retention.data])

  const save = useBucketMutation(
    (retentionPolicy: { retentionPeriod: string } | null) =>
      putBucketRetention(bucket, retentionPolicy),
    bucket,
  )
  const queryClient = useQueryClient()
  const lock = useMutation({
    mutationFn: () => lockBucketRetention(bucket),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'bucket', bucket] }),
  })
  const defaultHold = useBucketMutation(
    (hold: boolean) => putDefaultEventBasedHold(bucket, hold),
    bucket,
  )

  const policy = retention.data?.retentionPolicy
  const isLocked = Boolean(policy?.isLocked)
  const holdEnabled = Boolean((bucketMeta.data as { defaultEventBasedHold?: boolean })?.defaultEventBasedHold)

  return (
    <Stack spacing={2} sx={{ maxWidth: 560 }}>
      {retention.isError && <Alert severity="error">Failed to load retention policy.</Alert>}
      {save.isError && <Alert severity="error">Failed to update retention policy.</Alert>}
      {lock.isError && <Alert severity="error">Failed to lock retention policy.</Alert>}
      {save.isSuccess && <Alert severity="success">Retention policy saved.</Alert>}
      {lock.isSuccess && <Alert severity="success">Retention policy locked.</Alert>}

      <Stack direction="row" spacing={1} sx={{ alignItems: 'center' }}>
        <TextField
          label="Retention period (seconds)"
          value={period}
          onChange={(e) => setPeriod(e.target.value)}
          disabled={isLocked}
          size="small"
        />
        <Button
          variant="contained"
          disabled={save.isPending || isLocked}
          onClick={() =>
            save.mutate(period ? { retentionPeriod: period } : null)
          }
        >
          Save
        </Button>
        {policy && !isLocked && (
          <Button color="warning" disabled={lock.isPending} onClick={() => lock.mutate()}>
            Lock
          </Button>
        )}
        {isLocked && <Chip color="warning" label="Locked" />}
      </Stack>
      {policy?.effectiveTime && (
        <Typography variant="body2" color="text.secondary">
          Effective {policy.effectiveTime}
        </Typography>
      )}

      <Divider />
      <FormControlLabel
        control={
          <Switch
            checked={holdEnabled}
            disabled={defaultHold.isPending}
            onChange={(e) => defaultHold.mutate(e.target.checked)}
          />
        }
        label="Default event-based hold for new objects"
      />
    </Stack>
  )
}

function PermissionsTab({ bucket }: { bucket: string }) {
  return (
    <Stack spacing={4}>
      <IamSection bucket={bucket} />
      <Divider />
      <AclSection bucket={bucket} />
    </Stack>
  )
}

function IamSection({ bucket }: { bucket: string }) {
  const { accountId } = useAccount()
  const query = useQuery({
    queryKey: ['gcp', 'storage', 'bucket', bucket, 'iam', accountId],
    queryFn: () => getBucketIam(bucket),
  })
  const [bindings, setBindings] = useState<IamBinding[]>([])
  const [role, setRole] = useState('roles/storage.objectViewer')
  const [members, setMembers] = useState('')

  useEffect(() => {
    setBindings(query.data?.bindings ?? [])
  }, [query.data])

  const save = useBucketMutation(
    (policy: IamPolicy) => putBucketIam(bucket, policy),
    bucket,
  )

  const add = () => {
    if (!role || !members.trim()) return
    setBindings((prev) => [
      ...prev.filter((b) => b.role !== role),
      { role, members: members.split(',').map((m) => m.trim()).filter(Boolean) },
    ])
    setMembers('')
  }

  return (
    <Box sx={{ maxWidth: 720 }}>
      <Typography variant="h6" sx={{ mb: 1 }}>
        Bucket IAM policy
      </Typography>
      {query.isError && <Alert severity="error">Failed to load IAM policy.</Alert>}
      {save.isError && <Alert severity="error">Failed to save (etag mismatch?).</Alert>}
      {save.isSuccess && <Alert severity="success">Policy saved.</Alert>}

      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>Role</TableCell>
            <TableCell>Members</TableCell>
            <TableCell align="right">Actions</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {bindings.map((b) => (
            <TableRow key={b.role}>
              <TableCell>{b.role}</TableCell>
              <TableCell>{b.members.join(', ')}</TableCell>
              <TableCell align="right">
                <IconButton
                  size="small"
                  onClick={() => setBindings((prev) => prev.filter((x) => x.role !== b.role))}
                >
                  <DeleteOutlineIcon fontSize="small" />
                </IconButton>
              </TableCell>
            </TableRow>
          ))}
          {bindings.length === 0 && (
            <TableRow>
              <TableCell colSpan={3} sx={{ color: 'text.secondary' }}>
                No bindings.
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>

      <Stack direction="row" spacing={1} sx={{ mt: 2 }}>
        <TextField label="Role" value={role} onChange={(e) => setRole(e.target.value)} size="small" sx={{ minWidth: 260 }} />
        <TextField
          label="Members (comma separated)"
          value={members}
          onChange={(e) => setMembers(e.target.value)}
          size="small"
          fullWidth
        />
        <Button onClick={add} startIcon={<AddIcon />}>
          Add
        </Button>
      </Stack>
      <Button
        variant="contained"
        sx={{ mt: 2 }}
        disabled={save.isPending}
        onClick={() => save.mutate({ bindings, etag: query.data?.etag, version: query.data?.version ?? 1 })}
      >
        Save policy
      </Button>
    </Box>
  )
}

function AclSection({ bucket }: { bucket: string }) {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [bucketKey] = useState(bucket)
  const query = useQuery({
    queryKey: ['gcp', 'storage', 'bucket', bucketKey, 'acl', accountId],
    queryFn: () => listBucketAcl(bucketKey),
  })
  const [entity, setEntity] = useState('')
  const [role, setRole] = useState('READER')

  const add = useMutation({
    mutationFn: () => insertBucketAcl(bucketKey, entity, role),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'bucket', bucketKey, 'acl'] })
      setEntity('')
    },
  })

  return (
    <Box sx={{ maxWidth: 720 }}>
      <Typography variant="h6" sx={{ mb: 1 }}>
        Bucket ACL
      </Typography>
      {add.isError && <Alert severity="error">Failed to add ACL entry.</Alert>}
      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>Entity</TableCell>
            <TableCell>Role</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {query.data?.items?.map((entry) => (
            <TableRow key={entry.id ?? entry.entity}>
              <TableCell>{entry.entity}</TableCell>
              <TableCell>{entry.role}</TableCell>
            </TableRow>
          ))}
          {(query.data?.items?.length ?? 0) === 0 && (
            <TableRow>
              <TableCell colSpan={2} sx={{ color: 'text.secondary' }}>
                No ACL entries.
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
      <Stack direction="row" spacing={1} sx={{ mt: 2 }}>
        <TextField
          label="Entity (e.g. allUsers)"
          value={entity}
          onChange={(e) => setEntity(e.target.value)}
          size="small"
          fullWidth
        />
        <TextField
          label="Role"
          value={role}
          onChange={(e) => setRole(e.target.value)}
          size="small"
          sx={{ minWidth: 140 }}
        />
        <Button
          startIcon={<AddIcon />}
          disabled={!entity || add.isPending}
          onClick={() => add.mutate()}
        >
          Add
        </Button>
      </Stack>
    </Box>
  )
}
