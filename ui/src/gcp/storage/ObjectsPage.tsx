import { useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  Drawer,
  FormControlLabel,
  IconButton,
  Link,
  Stack,
  Switch,
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import CloudUploadIcon from '@mui/icons-material/CloudUpload'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import DownloadIcon from '@mui/icons-material/Download'
import FolderOutlinedIcon from '@mui/icons-material/FolderOutlined'
import RestoreIcon from '@mui/icons-material/Restore'
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined'
import { Link as RouterLink, useParams, useSearchParams } from 'react-router-dom'
import {
  deleteObject,
  downloadObjectUrl,
  getObject,
  listObjectAcl,
  listObjectVersions,
  listObjects,
  patchObject,
  restoreObject,
  uploadObject,
  type GCSObject,
} from '../../api/gcp/storage'
import { useAccount } from '../../context/AccountContext'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

function formatSize(value?: string): string {
  if (!value) return '—'
  const n = Number(value)
  if (!Number.isFinite(n)) return value
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`
  return `${(n / (1024 * 1024)).toFixed(1)} MiB`
}

const DRAWER_WIDTH = 420

/** A folder placeholder (common prefix) or a real object row. */
type ObjectRow =
  | { kind: 'prefix'; name: string }
  | { kind: 'object'; object: GCSObject }

/** Object browser for one Cloud Storage bucket. */
export function ObjectsPage() {
  const { bucket = '' } = useParams()
  const [params, setParams] = useSearchParams()
  const prefix = params.get('prefix') ?? ''
  const versions = params.get('versions') === '1'
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const fileInput = useRef<HTMLInputElement>(null)
  const [detail, setDetail] = useState<{ name: string; generation?: string } | null>(null)
  const [filter, setFilter] = useState('')
  const [selectedKeys, setSelectedKeys] = useState<string[]>([])

  const objects = useQuery({
    queryKey: ['gcp', 'storage', 'objects', accountId, bucket, prefix, versions],
    queryFn: () =>
      versions
        ? listObjectVersions(bucket, prefix || undefined)
        : listObjects(bucket, { prefix: prefix || undefined, delimiter: '/' }),
    enabled: bucket !== '',
  })

  const upload = useMutation({
    mutationFn: (file: File) => uploadObject(bucket, file.name, file),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'objects'] }),
  })

  const remove = useMutation({
    mutationFn: (o: { name: string; generation?: string }) =>
      deleteObject(bucket, o.name, o.generation),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'objects'] }),
  })

  const restore = useMutation({
    mutationFn: (o: { name: string; generation: string }) =>
      restoreObject(bucket, o.name, o.generation),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'objects'] }),
  })

  const refresh = () => {
    void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'objects'] })
    void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'object'] })
  }

  const allRows: ObjectRow[] = [
    ...(!versions
      ? (objects.data?.prefixes ?? []).map((name): ObjectRow => ({ kind: 'prefix', name }))
      : []),
    ...(objects.data?.items ?? []).map((object): ObjectRow => ({ kind: 'object', object })),
  ]
  const rows = filterRows(allRows, filter, (row) =>
    row.kind === 'prefix'
      ? row.name
      : `${row.object.name} ${row.object.contentType ?? ''}`,
  )

  const columns: GcpColumn<ObjectRow>[] = [
    {
      key: 'name',
      header: 'Name',
      sortable: true,
      sortValue: (row) => (row.kind === 'prefix' ? row.name : row.object.name),
      render: (row) => {
        if (row.kind === 'prefix') {
          return (
            <Stack direction="row" spacing={1} sx={{ alignItems: 'center' }}>
              <FolderOutlinedIcon fontSize="small" color="action" />
              <span>{row.name}</span>
            </Stack>
          )
        }
        const object = row.object
        return (
          <Stack direction="row" spacing={1} sx={{ alignItems: 'center', minWidth: 0 }}>
            <span style={{ wordBreak: 'break-all' }}>{object.name}</span>
            {object.temporaryHold && <Chip size="small" label="temp hold" />}
            {object.eventBasedHold && <Chip size="small" label="event hold" />}
            {object.timeDeleted && <Chip size="small" color="default" label="noncurrent" />}
          </Stack>
        )
      },
    },
    ...(versions
      ? [
          {
            key: 'generation',
            header: 'Generation',
            sortable: true,
            sortValue: (row: ObjectRow) =>
              row.kind === 'object' ? row.object.generation ?? '' : '',
            render: (row: ObjectRow) =>
              row.kind === 'object' ? row.object.generation || '—' : '—',
          } satisfies GcpColumn<ObjectRow>,
        ]
      : []),
    {
      key: 'size',
      header: 'Size',
      sortable: true,
      sortValue: (row) => (row.kind === 'object' ? Number(row.object.size ?? 0) : -1),
      render: (row) => (row.kind === 'object' ? formatSize(row.object.size) : '—'),
    },
    {
      key: 'type',
      header: 'Type',
      sortable: true,
      sortValue: (row) => (row.kind === 'object' ? row.object.contentType ?? '' : 'Folder'),
      render: (row) => (row.kind === 'object' ? row.object.contentType || '—' : 'Folder'),
    },
    {
      key: 'updated',
      header: 'Updated',
      sortable: true,
      sortValue: (row) =>
        row.kind === 'object' ? row.object.updated ?? row.object.timeCreated ?? '' : '',
      render: (row) =>
        row.kind === 'object' ? row.object.updated || row.object.timeCreated || '—' : '—',
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (row) => {
        if (row.kind === 'prefix') return null
        const object = row.object
        const dead = Boolean(object.timeDeleted)
        return (
          <Stack direction="row" spacing={0.5} sx={{ justifyContent: 'flex-end' }}>
            {!dead && (
              <IconButton
                size="small"
                component="a"
                href={downloadObjectUrl(bucket, object.name, object.generation)}
                aria-label={`Download ${object.name}`}
              >
                <DownloadIcon fontSize="small" />
              </IconButton>
            )}
            {dead && object.generation && (
              <IconButton
                size="small"
                aria-label={`Restore ${object.name}`}
                onClick={() => restore.mutate({ name: object.name, generation: object.generation! })}
              >
                <RestoreIcon fontSize="small" />
              </IconButton>
            )}
            <IconButton
              size="small"
              aria-label={`Delete ${object.name}`}
              onClick={() => remove.mutate({ name: object.name, generation: object.generation })}
            >
              <DeleteOutlineIcon fontSize="small" />
            </IconButton>
          </Stack>
        )
      },
    },
  ]

  return (
    <Box>
      <GcpPageHeader
        id="storage"
        title={bucket}
        backTo="/gcp/storage/buckets"
        backAriaLabel="Back to buckets"
        actions={
          <>
            <Button
              component={RouterLink}
              to={`/gcp/storage/buckets/${encodeURIComponent(bucket)}/settings`}
              startIcon={<SettingsOutlinedIcon />}
            >
              Settings
            </Button>
            <Button
              variant="contained"
              startIcon={<CloudUploadIcon />}
              disabled={upload.isPending}
              onClick={() => fileInput.current?.click()}
            >
              Upload
            </Button>
          </>
        }
      >
        <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mt: 1 }}>
          <Link component={RouterLink} to={`/gcp/storage/buckets/${encodeURIComponent(bucket)}`}>
            {prefix ? 'Buckets / ' + prefix : 'Objects'}
          </Link>
          <Chip
            size="small"
            label="Versions"
            color={versions ? 'primary' : 'default'}
            onClick={() => {
              const next = new URLSearchParams()
              if (prefix) next.set('prefix', prefix)
              if (!versions) next.set('versions', '1')
              setParams(next)
            }}
          />
        </Stack>
      </GcpPageHeader>

      <input
        ref={fileInput}
        type="file"
        hidden
        onChange={(e) => {
          const file = e.target.files?.[0]
          if (file) upload.mutate(file)
          e.target.value = ''
        }}
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter objects"
        onRefresh={refresh}
        refreshing={objects.isFetching}
      />

      {upload.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Upload failed.
        </Alert>
      )}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed. The object may be held or retention-protected.
        </Alert>
      )}

      <GcpDataTable
        aria-label="Objects"
        columns={columns}
        rows={rows}
        getRowKey={(row) =>
          row.kind === 'prefix'
            ? `prefix:${row.name}`
            : `object:${row.object.name}#${row.object.generation ?? ''}`
        }
        loading={objects.isLoading}
        error={objects.isError ? 'Failed to list objects.' : null}
        emptyMessage={
          filter
            ? 'No objects match the filter.'
            : `No objects${prefix ? ` under ${prefix}` : ''}.`
        }
        selectable
        selectedKeys={selectedKeys}
        onSelectionChange={setSelectedKeys}
        onRowClick={(row) => {
          if (row.kind === 'prefix') setParams({ prefix: row.name })
          else setDetail({ name: row.object.name, generation: row.object.generation })
        }}
      />

      <ObjectDrawer
        bucket={bucket}
        selection={detail}
        onClose={() => setDetail(null)}
        onChanged={refresh}
      />
    </Box>
  )
}

interface ObjectDrawerProps {
  bucket: string
  selection: { name: string; generation?: string } | null
  onClose: () => void
  onChanged: () => void
}

function ObjectDrawer({ bucket, selection, onClose, onChanged }: ObjectDrawerProps) {
  const { accountId } = useAccount()
  const open = selection !== null
  const name = selection?.name ?? ''
  const generation = selection?.generation

  const detail = useQuery({
    queryKey: ['gcp', 'storage', 'object', accountId, bucket, name, generation],
    queryFn: () => getObject(bucket, name, generation),
    enabled: open,
  })

  const acl = useQuery({
    queryKey: ['gcp', 'storage', 'objectAcl', accountId, bucket, name],
    queryFn: () => listObjectAcl(bucket, name),
    enabled: open,
  })

  const patch = useMutation({
    mutationFn: (body: { temporaryHold?: boolean; eventBasedHold?: boolean }) =>
      patchObject(bucket, name, body, generation),
    onSuccess: onChanged,
  })

  const obj: GCSObject | undefined = detail.data

  return (
    <Drawer anchor="right" open={open} onClose={onClose}>
      <Box sx={{ width: { xs: '100vw', sm: DRAWER_WIDTH }, maxWidth: '100%', p: 2 }} role="presentation">
        <Stack direction="row" sx={{ alignItems: 'center', mb: 1 }}>
          <Typography variant="h6" sx={{ flexGrow: 1, wordBreak: 'break-all' }}>
            {name}
          </Typography>
          <IconButton onClick={onClose} aria-label="Close">
            <ArrowBackIcon />
          </IconButton>
        </Stack>
        <Divider sx={{ mb: 2 }} />

        {detail.isLoading && <CircularProgress size={20} />}
        {detail.isError && <Alert severity="error">Failed to load object metadata.</Alert>}
        {patch.isError && <Alert severity="error">Update failed (retention may be active).</Alert>}

        {obj && (
          <Stack spacing={1.5}>
            <Detail label="Generation" value={obj.generation} />
            <Detail label="Metageneration" value={obj.metageneration} />
            <Detail label="Size" value={formatSize(obj.size)} />
            <Detail label="Content type" value={obj.contentType} />
            <Detail label="Storage class" value={obj.storageClass} />
            <Detail label="Created" value={obj.timeCreated} />
            <Detail label="Updated" value={obj.updated} />
            {obj.timeDeleted && <Detail label="Deleted" value={obj.timeDeleted} />}
            {obj.retentionExpirationTime && (
              <Detail label="Retention expires" value={obj.retentionExpirationTime} />
            )}
            <Detail label="MD5" value={obj.md5Hash} />

            <Divider />
            <Typography variant="subtitle2">Holds</Typography>
            <FormControlLabel
              control={
                <Switch
                  checked={Boolean(obj.temporaryHold)}
                  disabled={patch.isPending || Boolean(obj.timeDeleted)}
                  onChange={(e) => patch.mutate({ temporaryHold: e.target.checked })}
                />
              }
              label="Temporary hold"
            />
            <FormControlLabel
              control={
                <Switch
                  checked={Boolean(obj.eventBasedHold)}
                  disabled={patch.isPending || Boolean(obj.timeDeleted)}
                  onChange={(e) => patch.mutate({ eventBasedHold: e.target.checked })}
                />
              }
              label="Event-based hold"
            />

            {obj.metadata && Object.keys(obj.metadata).length > 0 && (
              <>
                <Divider />
                <Typography variant="subtitle2">Custom metadata</Typography>
                {Object.entries(obj.metadata).map(([k, v]) => (
                  <Detail key={k} label={k} value={v} />
                ))}
              </>
            )}

            <Divider />
            <Typography variant="subtitle2">Object ACL</Typography>
            {acl.data?.items?.map((entry) => (
              <Detail key={entry.id ?? entry.entity} label={entry.entity} value={entry.role} />
            ))}
          </Stack>
        )}
      </Box>
    </Drawer>
  )
}

function Detail({ label, value }: { label: string; value?: string }) {
  return (
    <Stack direction="row" spacing={1}>
      <Typography variant="body2" color="text.secondary" sx={{ minWidth: 130 }}>
        {label}
      </Typography>
      <Typography variant="body2" sx={{ wordBreak: 'break-all' }}>
        {value || '—'}
      </Typography>
    </Stack>
  )
}
