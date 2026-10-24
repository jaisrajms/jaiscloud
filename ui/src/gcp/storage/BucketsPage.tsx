import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  Link,
  MenuItem,
  Stack,
  TextField,
  Tooltip,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink } from 'react-router-dom'
import { createBucket, deleteBucket, listBuckets, type Bucket } from '../../api/gcp/storage'
import { useAccount } from '../../context/AccountContext'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'
import { useGcpSnackbar } from '../common/SnackbarProvider'

const LOCATIONS = ['US', 'EU', 'ASIA']

function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** Cloud Storage bucket list — the reference page for the shared scaffold. */
export function BucketsPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const { notify } = useGcpSnackbar()
  const [createOpen, setCreateOpen] = useState(false)
  const [name, setName] = useState('')
  const [location, setLocation] = useState('US')
  const [filter, setFilter] = useState('')

  const buckets = useQuery({
    queryKey: ['gcp', 'storage', 'buckets', accountId],
    queryFn: listBuckets,
  })

  const create = useMutation({
    mutationFn: createBucket,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'buckets'] })
      setCreateOpen(false)
      setName('')
      setLocation('US')
      notify('Bucket created.')
    },
  })

  const remove = useMutation({
    mutationFn: deleteBucket,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'buckets'] })
      notify('Bucket deleted.')
    },
    onError: () =>
      notify('Delete failed. The bucket may not be empty.', { severity: 'error' }),
  })

  const rows = filterRows(buckets.data?.items ?? [], filter, (bucket) => bucket.name)

  const columns: GcpColumn<Bucket>[] = [
    {
      key: 'name',
      header: 'Name',
      render: (bucket) => (
        <Link component={RouterLink} to={`/gcp/storage/buckets/${bucket.name}`}>
          {bucket.name}
        </Link>
      ),
    },
    { key: 'location', header: 'Location', render: (bucket) => bucket.location || '—' },
    {
      key: 'storageClass',
      header: 'Storage class',
      render: (bucket) => bucket.storageClass || 'STANDARD',
    },
    { key: 'created', header: 'Created', render: (bucket) => shortDate(bucket.timeCreated) },
    {
      key: 'versioning',
      header: 'Versioning',
      render: (bucket) => (bucket.versioning ? 'Enabled' : 'Off'),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (bucket) => (
        <Tooltip title="Delete bucket">
          <span>
            <IconButton
              size="small"
              disabled={remove.isPending}
              onClick={() => remove.mutate(bucket.name)}
              aria-label={`Delete ${bucket.name}`}
            >
              <DeleteOutlineIcon fontSize="small" />
            </IconButton>
          </span>
        </Tooltip>
      ),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="storage"
        title="Buckets"
        subtitle={`Cloud Storage · project ${accountId || '—'}`}
        actions={
          <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
            Create bucket
          </Button>
        }
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter buckets"
        onRefresh={() => void buckets.refetch()}
        refreshing={buckets.isFetching}
      />

      <GcpDataTable
        aria-label="Buckets"
        columns={columns}
        rows={rows}
        getRowKey={(bucket) => bucket.name}
        loading={buckets.isLoading}
        error={buckets.isError ? 'Failed to load buckets.' : null}
        emptyMessage={filter ? 'No buckets match the filter.' : 'No buckets in this project.'}
      />

      <Dialog open={createOpen} onClose={() => setCreateOpen(false)} fullWidth maxWidth="xs">
        <DialogTitle>Create bucket</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ mt: 1 }}>
            {create.isError && <Alert severity="error">Could not create the bucket.</Alert>}
            <TextField
              autoFocus
              label="Name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="my-bucket"
              fullWidth
            />
            <TextField
              select
              label="Location"
              value={location}
              onChange={(e) => setLocation(e.target.value)}
              fullWidth
            >
              {LOCATIONS.map((loc) => (
                <MenuItem key={loc} value={loc}>
                  {loc}
                </MenuItem>
              ))}
            </TextField>
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={!name || create.isPending}
            onClick={() => create.mutate({ name, location })}
          >
            Create
          </Button>
        </DialogActions>
      </Dialog>
    </Stack>
  )
}
