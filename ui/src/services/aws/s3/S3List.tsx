import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  Badge,
  Box,
  Button,
  ButtonDropdown,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Link,
  Modal,
  SpaceBetween,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import { listBuckets, createBucket, deleteBucket, type Bucket } from '../../../api/s3'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

export function S3List() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [selected, setSelected] = useState<Bucket[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const qc = useQueryClient()
  const navigate = useNavigate()
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['s3', 'buckets'],
    queryFn: () => listBuckets(),
  })

  const createMut = useMutation({
    mutationFn: () => createBucket({ name: newName }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['s3', 'buckets'] })
      notify({ type: 'success', header: 'Bucket created', content: newName })
      setCreateOpen(false)
      setNewName('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create bucket', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: async (buckets: Bucket[]) => {
      for (const bucket of buckets) await deleteBucket(bucket.name)
    },
    onSuccess: (_result, buckets) => {
      void qc.invalidateQueries({ queryKey: ['s3', 'buckets'] })
      notify({
        type: 'success',
        header: `Deleted ${buckets.length} bucket${buckets.length !== 1 ? 's' : ''}`,
      })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const buckets = data?.items ?? []

  const columns: ResourceColumn<Bucket>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (b) => b.name,
      cell: (b) => (
        <Link
          href={`/ui/aws/s3/${encodeURIComponent(b.name)}`}
          onFollow={(event) => {
            event.preventDefault()
            navigate(`/aws/s3/${encodeURIComponent(b.name)}`)
          }}
        >
          {b.name}
        </Link>
      ),
    },
    { id: 'region', header: 'Region', filterLabel: 'Region', filterValue: (b) => b.region, cell: (b) => b.region },
    {
      id: 'versioning',
      header: 'Versioning',
      cell: (b) => (
        <Badge color={b.versioning === 'Enabled' ? 'green' : 'grey'}>{b.versioning}</Badge>
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">S3 buckets</Header>}>
      {error ? (
        <ErrorState header="Failed to load buckets" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="s3"
          favorite={(b) => ({ id: b.name, label: b.name, href: '/aws/s3/' + encodeURIComponent(b.name), type: 'bucket' })}
          items={buckets}
          columns={columns}
          trackBy={(b) => b.name}
          title="Buckets"
          loading={isLoading}
          onRowClick={(b) => navigate(`/aws/s3/${encodeURIComponent(b.name)}`)}
          selectionType="multi"
          selectedItems={selected}
          onSelectionChange={setSelected}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <ButtonDropdown
                items={[{ id: 'delete', text: 'Delete', disabled: selected.length === 0 }]}
                onItemClick={() => setConfirmDelete(true)}
                disabled={selected.length === 0}
              >
                Actions
              </ButtonDropdown>
              <Button variant="primary" onClick={() => setCreateOpen(true)}>
                Create bucket
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No buckets"
          emptyBody="S3 buckets store your objects and files."
        />
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create bucket"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!newName}
                onClick={() => createMut.mutate()}
              >
                Create bucket
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <FormField label="Bucket name" description="Bucket names must be globally unique.">
            <Input
              autoFocus
              value={newName}
              onChange={({ detail }) => setNewName(detail.value)}
              placeholder="my-bucket"
            />
          </FormField>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete buckets"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => deleteMut.mutate(selected)}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Permanently delete {selected.length} bucket{selected.length !== 1 ? 's' : ''}? Each bucket
        must be empty.
      </Modal>
    </ContentLayout>
  )
}
