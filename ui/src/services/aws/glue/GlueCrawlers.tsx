import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Box,
  Button,
  ButtonDropdown,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Modal,
  SpaceBetween,
  StatusIndicator,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import {
  listCrawlers,
  createCrawler,
  deleteCrawler,
  startCrawler,
  type Crawler,
} from '../../../api/glue'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { ResourceDetailsModal } from '../../../components/ResourceDetailsModal'
import { useNotifications } from '../../../components/notifications'

const EMPTY_FORM = { name: '', role: '', databaseName: '', s3Targets: '' }

export function GlueCrawlers() {
  const qc = useQueryClient()
  const { notify } = useNotifications()
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<Crawler[]>([])
  const [details, setDetails] = useState<Crawler | null>(null)
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [form, setForm] = useState(EMPTY_FORM)

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['glue', 'crawlers'],
    queryFn: () => listCrawlers(),
  })

  const createMut = useMutation({
    mutationFn: () =>
      createCrawler({
        name: form.name,
        role: form.role || undefined,
        databaseName: form.databaseName || undefined,
        s3Targets: form.s3Targets
          ? form.s3Targets
              .split(',')
              .map((s) => s.trim())
              .filter(Boolean)
          : undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'crawlers'] })
      notify({ type: 'success', header: 'Crawler created', content: form.name })
      setCreateOpen(false)
      setForm(EMPTY_FORM)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: async (crawlers: Crawler[]) => {
      for (const crawler of crawlers) await deleteCrawler(crawler.name)
    },
    onSuccess: (_result, crawlers) => {
      void qc.invalidateQueries({ queryKey: ['glue', 'crawlers'] })
      notify({
        type: 'success',
        header: `Deleted ${crawlers.length} crawler${crawlers.length !== 1 ? 's' : ''}`,
      })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const startMut = useMutation({
    mutationFn: async (crawlers: Crawler[]) => {
      for (const crawler of crawlers) await startCrawler(crawler.name)
    },
    onSuccess: (_result, crawlers) => {
      void qc.invalidateQueries({ queryKey: ['glue', 'crawlers'] })
      notify({
        type: 'success',
        header: `Started ${crawlers.length} crawler${crawlers.length !== 1 ? 's' : ''}`,
      })
      setSelected([])
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Start failed', content: (err as Error).message }),
  })

  const crawlers = data?.items ?? []
  const anyRunning = selected.some((c) => c.state === 'RUNNING')

  const columns: ResourceColumn<Crawler>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (c) => c.name,
      cell: (c) => <Box fontWeight="bold">{c.name}</Box>,
    },
    {
      id: 'role',
      header: 'Role',
      filterLabel: 'Role',
      filterValue: (c) => c.role ?? '',
      cell: (c) => c.role || '—',
    },
    {
      id: 'state',
      header: 'State',
      filterLabel: 'State',
      filterValue: (c) => c.state ?? '',
      cell: (c) => <StatusIndicator type={resourceStatus(c.state)}>{c.state || '—'}</StatusIndicator>,
    },
    {
      id: 'actions',
      header: '',
      cell: (c) => (
        <Button variant="inline-link" onClick={() => setDetails(c)}>
          View details
        </Button>
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">Glue crawlers</Header>}>
      {error ? (
        <ErrorState header="Failed to load crawlers" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="glue"
          favorite={(c) => ({ id: c.name, label: c.name, href: '/aws/glue/crawlers', type: 'crawler' })}
          items={crawlers}
          columns={columns}
          trackBy={(c) => c.name}
          title="Crawlers"
          loading={isLoading}
          selectionType="multi"
          selectedItems={selected}
          onSelectionChange={setSelected}
          actions={
            <SpaceBetween direction="horizontal" size="xs">
              <ButtonDropdown
                items={[
                  { id: 'start', text: 'Start', disabled: selected.length === 0 || anyRunning },
                  { id: 'delete', text: 'Delete', disabled: selected.length === 0 },
                ]}
                onItemClick={({ detail }) => {
                  if (detail.id === 'start') startMut.mutate(selected)
                  else if (detail.id === 'delete') setConfirmDelete(true)
                }}
                disabled={selected.length === 0}
              >
                Actions
              </ButtonDropdown>
              <Button variant="primary" onClick={() => setCreateOpen(true)}>
                Create crawler
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No crawlers"
          emptyBody="Create a crawler to discover and catalog data from S3."
        />
      )}

      <ResourceDetailsModal
        visible={details != null}
        onDismiss={() => setDetails(null)}
        header={details?.name ?? 'Crawler'}
        items={
          details
            ? [
                { label: 'Name', value: details.name },
                { label: 'Role', value: details.role || '—' },
                {
                  label: 'State',
                  value: (
                    <StatusIndicator type={resourceStatus(details.state)}>
                      {details.state || '—'}
                    </StatusIndicator>
                  ),
                },
              ]
            : []
        }
      />

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create crawler"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!form.name.trim()}
                onClick={() => createMut.mutate()}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <FormField label="Name" constraintText="Required">
              <Input
                value={form.name}
                placeholder="my-crawler"
                onChange={({ detail }) => setForm({ ...form, name: detail.value })}
              />
            </FormField>
            <FormField label="IAM role">
              <Input
                value={form.role}
                placeholder="AWSGlueServiceRole"
                onChange={({ detail }) => setForm({ ...form, role: detail.value })}
              />
            </FormField>
            <FormField label="Target database">
              <Input
                value={form.databaseName}
                placeholder="my_database"
                onChange={({ detail }) => setForm({ ...form, databaseName: detail.value })}
              />
            </FormField>
            <FormField label="S3 paths" description="Comma-separated list of S3 paths.">
              <Input
                value={form.s3Targets}
                placeholder="s3://bucket/prefix"
                onChange={({ detail }) => setForm({ ...form, s3Targets: detail.value })}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete crawlers"
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
        Delete {selected.length} crawler{selected.length !== 1 ? 's' : ''}? This action cannot be
        undone.
      </Modal>
    </ContentLayout>
  )
}
