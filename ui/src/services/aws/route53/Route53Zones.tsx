import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Badge,
  Box,
  Button,
  ButtonDropdown,
  Container,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Modal,
  SpaceBetween,
  Table,
} from '@cloudscape-design/components'
import { CopyText } from '../../../components/CopyText'
import { ErrorState } from '../../../components/ErrorState'
import type { TableProps } from '@cloudscape-design/components'
import {
  listZones,
  createZone,
  deleteZone,
  listRecords,
  type HostedZone,
  type RecordSet,
} from '../../../api/route53'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

const recordColumns: TableProps.ColumnDefinition<RecordSet>[] = [
  { id: 'name', header: 'Name', cell: (r) => r.name },
  { id: 'type', header: 'Type', cell: (r) => <Badge color="blue">{r.type}</Badge> },
  { id: 'ttl', header: 'TTL', cell: (r) => r.ttl },
  { id: 'records', header: 'Records', cell: (r) => <Box variant="code">{r.records?.join(', ') || '—'}</Box> },
]

export function Route53Zones() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [zoneName, setZoneName] = useState('')
  const [selected, setSelected] = useState<HostedZone[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [expandedId, setExpandedId] = useState<string | null>(null)
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['route53', 'zones'],
    queryFn: listZones,
  })

  const recordsQuery = useQuery({
    queryKey: ['route53', 'records', expandedId],
    queryFn: () => listRecords(expandedId!),
    enabled: !!expandedId,
  })

  const create = useMutation({
    mutationFn: (name: string) => createZone(name),
    onSuccess: (_r, name) => {
      void qc.invalidateQueries({ queryKey: ['route53', 'zones'] })
      notify({ type: 'success', header: 'Hosted zone creating', content: name })
      setCreateOpen(false)
      setZoneName('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const del = useMutation({
    mutationFn: async (ids: string[]) => {
      for (const id of ids) await deleteZone(id)
    },
    onSuccess: (_r, ids) => {
      void qc.invalidateQueries({ queryKey: ['route53', 'zones'] })
      notify({ type: 'success', header: `Deleted ${ids.length} hosted zone(s)` })
      setExpandedId(null)
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const items = data?.items ?? []
  const expandedZone = items.find((z) => z.id === expandedId)

  const columns: ResourceColumn<HostedZone>[] = [
    { id: 'name', header: 'Name', filterLabel: 'Name', filterValue: (z) => z.name, cell: (z) => z.name },
    { id: 'id', header: 'ID', cell: (z) => <CopyText value={z.id} label="hosted zone ID" /> },
    {
      id: 'private',
      header: 'Visibility',
      filterLabel: 'Visibility',
      filterValue: (z) => (z.private ? 'Private' : 'Public'),
      cell: (z) => <Badge color={z.private ? 'blue' : 'grey'}>{z.private ? 'Private' : 'Public'}</Badge>,
    },
    { id: 'recordCount', header: 'Records', cell: (z) => z.recordCount },
  ]

  return (
    <ContentLayout header={<Header variant="h1">Route 53 hosted zones</Header>}>
      {error ? (
        <ErrorState header="Failed to load hosted zones" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <SpaceBetween size="l">
          <ResourceTable
            favoriteService="route53"
            favorite={(z) => ({ id: z.id, label: z.name, href: '/aws/route53/zones', type: 'hosted zone' })}
            items={items}
            columns={columns}
            trackBy={(z) => z.id}
            title="Hosted zones"
            description="metadata only"
            loading={isLoading}
            onRowClick={(z) => setExpandedId(expandedId === z.id ? null : z.id)}
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
                  Create zone
                </Button>
              </SpaceBetween>
            }
            emptyTitle="No hosted zones"
            emptyBody="Create a hosted zone to manage DNS records."
          />

          {expandedZone && (
            <Container
              header={
                <Header
                  variant="h2"
                  description={expandedZone.id}
                  actions={
                    <Button variant="link" onClick={() => setExpandedId(null)}>
                      Close
                    </Button>
                  }
                >
                  Record sets
                </Header>
              }
            >
              <Table
                items={recordsQuery.data?.items ?? []}
                columnDefinitions={recordColumns}
                loading={recordsQuery.isLoading}
                loadingText="Loading records"
                trackBy={(r) => `${r.name}/${r.type}`}
                empty={<Box textAlign="center">No records found</Box>}
              />
            </Container>
          )}
        </SpaceBetween>
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create hosted zone"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={create.isPending}
                disabled={!zoneName.trim()}
                onClick={() => create.mutate(zoneName)}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <FormField label="Domain name" description="The domain name the hosted zone will manage.">
            <Input
              autoFocus
              value={zoneName}
              onChange={({ detail }) => setZoneName(detail.value)}
              placeholder="example.com"
            />
          </FormField>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete hosted zones"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={del.isPending}
                onClick={() => del.mutate(selected.map((z) => z.id))}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Permanently delete {selected.length} hosted zone{selected.length !== 1 ? 's' : ''}?
      </Modal>
    </ContentLayout>
  )
}
