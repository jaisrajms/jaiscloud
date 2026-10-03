import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
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
import { listLogGroups, createLogGroup, deleteLogGroup, type LogGroup } from '../../../api/logs'
import { formatDate } from '../../../lib/date'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

function fmtBytes(n: number): string {
  if (n === 0) return '0 B'
  if (n < 1024) return `${n} B`
  if (n < 1048576) return `${(n / 1024).toFixed(1)} KB`
  if (n < 1073741824) return `${(n / 1048576).toFixed(1)} MB`
  return `${(n / 1073741824).toFixed(2)} GB`
}

export function LogGroupList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [createName, setCreateName] = useState('')
  const [selected, setSelected] = useState<LogGroup[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const qc = useQueryClient()
  const navigate = useNavigate()
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['logs', 'groups'],
    queryFn: () => listLogGroups(),
  })

  const deleteMut = useMutation({
    mutationFn: async (groups: LogGroup[]) => {
      for (const group of groups) await deleteLogGroup(group.name)
    },
    onSuccess: (_result, groups) => {
      void qc.invalidateQueries({ queryKey: ['logs', 'groups'] })
      notify({
        type: 'success',
        header: `Deleted ${groups.length} log group${groups.length !== 1 ? 's' : ''}`,
      })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const createMut = useMutation({
    mutationFn: (name: string) => createLogGroup(name),
    onSuccess: (_result, name) => {
      void qc.invalidateQueries({ queryKey: ['logs', 'groups'] })
      notify({ type: 'success', header: 'Log group created', content: name })
      setCreateOpen(false)
      setCreateName('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create log group', content: (err as Error).message }),
  })

  const groups = data?.items ?? []

  const columns: ResourceColumn<LogGroup>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (g) => g.name,
      cell: (g) => (
        <Link
          href={`/ui/aws/logs/groups/${encodeURIComponent(g.name)}`}
          onFollow={(event) => {
            event.preventDefault()
            navigate(`/aws/logs/groups/${encodeURIComponent(g.name)}`)
          }}
        >
          {g.name}
        </Link>
      ),
    },
    {
      id: 'retention',
      header: 'Retention',
      filterLabel: 'Retention',
      filterValue: (g) => (g.retentionDays ? `${g.retentionDays} days` : 'Never expire'),
      cell: (g) => (g.retentionDays ? `${g.retentionDays} days` : 'Never expire'),
    },
    { id: 'stored', header: 'Stored', cell: (g) => fmtBytes(g.storedBytes) },
    { id: 'created', header: 'Created', cell: (g) => formatDate(g.createdAt) },
  ]

  return (
    <ContentLayout header={<Header variant="h1">CloudWatch log groups</Header>}>
      {error ? (
        <ErrorState header="Failed to load log groups" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="logs"
          favorite={(g) => ({ id: g.name, label: g.name, href: '/aws/logs/groups/' + encodeURIComponent(g.name), type: 'log group' })}
          items={groups}
          columns={columns}
          trackBy={(g) => g.arn || g.name}
          title="Log groups"
          loading={isLoading}
          onRowClick={(g) => navigate(`/aws/logs/groups/${encodeURIComponent(g.name)}`)}
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
                Create log group
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No log groups"
          emptyBody="CloudWatch Logs lets you monitor, store, and access log files from your resources."
        />
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create log group"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!createName.trim()}
                onClick={() => createMut.mutate(createName.trim())}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <FormField
            label="Log group name"
            description="Use the name your service writes to, e.g. /aws/lambda/my-function."
          >
            <Input
              autoFocus
              value={createName}
              onChange={({ detail }) => setCreateName(detail.value)}
              placeholder="/aws/lambda/my-function"
            />
          </FormField>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete log groups"
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
        Permanently delete {selected.length} log group{selected.length !== 1 ? 's' : ''} and all of
        their log streams? This action cannot be undone.
      </Modal>
    </ContentLayout>
  )
}
