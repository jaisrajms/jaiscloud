import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  Box,
  Button,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Link,
  Modal,
  SpaceBetween,
  Textarea,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import { listSecrets, createSecret, deleteSecret, type Secret } from '../../../api/secretsmanager'
import { formatDate } from '../../../lib/date'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

export function SecretsList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newValue, setNewValue] = useState('')
  const [newDesc, setNewDesc] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<Secret | null>(null)
  const qc = useQueryClient()
  const navigate = useNavigate()
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['secretsmanager', 'secrets'],
    queryFn: () => listSecrets(),
  })

  const createMut = useMutation({
    mutationFn: () =>
      createSecret({
        name: newName,
        secretString: newValue || undefined,
        description: newDesc || undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['secretsmanager', 'secrets'] })
      notify({ type: 'success', header: 'Secret created', content: newName })
      setCreateOpen(false)
      setNewName('')
      setNewValue('')
      setNewDesc('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create secret', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteSecret(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['secretsmanager', 'secrets'] })
      notify({ type: 'success', header: 'Secret deleted' })
      setConfirmDelete(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const secrets = data?.items ?? []

  const columns: ResourceColumn<Secret>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (s) => s.name,
      cell: (s) => (
        <Link
          href={`/ui/aws/secretsmanager/${encodeURIComponent(s.name)}`}
          onFollow={(event) => {
            event.preventDefault()
            navigate(encodeURIComponent(s.name))
          }}
        >
          {s.name}
        </Link>
      ),
    },
    {
      id: 'description',
      header: 'Description',
      filterLabel: 'Description',
      filterValue: (s) => s.description ?? '',
      cell: (s) => s.description || '—',
    },
    {
      id: 'lastChanged',
      header: 'Last changed',
      cell: (s) => formatDate(s.lastChangedDate),
    },
    {
      id: 'actions',
      header: 'Actions',
      cell: (s) => (
        <div onClick={(event) => event.stopPropagation()}>
          <Button variant="link" onClick={() => setConfirmDelete(s)}>
            Delete
          </Button>
        </div>
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">Secrets Manager</Header>}>
      {error ? (
        <ErrorState header="Failed to load secrets" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="secretsmanager"
          favorite={(s) => ({ id: s.name, label: s.name, href: '/aws/secretsmanager/' + encodeURIComponent(s.name), type: 'secret' })}
          items={secrets}
          columns={columns}
          trackBy={(s) => s.arn}
          title="Secrets"
          loading={isLoading}
          onRowClick={(s) => navigate(encodeURIComponent(s.name))}
          actions={
            <Button variant="primary" onClick={() => setCreateOpen(true)}>
              Create secret
            </Button>
          }
          emptyTitle="No secrets"
          emptyBody="Store and retrieve database credentials, API keys, and other secrets."
        />
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create secret"
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
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="l">
            <FormField label="Secret name">
              <Input
                autoFocus
                value={newName}
                onChange={({ detail }) => setNewName(detail.value)}
                placeholder="my-secret"
              />
            </FormField>
            <FormField label="Secret value" description="Optional">
              <Textarea
                value={newValue}
                onChange={({ detail }) => setNewValue(detail.value)}
                placeholder='{"username":"admin","password":"secret"}'
              />
            </FormField>
            <FormField label="Description" description="Optional">
              <Input
                value={newDesc}
                onChange={({ detail }) => setNewDesc(detail.value)}
                placeholder="Database credentials"
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete !== null}
        onDismiss={() => setConfirmDelete(null)}
        header="Delete secret"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => deleteMut.mutate(confirmDelete?.name ?? '')}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        This will permanently delete <b>{confirmDelete?.name}</b> and all its versions.
      </Modal>
    </ContentLayout>
  )
}
