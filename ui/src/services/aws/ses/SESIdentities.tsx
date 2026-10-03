import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
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
  Modal,
  SpaceBetween,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import {
  listIdentities,
  verifyEmailIdentity,
  deleteIdentity,
  type Identity,
} from '../../../api/ses'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { ResourceDetailsModal } from '../../../components/ResourceDetailsModal'
import { useNotifications } from '../../../components/notifications'

export function SESIdentities() {
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<Identity[]>([])
  const [details, setDetails] = useState<Identity | null>(null)
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [identityInput, setIdentityInput] = useState('')
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['ses', 'identities'],
    queryFn: listIdentities,
  })

  const verify = useMutation({
    mutationFn: () => verifyEmailIdentity(identityInput),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['ses', 'identities'] })
      notify({ type: 'success', header: 'Verification email sent', content: identityInput })
      setCreateOpen(false)
      setIdentityInput('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Verification failed', content: (err as Error).message }),
  })

  const del = useMutation({
    mutationFn: async (identities: Identity[]) => {
      for (const identity of identities) await deleteIdentity(identity.identity)
    },
    onSuccess: (_r, identities) => {
      void qc.invalidateQueries({ queryKey: ['ses', 'identities'] })
      notify({ type: 'success', header: `Deleted ${identities.length} identity(ies)` })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const items = data?.items ?? []

  const columns: ResourceColumn<Identity>[] = [
    {
      id: 'identity',
      header: 'Identity',
      filterLabel: 'Identity',
      filterValue: (i) => i.identity,
      cell: (i) => i.identity,
    },
    {
      id: 'type',
      header: 'Type',
      filterLabel: 'Type',
      filterValue: (i) => i.type,
      cell: (i) => <Badge color="blue">{i.type}</Badge>,
    },
    {
      id: 'actions',
      header: '',
      cell: (i) => (
        <Button variant="inline-link" onClick={() => setDetails(i)}>
          View details
        </Button>
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">SES identities</Header>}>
      {error ? (
        <ErrorState header="Failed to load identities" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="ses"
          favorite={(i) => ({ id: i.identity, label: i.identity, href: '/aws/ses/identities', type: 'identity' })}
          items={items}
          columns={columns}
          trackBy={(i) => i.identity}
          title="Identities"
          loading={isLoading}
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
                Verify identity
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No identities"
          emptyBody="Verify an email address or domain to start sending with SES."
        />
      )}

      <ResourceDetailsModal
        visible={details != null}
        onDismiss={() => setDetails(null)}
        header={details?.identity ?? 'Identity'}
        items={
          details
            ? [
                { label: 'Identity', value: details.identity },
                { label: 'Type', value: details.type },
              ]
            : []
        }
      />

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Verify email identity"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={verify.isPending}
                disabled={!identityInput.trim()}
                onClick={() => verify.mutate()}
              >
                Verify
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <FormField
            label="Email address or domain"
            description="A verification email will be sent to this identity."
          >
            <Input
              autoFocus
              value={identityInput}
              onChange={({ detail }) => setIdentityInput(detail.value)}
              placeholder="user@example.com or example.com"
            />
          </FormField>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete identities"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(false)}>
                Cancel
              </Button>
              <Button variant="primary" loading={del.isPending} onClick={() => del.mutate(selected)}>
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Permanently delete {selected.length} identit{selected.length !== 1 ? 'ies' : 'y'}?
      </Modal>
    </ContentLayout>
  )
}
