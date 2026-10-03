import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Box,
  Button,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Modal,
  Select,
  SpaceBetween,
  StatusIndicator,
} from '@cloudscape-design/components'
import { CopyText } from '../../../components/CopyText'
import { ErrorState } from '../../../components/ErrorState'
import {
  listKeys,
  createKey,
  enableKey,
  disableKey,
  scheduleKeyDeletion,
  cancelKeyDeletion,
  type KMSKey,
} from '../../../api/kms'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { ResourceDetailsModal } from '../../../components/ResourceDetailsModal'
import { formatDate } from '../../../lib/date'
import { useNotifications } from '../../../components/notifications'

const KEY_USAGES = [
  { value: 'ENCRYPT_DECRYPT', label: 'ENCRYPT_DECRYPT' },
  { value: 'SIGN_VERIFY', label: 'SIGN_VERIFY' },
  { value: 'GENERATE_VERIFY_MAC', label: 'GENERATE_VERIFY_MAC' },
]

const KEY_SPECS = [
  { value: 'SYMMETRIC_DEFAULT', label: 'SYMMETRIC_DEFAULT' },
  { value: 'RSA_2048', label: 'RSA_2048' },
  { value: 'RSA_4096', label: 'RSA_4096' },
  { value: 'ECC_NIST_P256', label: 'ECC_NIST_P256' },
  { value: 'HMAC_256', label: 'HMAC_256' },
]

export function KMSList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newDesc, setNewDesc] = useState('')
  const [newUsage, setNewUsage] = useState('ENCRYPT_DECRYPT')
  const [newSpec, setNewSpec] = useState('SYMMETRIC_DEFAULT')
  const [deleteKey, setDeleteKey] = useState<KMSKey | null>(null)
  const [details, setDetails] = useState<KMSKey | null>(null)
  const [deleteDays, setDeleteDays] = useState(30)
  const qc = useQueryClient()
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['kms', 'keys'],
    queryFn: () => listKeys(),
  })

  const createMut = useMutation({
    mutationFn: () =>
      createKey({ description: newDesc || undefined, keyUsage: newUsage, keySpec: newSpec }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['kms', 'keys'] })
      notify({ type: 'success', header: 'Key created' })
      setCreateOpen(false)
      setNewDesc('')
      setNewUsage('ENCRYPT_DECRYPT')
      setNewSpec('SYMMETRIC_DEFAULT')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create key', content: (err as Error).message }),
  })

  const enableMut = useMutation({
    mutationFn: (keyId: string) => enableKey(keyId),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['kms', 'keys'] })
      notify({ type: 'success', header: 'Key enabled' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Enable failed', content: (err as Error).message }),
  })

  const disableMut = useMutation({
    mutationFn: (keyId: string) => disableKey(keyId),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['kms', 'keys'] })
      notify({ type: 'success', header: 'Key disabled' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Disable failed', content: (err as Error).message }),
  })

  const scheduleMut = useMutation({
    mutationFn: ({ keyId, days }: { keyId: string; days: number }) =>
      scheduleKeyDeletion(keyId, days),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['kms', 'keys'] })
      notify({ type: 'success', header: 'Key deletion scheduled' })
      setDeleteKey(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Scheduling failed', content: (err as Error).message }),
  })

  const cancelMut = useMutation({
    mutationFn: (keyId: string) => cancelKeyDeletion(keyId),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['kms', 'keys'] })
      notify({ type: 'success', header: 'Deletion canceled' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Cancel failed', content: (err as Error).message }),
  })

  const keys = data?.items ?? []

  const columns: ResourceColumn<KMSKey>[] = [
    {
      id: 'keyId',
      header: 'Key ID',
      filterLabel: 'Key ID',
      filterValue: (k) => k.keyId,
      cell: (k) => <CopyText value={k.keyId} label="key ID" />,
    },
    {
      id: 'description',
      header: 'Description',
      filterLabel: 'Description',
      filterValue: (k) => k.description ?? '',
      cell: (k) => k.description || '—',
    },
    {
      id: 'keyUsage',
      header: 'Usage',
      filterLabel: 'Usage',
      filterValue: (k) => k.keyUsage,
      cell: (k) => k.keyUsage,
    },
    {
      id: 'keySpec',
      header: 'Spec',
      filterLabel: 'Spec',
      filterValue: (k) => k.keySpec,
      cell: (k) => k.keySpec,
    },
    {
      id: 'keyState',
      header: 'State',
      filterLabel: 'State',
      filterValue: (k) => k.keyState,
      cell: (k) => <StatusIndicator type={resourceStatus(k.keyState)}>{k.keyState}</StatusIndicator>,
    },
    {
      id: 'actions',
      header: 'Actions',
      cell: (k) => (
        <div onClick={(event) => event.stopPropagation()}>
          <SpaceBetween direction="horizontal" size="xs">
            <Button variant="link" onClick={() => setDetails(k)}>
              View details
            </Button>
            {k.keyState === 'Disabled' && (
              <Button variant="link" onClick={() => enableMut.mutate(k.keyId)}>
                Enable
              </Button>
            )}
            {k.keyState === 'Enabled' && (
              <Button variant="link" onClick={() => disableMut.mutate(k.keyId)}>
                Disable
              </Button>
            )}
            {k.keyState === 'PendingDeletion' && (
              <Button variant="link" onClick={() => cancelMut.mutate(k.keyId)}>
                Cancel deletion
              </Button>
            )}
            {k.keyState !== 'PendingDeletion' && (
              <Button variant="link" onClick={() => setDeleteKey(k)}>
                Schedule deletion
              </Button>
            )}
          </SpaceBetween>
        </div>
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">KMS keys</Header>}>
      {error ? (
        <ErrorState header="Failed to load keys" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="kms"
          favorite={(k) => ({ id: k.keyId, label: k.description || k.keyId, href: '/aws/kms', type: 'key' })}
          items={keys}
          columns={columns}
          trackBy={(k) => k.keyId}
          title="Keys"
          loading={isLoading}
          actions={
            <Button variant="primary" onClick={() => setCreateOpen(true)}>
              Create key
            </Button>
          }
          emptyTitle="No KMS keys"
          emptyBody="KMS keys encrypt and protect your data across AWS services."
        />
      )}

      <ResourceDetailsModal
        visible={details != null}
        onDismiss={() => setDetails(null)}
        header={details?.description || details?.keyId || 'Key'}
        items={
          details
            ? [
                { label: 'Key ID', value: <Box variant="code">{details.keyId}</Box> },
                { label: 'ARN', value: <Box variant="code">{details.arn || '—'}</Box> },
                { label: 'Description', value: details.description || '—' },
                { label: 'Usage', value: details.keyUsage || '—' },
                { label: 'Spec', value: details.keySpec || '—' },
                {
                  label: 'State',
                  value: (
                    <StatusIndicator type={resourceStatus(details.keyState)}>
                      {details.keyState || '—'}
                    </StatusIndicator>
                  ),
                },
                { label: 'Origin', value: details.origin || '—' },
                { label: 'Created', value: formatDate(details.createdAt) },
              ]
            : []
        }
      />

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create KMS key"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
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
            <FormField label="Description" description="Optional">
              <Input
                value={newDesc}
                onChange={({ detail }) => setNewDesc(detail.value)}
                placeholder="My encryption key"
              />
            </FormField>
            <FormField label="Key usage">
              <Select
                selectedOption={KEY_USAGES.find((o) => o.value === newUsage) ?? KEY_USAGES[0]!}
                onChange={({ detail }) => setNewUsage(detail.selectedOption.value ?? 'ENCRYPT_DECRYPT')}
                options={KEY_USAGES}
                ariaLabel="Key usage"
              />
            </FormField>
            <FormField label="Key spec">
              <Select
                selectedOption={KEY_SPECS.find((o) => o.value === newSpec) ?? KEY_SPECS[0]!}
                onChange={({ detail }) => setNewSpec(detail.selectedOption.value ?? 'SYMMETRIC_DEFAULT')}
                options={KEY_SPECS}
                ariaLabel="Key spec"
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={deleteKey !== null}
        onDismiss={() => setDeleteKey(null)}
        header="Schedule key deletion"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setDeleteKey(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={scheduleMut.isPending}
                onClick={() =>
                  scheduleMut.mutate({ keyId: deleteKey?.keyId ?? '', days: deleteDays })
                }
              >
                Schedule deletion
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="l">
            <FormField label="Key">
              <Box variant="code">{deleteKey?.keyId}</Box>
            </FormField>
            <FormField
              label="Waiting period (days)"
              description="Between 7 and 30 days before the key is permanently deleted."
            >
              <Input
                type="number"
                value={String(deleteDays)}
                onChange={({ detail }) => setDeleteDays(Number(detail.value))}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>
    </ContentLayout>
  )
}
