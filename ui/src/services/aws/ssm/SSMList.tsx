import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Badge,
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
  Textarea,
} from '@cloudscape-design/components'
import { CopyText } from '../../../components/CopyText'
import { ErrorState } from '../../../components/ErrorState'
import {
  listParameters,
  putParameter,
  deleteParameter,
  getParameter,
  type Parameter,
} from '../../../api/ssm'
import { formatDate } from '../../../lib/date'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

const PARAMETER_TYPES = [
  { value: 'String', label: 'String' },
  { value: 'StringList', label: 'StringList' },
  { value: 'SecureString', label: 'SecureString' },
]

export function SSMList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newValue, setNewValue] = useState('')
  const [newType, setNewType] = useState('String')
  const [newDesc, setNewDesc] = useState('')
  const [pathFilter, setPathFilter] = useState('')
  const [viewParam, setViewParam] = useState<Parameter | null>(null)
  const [viewValue, setViewValue] = useState<string | null>(null)
  const [confirmDelete, setConfirmDelete] = useState<Parameter | null>(null)
  const qc = useQueryClient()
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['ssm', 'parameters', pathFilter],
    queryFn: () => listParameters(pathFilter ? { path: pathFilter } : undefined),
  })

  const putMut = useMutation({
    mutationFn: () =>
      putParameter({
        name: newName,
        value: newValue,
        type: newType,
        description: newDesc || undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['ssm', 'parameters'] })
      notify({ type: 'success', header: 'Parameter created', content: newName })
      setCreateOpen(false)
      setNewName('')
      setNewValue('')
      setNewType('String')
      setNewDesc('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create parameter', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteParameter(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['ssm', 'parameters'] })
      notify({ type: 'success', header: 'Parameter deleted' })
      setConfirmDelete(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const handleView = async (p: Parameter) => {
    setViewParam(p)
    setViewValue(null)
    try {
      const resp = await getParameter(p.name)
      setViewValue(resp.Parameter?.value ?? '(empty)')
    } catch {
      setViewValue('(could not retrieve value)')
    }
  }

  const params = data?.items ?? []

  const columns: ResourceColumn<Parameter>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (p) => p.name,
      cell: (p) => <CopyText value={p.name} label="parameter name" />,
    },
    {
      id: 'type',
      header: 'Type',
      filterLabel: 'Type',
      filterValue: (p) => p.type,
      cell: (p) => (
        <Badge color={p.type === 'SecureString' ? 'red' : 'blue'}>{p.type}</Badge>
      ),
    },
    {
      id: 'version',
      header: 'Version',
      cell: (p) => p.version,
    },
    {
      id: 'lastModified',
      header: 'Last modified',
      cell: (p) => formatDate(p.lastModifiedDate),
    },
    {
      id: 'actions',
      header: 'Actions',
      cell: (p) => (
        <div onClick={(event) => event.stopPropagation()}>
          <SpaceBetween direction="horizontal" size="xs">
            <Button variant="link" onClick={() => void handleView(p)}>
              View
            </Button>
            <Button variant="link" onClick={() => setConfirmDelete(p)}>
              Delete
            </Button>
          </SpaceBetween>
        </div>
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">SSM Parameter Store</Header>}>
      {error ? (
        <ErrorState header="Failed to load parameters" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <SpaceBetween size="m">
          <FormField label="Path prefix" description="Filter parameters by path prefix, e.g. /myapp/">
            <Input
              value={pathFilter}
              onChange={({ detail }) => setPathFilter(detail.value)}
              placeholder="/myapp/"
              ariaLabel="Filter by path prefix"
            />
          </FormField>
          <ResourceTable
            favoriteService="ssm"
            favorite={(p) => ({ id: p.name, label: p.name, href: '/aws/ssm', type: 'parameter' })}
            items={params}
            columns={columns}
            trackBy={(p) => p.name}
            title="Parameters"
            loading={isLoading}
            actions={
              <Button variant="primary" onClick={() => setCreateOpen(true)}>
                Create parameter
              </Button>
            }
            emptyTitle="No parameters"
            emptyBody="Store configuration values and secrets as SSM parameters."
          />
        </SpaceBetween>
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create parameter"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={putMut.isPending}
                disabled={!newName || !newValue}
                onClick={() => putMut.mutate()}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="l">
            <FormField label="Name">
              <Input
                autoFocus
                value={newName}
                onChange={({ detail }) => setNewName(detail.value)}
                placeholder="/myapp/db-password"
              />
            </FormField>
            <FormField label="Type">
              <Select
                selectedOption={PARAMETER_TYPES.find((o) => o.value === newType) ?? PARAMETER_TYPES[0]!}
                onChange={({ detail }) => setNewType(detail.selectedOption.value ?? 'String')}
                options={PARAMETER_TYPES}
                ariaLabel="Parameter type"
              />
            </FormField>
            <FormField label="Value">
              <Textarea
                value={newValue}
                onChange={({ detail }) => setNewValue(detail.value)}
                placeholder="parameter value"
              />
            </FormField>
            <FormField label="Description" description="Optional">
              <Input
                value={newDesc}
                onChange={({ detail }) => setNewDesc(detail.value)}
                placeholder="My parameter description"
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={viewParam !== null}
        onDismiss={() => {
          setViewParam(null)
          setViewValue(null)
        }}
        header={viewParam?.name ?? 'Parameter'}
        footer={
          <Box float="right">
            <Button
              onClick={() => {
                setViewParam(null)
                setViewValue(null)
              }}
            >
              Close
            </Button>
          </Box>
        }
      >
        <SpaceBetween size="m">
          <Box color="text-body-secondary">
            Type: {viewParam?.type} · Version: {viewParam?.version}
          </Box>
          {viewParam?.description && <Box>{viewParam.description}</Box>}
          <FormField label="Value">
            {viewValue === null ? (
              <Box color="text-status-inactive">Loading…</Box>
            ) : (
              <Box variant="pre">{viewValue}</Box>
            )}
          </FormField>
        </SpaceBetween>
      </Modal>

      <Modal
        visible={confirmDelete !== null}
        onDismiss={() => setConfirmDelete(null)}
        header="Delete parameter"
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
        Delete <b>{confirmDelete?.name}</b>? This cannot be undone.
      </Modal>
    </ContentLayout>
  )
}
