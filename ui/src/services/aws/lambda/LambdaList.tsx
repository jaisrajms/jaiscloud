import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  Box,
  Button,
  ButtonDropdown,
  ContentLayout,
  Header,
  Link,
  Modal,
  SpaceBetween,
  StatusIndicator,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import { listFunctions, deleteFunction, type LambdaFunction } from '../../../api/lambda'
import { formatDate } from '../../../lib/date'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

export function LambdaList() {
  const [selected, setSelected] = useState<LambdaFunction[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const qc = useQueryClient()
  const navigate = useNavigate()
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['lambda', 'functions'],
    queryFn: () => listFunctions(),
  })

  const deleteMut = useMutation({
    mutationFn: async (functions: LambdaFunction[]) => {
      for (const fn of functions) await deleteFunction(fn.name)
    },
    onSuccess: (_result, functions) => {
      void qc.invalidateQueries({ queryKey: ['lambda', 'functions'] })
      notify({
        type: 'success',
        header: `Deleted ${functions.length} function${functions.length !== 1 ? 's' : ''}`,
      })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const functions = data?.items ?? []

  const columns: ResourceColumn<LambdaFunction>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (fn) => fn.name,
      cell: (fn) => (
        <Link
          href={`/ui/aws/lambda/${encodeURIComponent(fn.name)}`}
          onFollow={(event) => {
            event.preventDefault()
            navigate(`/aws/lambda/${encodeURIComponent(fn.name)}`)
          }}
        >
          {fn.name}
        </Link>
      ),
    },
    {
      id: 'runtime',
      header: 'Runtime',
      filterLabel: 'Runtime',
      filterValue: (fn) => fn.runtime,
      cell: (fn) => <code>{fn.runtime}</code>,
    },
    { id: 'handler', header: 'Handler', cell: (fn) => <Box variant="code">{fn.handler}</Box> },
    { id: 'modified', header: 'Last modified', cell: (fn) => formatDate(fn.lastModified) },
    {
      id: 'state',
      header: 'State',
      cell: (fn) => <StatusIndicator type={resourceStatus(fn.state)}>{fn.state || '—'}</StatusIndicator>,
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">Lambda functions</Header>}>
      {error ? (
        <ErrorState header="Failed to load functions" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="lambda"
          favorite={(fn) => ({ id: fn.name, label: fn.name, href: '/aws/lambda/' + encodeURIComponent(fn.name), type: 'function' })}
          items={functions}
          columns={columns}
          trackBy={(fn) => fn.arn}
          title="Functions"
          loading={isLoading}
          onRowClick={(fn) => navigate(`/aws/lambda/${encodeURIComponent(fn.name)}`)}
          selectionType="multi"
          selectedItems={selected}
          onSelectionChange={setSelected}
          actions={
            <ButtonDropdown
              items={[{ id: 'delete', text: 'Delete', disabled: selected.length === 0 }]}
              onItemClick={() => setConfirmDelete(true)}
              disabled={selected.length === 0}
            >
              Actions
            </ButtonDropdown>
          }
          emptyTitle="No functions"
          emptyBody="Lambda functions let you run code without managing infrastructure."
        />
      )}

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete functions"
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
        Permanently delete {selected.length} function{selected.length !== 1 ? 's' : ''}?
      </Modal>
    </ContentLayout>
  )
}
