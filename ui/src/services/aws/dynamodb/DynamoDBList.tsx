import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  Alert,
  Box,
  Button,
  ButtonDropdown,
  Checkbox,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Link,
  Modal,
  Select,
  SpaceBetween,
  StatusIndicator,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import {
  listTables,
  createTable,
  deleteTable,
  type TableSummary,
  type CreateTableRequest,
} from '../../../api/dynamodb'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`
  return `${(n / 1024 / 1024).toFixed(1)} MB`
}

export function DynamoDBList() {
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<TableSummary[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const qc = useQueryClient()
  const navigate = useNavigate()
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['dynamodb', 'tables'],
    queryFn: () => listTables(),
  })

  const deleteMut = useMutation({
    mutationFn: async (tables: TableSummary[]) => {
      for (const table of tables) await deleteTable(table.name)
    },
    onSuccess: (_result, tables) => {
      void qc.invalidateQueries({ queryKey: ['dynamodb', 'tables'] })
      notify({
        type: 'success',
        header: `Deleted ${tables.length} table${tables.length !== 1 ? 's' : ''}`,
      })
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const tables = data?.items ?? []

  const columns: ResourceColumn<TableSummary>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (t) => t.name,
      cell: (t) => (
        <Link
          href={`/ui/aws/dynamodb/${encodeURIComponent(t.name)}`}
          onFollow={(event) => {
            event.preventDefault()
            navigate(`/aws/dynamodb/${encodeURIComponent(t.name)}`)
          }}
        >
          {t.name}
        </Link>
      ),
    },
    {
      id: 'status',
      header: 'Status',
      filterLabel: 'Status',
      filterValue: (t) => t.status,
      cell: (t) => <StatusIndicator type={resourceStatus(t.status)}>{t.status}</StatusIndicator>,
    },
    {
      id: 'items',
      header: 'Items',
      cell: (t) => t.itemCount.toLocaleString(),
    },
    { id: 'size', header: 'Size', cell: (t) => fmtBytes(t.sizeBytes) },
    {
      id: 'billing',
      header: 'Billing mode',
      filterLabel: 'Billing mode',
      filterValue: (t) => t.billingMode,
      cell: (t) => t.billingMode,
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">DynamoDB tables</Header>}>
      {error ? (
        <ErrorState header="Failed to load tables" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <ResourceTable
          favoriteService="dynamodb"
          favorite={(t) => ({ id: t.name, label: t.name, href: '/aws/dynamodb/' + encodeURIComponent(t.name), type: 'table' })}
          items={tables}
          columns={columns}
          trackBy={(t) => t.name}
          title="Tables"
          loading={isLoading}
          onRowClick={(t) => navigate(`/aws/dynamodb/${encodeURIComponent(t.name)}`)}
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
                Create table
              </Button>
            </SpaceBetween>
          }
          emptyTitle="No tables"
          emptyBody="DynamoDB tables store your NoSQL data."
        />
      )}

      {createOpen && (
        <CreateTableDialog
          onClose={() => setCreateOpen(false)}
          onCreated={(name) => {
            setCreateOpen(false)
            void qc.invalidateQueries({ queryKey: ['dynamodb', 'tables'] })
            notify({ type: 'success', header: 'Table created', content: name })
          }}
        />
      )}

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete tables"
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
        Permanently delete {selected.length} table{selected.length !== 1 ? 's' : ''} and all of their
        items? This action cannot be undone.
      </Modal>
    </ContentLayout>
  )
}

const TYPE_OPTIONS = [
  { value: 'S', label: 'String (S)' },
  { value: 'N', label: 'Number (N)' },
  { value: 'B', label: 'Binary (B)' },
]

const BILLING_OPTIONS = [
  { value: 'PAY_PER_REQUEST', label: 'On-demand (PAY_PER_REQUEST)' },
  { value: 'PROVISIONED', label: 'Provisioned' },
]

function CreateTableDialog({
  onClose,
  onCreated,
}: {
  onClose: () => void
  onCreated: (name: string) => void
}) {
  const [form, setForm] = useState<CreateTableRequest>({
    tableName: '',
    keySchema: [{ attributeName: 'pk', keyType: 'HASH' }],
    attributeDefinitions: [{ attributeName: 'pk', attributeType: 'S' }],
    billingMode: 'PAY_PER_REQUEST',
  })
  const [withSort, setWithSort] = useState(false)
  const { notify } = useNotifications()

  const createMut = useMutation({
    mutationFn: (req: CreateTableRequest) => createTable(req),
    onSuccess: () => onCreated(form.tableName),
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const handleCreate = () => {
    const req = { ...form }
    if (!withSort) {
      req.keySchema = req.keySchema.filter((k) => k.keyType === 'HASH')
      req.attributeDefinitions = req.attributeDefinitions.filter((a) =>
        req.keySchema.some((k) => k.attributeName === a.attributeName),
      )
    }
    createMut.mutate(req)
  }

  const pkName = form.keySchema.find((k) => k.keyType === 'HASH')?.attributeName ?? 'pk'
  const skName = form.keySchema.find((k) => k.keyType === 'RANGE')?.attributeName ?? 'sk'
  const pkType = form.attributeDefinitions.find((a) => a.attributeName === pkName)?.attributeType ?? 'S'
  const skType = form.attributeDefinitions.find((a) => a.attributeName === skName)?.attributeType ?? 'S'

  return (
    <Modal
      visible
      onDismiss={onClose}
      header="Create table"
      footer={
        <Box float="right">
          <SpaceBetween direction="horizontal" size="xs">
            <Button variant="link" onClick={onClose}>
              Cancel
            </Button>
            <Button
              variant="primary"
              loading={createMut.isPending}
              disabled={!form.tableName}
              onClick={handleCreate}
            >
              Create
            </Button>
          </SpaceBetween>
        </Box>
      }
    >
      <Form>
        <SpaceBetween size="m">
          <FormField label="Table name">
            <Input
              autoFocus
              value={form.tableName}
              onChange={({ detail }) => setForm({ ...form, tableName: detail.value })}
              placeholder="my-table"
            />
          </FormField>

          <FormField label="Partition key (HASH)">
            <SpaceBetween direction="horizontal" size="xs">
              <Input
                value={pkName}
                onChange={({ detail }) =>
                  setForm({
                    ...form,
                    keySchema: form.keySchema.map((k) =>
                      k.keyType === 'HASH' ? { ...k, attributeName: detail.value } : k,
                    ),
                    attributeDefinitions: form.attributeDefinitions.map((a) =>
                      a.attributeName === pkName ? { ...a, attributeName: detail.value } : a,
                    ),
                  })
                }
              />
              <Select
                selectedOption={TYPE_OPTIONS.find((o) => o.value === pkType) ?? TYPE_OPTIONS[0]!}
                onChange={({ detail }) =>
                  setForm({
                    ...form,
                    attributeDefinitions: form.attributeDefinitions.map((a) =>
                      a.attributeName === pkName
                        ? { ...a, attributeType: detail.selectedOption.value as 'S' | 'N' | 'B' }
                        : a,
                    ),
                  })
                }
                options={TYPE_OPTIONS}
                ariaLabel="Partition key type"
              />
            </SpaceBetween>
          </FormField>

          <Checkbox
            checked={withSort}
            onChange={({ detail }) => {
              setWithSort(detail.checked)
              if (detail.checked) {
                setForm({
                  ...form,
                  keySchema: [
                    ...form.keySchema.filter((k) => k.keyType === 'HASH'),
                    { attributeName: 'sk', keyType: 'RANGE' },
                  ],
                  attributeDefinitions: [
                    ...form.attributeDefinitions.filter((a) => a.attributeName !== 'sk'),
                    { attributeName: 'sk', attributeType: 'S' },
                  ],
                })
              }
            }}
          >
            Add sort key (RANGE)
          </Checkbox>

          {withSort && (
            <FormField label="Sort key (RANGE)">
              <SpaceBetween direction="horizontal" size="xs">
                <Input
                  value={skName}
                  onChange={({ detail }) =>
                    setForm({
                      ...form,
                      keySchema: form.keySchema.map((k) =>
                        k.keyType === 'RANGE' ? { ...k, attributeName: detail.value } : k,
                      ),
                      attributeDefinitions: form.attributeDefinitions.map((a) =>
                        a.attributeName === skName ? { ...a, attributeName: detail.value } : a,
                      ),
                    })
                  }
                />
                <Select
                  selectedOption={TYPE_OPTIONS.find((o) => o.value === skType) ?? TYPE_OPTIONS[0]!}
                  onChange={({ detail }) =>
                    setForm({
                      ...form,
                      attributeDefinitions: form.attributeDefinitions.map((a) =>
                        a.attributeName === skName
                          ? { ...a, attributeType: detail.selectedOption.value as 'S' | 'N' | 'B' }
                          : a,
                      ),
                    })
                  }
                  options={TYPE_OPTIONS}
                  ariaLabel="Sort key type"
                />
              </SpaceBetween>
            </FormField>
          )}

          <FormField label="Billing mode">
            <Select
              selectedOption={
                BILLING_OPTIONS.find((o) => o.value === form.billingMode) ?? BILLING_OPTIONS[0]!
              }
              onChange={({ detail }) =>
                setForm({
                  ...form,
                  billingMode: detail.selectedOption.value as 'PAY_PER_REQUEST' | 'PROVISIONED',
                })
              }
              options={BILLING_OPTIONS}
            />
          </FormField>

          {createMut.error && (
            <Alert type="error" header="Could not create table">
              {(createMut.error as Error).message}
            </Alert>
          )}
        </SpaceBetween>
      </Form>
    </Modal>
  )
}
