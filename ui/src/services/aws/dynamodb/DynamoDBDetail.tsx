import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  ContentLayout,
  Header,
  Icon,
  Link,
  Modal,
  Select,
  SpaceBetween,
  Table,
  Textarea,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import type { TableProps } from '@cloudscape-design/components'
import { JsonEditor } from '../../../components/JsonEditor'
import { scanTable, deleteItem, putItem, type ScanResponse } from '../../../api/dynamodb'
import { useNotifications } from '../../../components/notifications'

function renderValue(v: unknown): string {
  if (v === null || v === undefined) return '—'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

interface Row {
  key: string
  item: Record<string, unknown>
}

export function DynamoDBDetail() {
  const { table: encodedTable } = useParams<{ table: string }>()
  const table = decodeURIComponent(encodedTable ?? '')
  const navigate = useNavigate()
  const qc = useQueryClient()

  const [limit, setLimit] = useState(50)
  const [page, setPage] = useState(0)
  const [pages, setPages] = useState<Array<string | undefined>>([undefined])
  const [editItem, setEditItem] = useState<Record<string, unknown> | null>(null)
  const [editJson, setEditJson] = useState('')
  const [editErr, setEditErr] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<Record<string, unknown> | null>(null)
  const [jsonErr, setJsonErr] = useState('')
  const { notify } = useNotifications()

  const currentToken = pages[page]

  const { data, isLoading, error } = useQuery<ScanResponse>({
    queryKey: ['dynamodb', 'scan', table, currentToken, limit],
    queryFn: () => scanTable(table, { limit, nextToken: currentToken }),
  })

  const deleteMut = useMutation({
    mutationFn: (key: Record<string, unknown>) => deleteItem(table, key),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['dynamodb', 'scan', table] })
      notify({ type: 'success', header: 'Item deleted' })
      setConfirmDelete(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const putMut = useMutation({
    mutationFn: (item: Record<string, unknown>) => putItem(table, item),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['dynamodb', 'scan', table] })
      notify({ type: 'success', header: 'Item saved' })
      setEditItem(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Save failed', content: (err as Error).message }),
  })

  const items = data?.items ?? []
  const rows: Row[] = items.map((item, index) => ({ key: String(index), item }))

  // Derive columns from first few items
  const columnIds = Array.from(
    items.slice(0, 20).reduce((cols, item) => {
      Object.keys(item).forEach((k) => cols.add(k))
      return cols
    }, new Set<string>()),
  ).slice(0, 12)

  const columns: TableProps.ColumnDefinition<Row>[] = [
    ...columnIds.map((col) => ({
      id: col,
      header: col,
      cell: (row: Row) => renderDynamo(row.item[col]),
    })),
    {
      id: 'actions',
      header: '',
      cell: (row: Row) => (
        <SpaceBetween direction="horizontal" size="xs">
          <Button
            variant="inline-link"
            onClick={(event) => {
              event.stopPropagation()
              openEdit(row.item)
            }}
          >
            Edit
          </Button>
          <Button
            variant="inline-link"
            onClick={(event) => {
              event.stopPropagation()
              setConfirmDelete(row.item)
            }}
          >
            Delete
          </Button>
        </SpaceBetween>
      ),
    },
  ]

  const goNext = () => {
    if (data?.lastEvaluatedKey) {
      const tok = JSON.stringify(data.lastEvaluatedKey)
      const next = page + 1
      if (next >= pages.length) setPages([...pages, tok])
      setPage(next)
    }
  }

  const goPrev = () => {
    if (page > 0) {
      setPage(page - 1)
    }
  }

  const openEdit = (item: Record<string, unknown>) => {
    setEditItem(item)
    setEditJson(JSON.stringify(item, null, 2))
    setEditErr('')
  }

  const handleSave = () => {
    try {
      const parsed = JSON.parse(editJson) as Record<string, unknown>
      setEditErr('')
      putMut.mutate(parsed)
    } catch {
      setEditErr('Invalid JSON')
    }
  }

  return (
    <ContentLayout
      header={
        <Header
          variant="h1"
          description={
            <Link
              href="/ui/aws/dynamodb"
              onFollow={(event) => {
                event.preventDefault()
                navigate('/aws/dynamodb')
              }}
            >
              <Icon name="angle-left" /> All tables
            </Link>
          }
          actions={
            <Button
              variant="primary"
              onClick={() => {
                setEditJson('{}')
                setEditItem({})
                setEditErr('')
              }}
            >
              Put item
            </Button>
          }
        >
          {table}
        </Header>
      }
    >
      {error ? (
        <ErrorState header="Failed to scan table" message={(error as Error).message} />
      ) : (
        <Table
          items={rows}
          columnDefinitions={columns}
          trackBy={(row) => row.key}
          loading={isLoading}
          loadingText="Scanning table"
          onRowClick={({ detail }) => openEdit(detail.item.item)}
          stripedRows
          header={
            <Header
              variant="h2"
              description={data ? `${data.count} items · ${data.scannedCount} scanned` : undefined}
              actions={
                <SpaceBetween direction="horizontal" size="xs">
                  <Select
                    selectedOption={{
                      value: String(limit),
                      label: `${limit} rows`,
                    }}
                    onChange={({ detail }) => {
                      setLimit(Number(detail.selectedOption.value))
                      setPage(0)
                      setPages([undefined])
                    }}
                    options={[25, 50, 100, 250].map((n) => ({
                      value: String(n),
                      label: `${n} rows`,
                    }))}
                    ariaLabel="Rows per page"
                  />
                  <Button disabled={page === 0} onClick={goPrev}>
                    Previous
                  </Button>
                  <Button disabled={!data?.lastEvaluatedKey} onClick={goNext}>
                    Next
                  </Button>
                </SpaceBetween>
              }
            >
              Items
            </Header>
          }
          empty={
            <Box textAlign="center" color="inherit">
              <b>No items</b>
              <Box variant="p" color="inherit">
                No items in this table.
              </Box>
            </Box>
          }
        />
      )}

      <Modal
        visible={editItem !== null}
        onDismiss={() => setEditItem(null)}
        header={editItem && Object.keys(editItem).length === 0 ? 'Put item' : 'Edit item'}
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setEditItem(null)}>
                Cancel
              </Button>
              <Button variant="primary" loading={putMut.isPending} onClick={handleSave}>
                Save
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <SpaceBetween size="m">
          <Box variant="p" color="text-body-secondary">
            Edit as DynamoDB JSON (&#123;"pk": &#123;"S": "value"&#125;, ...&#125;)
          </Box>
          <JsonEditor value={editJson} onChange={setEditJson} height={240} ariaLabel="Item JSON" />
          {editErr && <Alert type="error">{editErr}</Alert>}
          {putMut.error && <Alert type="error">{(putMut.error as Error).message}</Alert>}
        </SpaceBetween>
      </Modal>

      <Modal
        visible={confirmDelete !== null}
        onDismiss={() => setConfirmDelete(null)}
        header="Delete item"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => confirmDelete && deleteMut.mutate(confirmDelete)}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <SpaceBetween size="m">
          <Box variant="p">Enter the key for this item to delete it.</Box>
          {confirmDelete && (
            <Textarea
              value={jsonErr || JSON.stringify(extractKey(confirmDelete), null, 2)}
              onChange={({ detail }) => setJsonErr(detail.value)}
              rows={4}
            />
          )}
          {deleteMut.error && <Alert type="error">{(deleteMut.error as Error).message}</Alert>}
        </SpaceBetween>
      </Modal>
    </ContentLayout>
  )
}

function renderDynamo(val: unknown): string {
  if (val === undefined || val === null) return '—'
  if (typeof val === 'object') {
    const obj = val as Record<string, unknown>
    // DynamoDB typed attribute: { S: "..." } | { N: "..." } | { BOOL: true } ...
    if ('S' in obj) return String(obj['S'])
    if ('N' in obj) return String(obj['N'])
    if ('BOOL' in obj) return String(obj['BOOL'])
    if ('NULL' in obj) return 'null'
    if ('L' in obj) return `[${(obj['L'] as unknown[]).length} items]`
    if ('M' in obj) return `{${Object.keys(obj['M'] as object).length} keys}`
    return renderValue(val)
  }
  return String(val)
}

function extractKey(item: Record<string, unknown>): Record<string, unknown> {
  // Heuristic: keep only fields that look like partition/sort key candidates
  return item
}
