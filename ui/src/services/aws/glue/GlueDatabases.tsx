import { useState } from 'react'
import { useQuery, useMutation, useQueries, useQueryClient } from '@tanstack/react-query'
import {
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
  TreeView,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import type { TableProps } from '@cloudscape-design/components'
import {
  listDatabases,
  createDatabase,
  deleteDatabase,
  listTables,
  createTable,
  deleteTable,
  type Database,
  type Table as GlueTable,
} from '../../../api/glue'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

const EMPTY_DB_FORM = { name: '', description: '', locationUri: '' }
const EMPTY_TABLE_FORM = { name: '', description: '', location: '', storageType: '' }

export function GlueDatabases() {
  const qc = useQueryClient()
  const { notify } = useNotifications()
  const [selectedDB, setSelectedDB] = useState<Database | null>(null)
  const [createDBOpen, setCreateDBOpen] = useState(false)
  const [deleteDBTarget, setDeleteDBTarget] = useState<Database | null>(null)
  const [createTableOpen, setCreateTableOpen] = useState(false)
  const [deleteTableTarget, setDeleteTableTarget] = useState<GlueTable | null>(null)
  const [dbForm, setDbForm] = useState(EMPTY_DB_FORM)
  const [tableForm, setTableForm] = useState(EMPTY_TABLE_FORM)

  const { data: dbData, isLoading, error } = useQuery({
    queryKey: ['glue', 'databases'],
    queryFn: () => listDatabases(),
  })

  const { data: tableData } = useQuery({
    queryKey: ['glue', 'tables', selectedDB?.name],
    queryFn: () => listTables(selectedDB!.name),
    enabled: !!selectedDB,
  })

  const createDBMut = useMutation({
    mutationFn: () =>
      createDatabase({
        name: dbForm.name,
        description: dbForm.description || undefined,
        locationUri: dbForm.locationUri || undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'databases'] })
      notify({ type: 'success', header: 'Database created', content: dbForm.name })
      setCreateDBOpen(false)
      setDbForm(EMPTY_DB_FORM)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const deleteDBMut = useMutation({
    mutationFn: (name: string) => deleteDatabase(name),
    onSuccess: (_result, name) => {
      void qc.invalidateQueries({ queryKey: ['glue', 'databases'] })
      notify({ type: 'success', header: 'Database deleted', content: name })
      if (deleteDBTarget?.name === selectedDB?.name) setSelectedDB(null)
      setDeleteDBTarget(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const createTableMut = useMutation({
    mutationFn: () =>
      createTable(selectedDB!.name, {
        name: tableForm.name,
        description: tableForm.description || undefined,
        location: tableForm.location || undefined,
        storageType: tableForm.storageType || undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['glue', 'tables', selectedDB?.name] })
      notify({ type: 'success', header: 'Table created', content: tableForm.name })
      setCreateTableOpen(false)
      setTableForm(EMPTY_TABLE_FORM)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const deleteTableMut = useMutation({
    mutationFn: ({ db, name }: { db: string; name: string }) => deleteTable(db, name),
    onSuccess: (_result, { name }) => {
      void qc.invalidateQueries({ queryKey: ['glue', 'tables', selectedDB?.name] })
      notify({ type: 'success', header: 'Table deleted', content: name })
      setDeleteTableTarget(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const databases = dbData?.items ?? []
  const tables = tableData?.items ?? []

  const columns: ResourceColumn<Database>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (db) => db.name,
      cell: (db) => <Box fontWeight="bold">{db.name}</Box>,
    },
    {
      id: 'description',
      header: 'Description',
      filterLabel: 'Description',
      filterValue: (db) => db.description ?? '',
      cell: (db) => db.description || '—',
    },
    {
      id: 'location',
      header: 'Location URI',
      cell: (db) => (db.locationUri ? <Box variant="code">{db.locationUri}</Box> : '—'),
    },
  ]

  const tableColumns: TableProps.ColumnDefinition<GlueTable>[] = [
    { id: 'name', header: 'Name', cell: (t) => t.name },
    {
      id: 'location',
      header: 'Location',
      cell: (t) => (t.location ? <Box variant="code">{t.location}</Box> : '—'),
    },
    {
      id: 'actions',
      header: '',
      cell: (t) => <Button onClick={() => setDeleteTableTarget(t)}>Delete</Button>,
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">Glue databases</Header>}>
      <SpaceBetween size="l">
        {error ? (
          <ErrorState header="Failed to load databases" message={(error as Error).message} />
        ) : (
          <ResourceTable
            favoriteService="glue"
            favorite={(d) => ({ id: d.name, label: d.name, href: '/aws/glue/databases', type: 'database' })}
            items={databases}
            columns={columns}
            trackBy={(db) => db.name}
            title="Databases"
            loading={isLoading}
            onRowClick={(db) => setSelectedDB(db)}
            selectionType="single"
            selectedItems={selectedDB ? [selectedDB] : []}
            onSelectionChange={(items) => setSelectedDB(items[0] ?? null)}
            actions={
              <SpaceBetween direction="horizontal" size="xs">
                <ButtonDropdown
                  items={[{ id: 'delete', text: 'Delete', disabled: !selectedDB }]}
                  onItemClick={() => selectedDB && setDeleteDBTarget(selectedDB)}
                  disabled={!selectedDB}
                >
                  Actions
                </ButtonDropdown>
                <Button variant="primary" onClick={() => setCreateDBOpen(true)}>
                  Create database
                </Button>
              </SpaceBetween>
            }
            emptyTitle="No databases"
            emptyBody="Create a database to store your Glue tables."
          />
        )}

        <GlueCatalogTree databases={databases} />

        {selectedDB && (
          <Container
            header={
              <Header
                variant="h2"
                counter={`(${tables.length})`}
                actions={
                  <SpaceBetween direction="horizontal" size="xs">
                    <Button
                      iconName="close"
                      variant="icon"
                      ariaLabel="Close tables"
                      onClick={() => setSelectedDB(null)}
                    />
                    <Button variant="primary" onClick={() => setCreateTableOpen(true)}>
                      Create table
                    </Button>
                  </SpaceBetween>
                }
              >
                Tables in {selectedDB.name}
              </Header>
            }
          >
            <Table
              variant="embedded"
              items={tables}
              trackBy={(t) => t.name}
              columnDefinitions={tableColumns}
              empty={
                <Box textAlign="center" color="inherit">
                  <b>No tables</b>
                  <Box variant="p" color="inherit">
                    No tables in this database.
                  </Box>
                </Box>
              }
            />
          </Container>
        )}
      </SpaceBetween>

      <Modal
        visible={createDBOpen}
        onDismiss={() => setCreateDBOpen(false)}
        header="Create database"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateDBOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createDBMut.isPending}
                disabled={!dbForm.name.trim()}
                onClick={() => createDBMut.mutate()}
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
                value={dbForm.name}
                placeholder="my_database"
                onChange={({ detail }) => setDbForm({ ...dbForm, name: detail.value })}
              />
            </FormField>
            <FormField label="Description">
              <Input
                value={dbForm.description}
                onChange={({ detail }) => setDbForm({ ...dbForm, description: detail.value })}
              />
            </FormField>
            <FormField label="Location URI">
              <Input
                value={dbForm.locationUri}
                placeholder="s3://bucket/prefix"
                onChange={({ detail }) => setDbForm({ ...dbForm, locationUri: detail.value })}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={!!deleteDBTarget}
        onDismiss={() => setDeleteDBTarget(null)}
        header="Delete database"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setDeleteDBTarget(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteDBMut.isPending}
                onClick={() => deleteDBTarget && deleteDBMut.mutate(deleteDBTarget.name)}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Delete database <b>{deleteDBTarget?.name}</b>? This action cannot be undone.
      </Modal>

      <Modal
        visible={createTableOpen && !!selectedDB}
        onDismiss={() => setCreateTableOpen(false)}
        header={selectedDB ? `Create table in ${selectedDB.name}` : 'Create table'}
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateTableOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createTableMut.isPending}
                disabled={!tableForm.name.trim()}
                onClick={() => createTableMut.mutate()}
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
                value={tableForm.name}
                placeholder="my_table"
                onChange={({ detail }) => setTableForm({ ...tableForm, name: detail.value })}
              />
            </FormField>
            <FormField label="Description">
              <Input
                value={tableForm.description}
                onChange={({ detail }) => setTableForm({ ...tableForm, description: detail.value })}
              />
            </FormField>
            <FormField label="S3 location">
              <Input
                value={tableForm.location}
                placeholder="s3://bucket/prefix/"
                onChange={({ detail }) => setTableForm({ ...tableForm, location: detail.value })}
              />
            </FormField>
            <FormField label="Input format">
              <Input
                value={tableForm.storageType}
                placeholder="org.apache.hadoop.mapred.TextInputFormat"
                onChange={({ detail }) => setTableForm({ ...tableForm, storageType: detail.value })}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={!!deleteTableTarget}
        onDismiss={() => setDeleteTableTarget(null)}
        header="Delete table"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setDeleteTableTarget(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteTableMut.isPending}
                onClick={() =>
                  deleteTableTarget &&
                  deleteTableMut.mutate({
                    db: deleteTableTarget.databaseName,
                    name: deleteTableTarget.name,
                  })
                }
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Delete table <b>{deleteTableTarget?.name}</b>? This action cannot be undone.
      </Modal>
    </ContentLayout>
  )
}

interface CatalogNode {
  id: string
  label: string
  children?: CatalogNode[]
}

function GlueCatalogTree({ databases }: { databases: Database[] }) {
  const [expanded, setExpanded] = useState<string[]>([])

  const tableQueries = useQueries({
    queries: databases.map((db) => ({
      queryKey: ['glue', 'tables', db.name],
      queryFn: () => listTables(db.name),
    })),
  })

  if (databases.length === 0) return null

  const items: CatalogNode[] = databases.map((db, index) => ({
    id: `db:${db.name}`,
    label: db.name,
    children: (tableQueries[index]?.data?.items ?? []).map((table) => ({
      id: `table:${db.name}:${table.name}`,
      label: table.name,
    })),
  }))

  return (
    <Container header={<Header variant="h2">Catalog tree</Header>}>
      <TreeView
        items={items}
        getItemId={(item) => item.id}
        getItemChildren={(item) => item.children}
        renderItem={(item) => ({ content: item.label })}
        expandedItems={expanded}
        onItemToggle={({ detail }) =>
          setExpanded((prev) =>
            detail.expanded ? [...prev, detail.id] : prev.filter((id) => id !== detail.id),
          )
        }
        ariaLabel="Glue catalog tree"
      />
    </Container>
  )
}
