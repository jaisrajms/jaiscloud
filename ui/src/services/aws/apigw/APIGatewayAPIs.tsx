import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
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
  Tabs,
  Textarea,
} from '@cloudscape-design/components'
import { CopyText } from '../../../components/CopyText'
import { ErrorState } from '../../../components/ErrorState'
import type { TableProps } from '@cloudscape-design/components'
import {
  listRestAPIs,
  createRestAPI,
  deleteRestAPI,
  listResources,
  listStages,
  listDeployments,
  createDeployment,
  type RestAPI,
  type Resource,
  type Stage,
  type Deployment,
} from '../../../api/apigw'
import { formatDate } from '../../../lib/date'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

type Tab = 'resources' | 'stages' | 'deployments'

const resourceColumns: TableProps.ColumnDefinition<Resource>[] = [
  { id: 'path', header: 'Path', cell: (r) => <Box variant="code">{r.path}</Box> },
  { id: 'id', header: 'ID', cell: (r) => <Box variant="code">{r.id}</Box> },
  { id: 'parent', header: 'Parent', cell: (r) => <Box variant="code">{r.parentId || '—'}</Box> },
]

const stageColumns: TableProps.ColumnDefinition<Stage>[] = [
  { id: 'name', header: 'Stage', cell: (s) => s.name },
  { id: 'deploymentId', header: 'Deployment ID', cell: (s) => <Box variant="code">{s.deploymentId || '—'}</Box> },
  { id: 'description', header: 'Description', cell: (s) => s.description || '—' },
]

const deploymentColumns: TableProps.ColumnDefinition<Deployment>[] = [
  { id: 'id', header: 'ID', cell: (d) => <Box variant="code">{d.id}</Box> },
  { id: 'description', header: 'Description', cell: (d) => d.description || '—' },
  { id: 'created', header: 'Created', cell: (d) => formatDate(d.createdDate) },
]

export function APIGatewayAPIs() {
  const qc = useQueryClient()
  const [selectedAPI, setSelectedAPI] = useState<RestAPI | null>(null)
  const [activeTab, setActiveTab] = useState<Tab>('resources')
  const [createOpen, setCreateOpen] = useState(false)
  const [selected, setSelected] = useState<RestAPI[]>([])
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [deployOpen, setDeployOpen] = useState(false)
  const [form, setForm] = useState({ name: '', description: '' })
  const [deployForm, setDeployForm] = useState({ stageName: '', description: '' })
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['apigw', 'apis'],
    queryFn: () => listRestAPIs(),
  })

  const { data: resourcesData, isLoading: resourcesLoading } = useQuery({
    queryKey: ['apigw', 'resources', selectedAPI?.id],
    queryFn: () => listResources(selectedAPI!.id),
    enabled: !!selectedAPI && activeTab === 'resources',
  })

  const { data: stagesData, isLoading: stagesLoading } = useQuery({
    queryKey: ['apigw', 'stages', selectedAPI?.id],
    queryFn: () => listStages(selectedAPI!.id),
    enabled: !!selectedAPI && activeTab === 'stages',
  })

  const { data: deploymentsData, isLoading: deploymentsLoading } = useQuery({
    queryKey: ['apigw', 'deployments', selectedAPI?.id],
    queryFn: () => listDeployments(selectedAPI!.id),
    enabled: !!selectedAPI && activeTab === 'deployments',
  })

  const createMut = useMutation({
    mutationFn: () => createRestAPI({ name: form.name, description: form.description || undefined }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['apigw', 'apis'] })
      notify({ type: 'success', header: 'REST API created', content: form.name })
      setCreateOpen(false)
      setForm({ name: '', description: '' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: async (apis: RestAPI[]) => {
      for (const api of apis) await deleteRestAPI(api.id)
    },
    onSuccess: (_r, apis) => {
      void qc.invalidateQueries({ queryKey: ['apigw', 'apis'] })
      notify({ type: 'success', header: `Deleted ${apis.length} REST API(s)` })
      if (selectedAPI && apis.some((api) => api.id === selectedAPI.id)) setSelectedAPI(null)
      setSelected([])
      setConfirmDelete(false)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const deployMut = useMutation({
    mutationFn: () =>
      createDeployment(selectedAPI!.id, {
        stageName: deployForm.stageName || undefined,
        description: deployForm.description || undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['apigw', 'deployments', selectedAPI?.id] })
      void qc.invalidateQueries({ queryKey: ['apigw', 'stages', selectedAPI?.id] })
      notify({ type: 'success', header: 'API deployed' })
      setDeployOpen(false)
      setDeployForm({ stageName: '', description: '' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Deploy failed', content: (err as Error).message }),
  })

  const apis = data?.items ?? []
  const resources = resourcesData?.items ?? []
  const stages = stagesData?.items ?? []
  const deployments = deploymentsData?.items ?? []

  const columns: ResourceColumn<RestAPI>[] = [
    { id: 'name', header: 'Name', filterLabel: 'Name', filterValue: (a) => a.name, cell: (a) => a.name },
    { id: 'id', header: 'ID', cell: (a) => <CopyText value={a.id} label="API ID" /> },
  ]

  return (
    <ContentLayout header={<Header variant="h1">API Gateway</Header>}>
      {error ? (
        <ErrorState header="Failed to load REST APIs" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <SpaceBetween size="l">
          <ResourceTable
            favoriteService="apigateway"
            favorite={(a) => ({ id: a.id, label: a.name, href: '/aws/apigateway/apis', type: 'REST API' })}
            items={apis}
            columns={columns}
            trackBy={(a) => a.id}
            title="REST APIs"
            loading={isLoading}
            onRowClick={(a) => setSelectedAPI(a)}
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
                  Create API
                </Button>
              </SpaceBetween>
            }
            emptyTitle="No REST APIs"
            emptyBody="Create a REST API to get started."
          />

          {selectedAPI && (
            <Container
              header={
                <Header
                  variant="h2"
                  description={<Box variant="code">{selectedAPI.id}</Box>}
                  actions={
                    <SpaceBetween direction="horizontal" size="xs">
                      {activeTab === 'deployments' && (
                        <Button onClick={() => setDeployOpen(true)}>Deploy</Button>
                      )}
                      <Button variant="link" onClick={() => setSelectedAPI(null)}>
                        Close
                      </Button>
                    </SpaceBetween>
                  }
                >
                  {selectedAPI.name}
                </Header>
              }
            >
              <Tabs
                tabs={[
                  { id: 'resources', label: 'Resources' },
                  { id: 'stages', label: 'Stages' },
                  { id: 'deployments', label: 'Deployments' },
                ]}
                activeTabId={activeTab}
                onChange={({ detail }) => setActiveTab(detail.activeTabId as Tab)}
              />

              {activeTab === 'resources' && (
                <Table
                  items={resources}
                  columnDefinitions={resourceColumns}
                  loading={resourcesLoading}
                  loadingText="Loading resources"
                  trackBy={(r) => r.id}
                  header={<Header variant="h3">Resources</Header>}
                  empty={<Box textAlign="center">No resources defined</Box>}
                />
              )}

              {activeTab === 'stages' && (
                <Table
                  items={stages}
                  columnDefinitions={stageColumns}
                  loading={stagesLoading}
                  loadingText="Loading stages"
                  trackBy={(s) => s.name}
                  header={<Header variant="h3">Stages</Header>}
                  empty={<Box textAlign="center">No stages. Deploy to create a stage.</Box>}
                />
              )}

              {activeTab === 'deployments' && (
                <Table
                  items={deployments}
                  columnDefinitions={deploymentColumns}
                  loading={deploymentsLoading}
                  loadingText="Loading deployments"
                  trackBy={(d) => d.id}
                  header={<Header variant="h3">Deployments</Header>}
                  empty={<Box textAlign="center">No deployments yet.</Box>}
                />
              )}
            </Container>
          )}
        </SpaceBetween>
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create REST API"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!form.name}
                onClick={() => createMut.mutate()}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <FormField label="Name">
              <Input
                autoFocus
                value={form.name}
                onChange={({ detail }) => setForm({ ...form, name: detail.value })}
                placeholder="my-api"
              />
            </FormField>
            <FormField label="Description">
              <Textarea
                rows={3}
                value={form.description}
                onChange={({ detail }) => setForm({ ...form, description: detail.value })}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete}
        onDismiss={() => setConfirmDelete(false)}
        header="Delete REST APIs"
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
        Permanently delete {selected.length} REST API{selected.length !== 1 ? 's' : ''}?
      </Modal>

      <Modal
        visible={deployOpen && !!selectedAPI}
        onDismiss={() => setDeployOpen(false)}
        header={selectedAPI ? `Deploy ${selectedAPI.name}` : 'Deploy'}
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setDeployOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deployMut.isPending}
                onClick={() => deployMut.mutate()}
              >
                Deploy
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <FormField label="Stage name">
              <Input
                autoFocus
                value={deployForm.stageName}
                onChange={({ detail }) => setDeployForm({ ...deployForm, stageName: detail.value })}
                placeholder="prod"
              />
            </FormField>
            <FormField label="Description">
              <Textarea
                rows={3}
                value={deployForm.description}
                onChange={({ detail }) =>
                  setDeployForm({ ...deployForm, description: detail.value })
                }
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>
    </ContentLayout>
  )
}
