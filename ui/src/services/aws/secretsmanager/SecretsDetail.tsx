import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Box,
  Button,
  Container,
  ContentLayout,
  Form,
  FormField,
  Header,
  Link,
  Modal,
  SpaceBetween,
  Table,
  Tabs,
  Textarea,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import {
  getSecretValue,
  putSecretValue,
  listSecretVersions,
  type SecretVersion,
} from '../../../api/secretsmanager'
import { formatDate } from '../../../lib/date'
import { useNotifications } from '../../../components/notifications'

type Tab = 'value' | 'versions'

export function SecretsDetail() {
  const { name } = useParams<{ name: string }>()
  const navigate = useNavigate()
  const qc = useQueryClient()
  const decodedName = decodeURIComponent(name ?? '')
  const [tab, setTab] = useState<Tab>('value')
  const [editOpen, setEditOpen] = useState(false)
  const [newValue, setNewValue] = useState('')
  const [showValue, setShowValue] = useState(false)
  const { notify } = useNotifications()

  const { data: valueData, isLoading, error } = useQuery({
    queryKey: ['secretsmanager', 'value', decodedName],
    queryFn: () => getSecretValue(decodedName),
    enabled: showValue,
    retry: false,
  })

  const { data: versionsData } = useQuery({
    queryKey: ['secretsmanager', 'versions', decodedName],
    queryFn: () => listSecretVersions(decodedName),
  })

  const putMut = useMutation({
    mutationFn: () => putSecretValue(decodedName, newValue),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['secretsmanager', 'value', decodedName] })
      void qc.invalidateQueries({ queryKey: ['secretsmanager', 'versions', decodedName] })
      notify({ type: 'success', header: 'Secret value updated' })
      setEditOpen(false)
      setNewValue('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Update failed', content: (err as Error).message }),
  })

  const versions = versionsData?.items ?? []

  return (
    <ContentLayout
      breadcrumbs={
        <Link
          href="/ui/aws/secretsmanager"
          onFollow={(event) => {
            event.preventDefault()
            navigate('/aws/secretsmanager')
          }}
        >
          Secrets
        </Link>
      }
      header={
        <Header
          variant="h1"
          actions={
            <Button
              variant="primary"
              onClick={() => {
                setNewValue('')
                setEditOpen(true)
              }}
            >
              Update
            </Button>
          }
        >
          {decodedName}
        </Header>
      }
    >
      <Tabs
        tabs={[
          { id: 'value', label: 'Secret value' },
          { id: 'versions', label: `Versions (${versions.length})` },
        ]}
        activeTabId={tab}
        onChange={({ detail }) => setTab(detail.activeTabId as Tab)}
      />

      {tab === 'value' && (
        <Container
          header={
            <Header
              variant="h2"
              actions={
                <Button onClick={() => setShowValue((v) => !v)}>
                  {showValue ? 'Hide' : 'Reveal'}
                </Button>
              }
            >
              Secret value
            </Header>
          }
        >
          {!showValue ? (
            <Box color="text-body-secondary">Click Reveal to view the secret value.</Box>
          ) : isLoading ? (
            <Box color="text-status-inactive">Loading…</Box>
          ) : error ? (
            <ErrorState header="Could not retrieve secret value" message={(error as Error).message} />
          ) : (
            <Box variant="pre">{valueData?.SecretString ?? '(binary)'}</Box>
          )}
        </Container>
      )}

      {tab === 'versions' && (
        <Container header={<Header variant="h2">Versions</Header>}>
          <Table
            items={versions}
            columnDefinitions={[
              {
                id: 'versionId',
                header: 'Version ID',
                cell: (v: SecretVersion) => <Box variant="code">{v.versionId.slice(0, 8)}…</Box>,
              },
              {
                id: 'stages',
                header: 'Stages',
                cell: (v: SecretVersion) => (v.versionStages ?? []).join(', ') || '—',
              },
              {
                id: 'created',
                header: 'Created',
                cell: (v: SecretVersion) => formatDate(v.createdDate),
              },
            ]}
            trackBy={(v) => v.versionId}
            empty={
              <Box textAlign="center" color="inherit">
                <b>No versions</b>
              </Box>
            }
          />
        </Container>
      )}

      <Modal
        visible={editOpen}
        onDismiss={() => setEditOpen(false)}
        header="Update secret value"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setEditOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={putMut.isPending}
                disabled={!newValue}
                onClick={() => putMut.mutate()}
              >
                Save
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <FormField label="New secret value">
            <Textarea
              autoFocus
              value={newValue}
              onChange={({ detail }) => setNewValue(detail.value)}
              placeholder='{"username":"admin","password":"new-secret"}'
            />
          </FormField>
        </Form>
      </Modal>
    </ContentLayout>
  )
}
