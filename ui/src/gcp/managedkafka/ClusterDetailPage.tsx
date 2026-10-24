import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  IconButton,
  Link,
  Stack,
  Tooltip,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import { Link as RouterLink, useParams } from 'react-router-dom'
import {
  deleteAcl,
  deleteConsumerGroup,
  deleteTopic,
  getCluster,
  getTopic,
  listAcls,
  listClusterTopics,
  listConsumerGroups,
  type ManagedKafkaAcl,
  type ManagedKafkaConsumerGroup,
  type ManagedKafkaConsumerGroupOffset,
  type ManagedKafkaTopic,
} from '../../api/gcp/managedkafka'
import { formatDate } from '../../lib/date'
import { useAccount } from '../../context/AccountContext'
import { AclDialog } from './AclDialog'
import { ClusterDialog } from './ClusterDialog'
import { ConsumerGroupDialog } from './ConsumerGroupDialog'
import { TopicDialog } from './TopicDialog'
import { Detail, JsonBlock } from './common'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpTabs } from '../common/GcpTabs'

type TabKey = 'topics' | 'acls' | 'consumer-groups'

const TABS: { value: TabKey; label: string }[] = [
  { value: 'topics', label: 'Topics' },
  { value: 'acls', label: 'ACLs' },
  { value: 'consumer-groups', label: 'Consumer groups' },
]

/** A single Managed Kafka cluster: overview, config and read-only data tabs. */
export function ClusterDetailPage() {
  const { location = '', cluster: clusterId = '' } = useParams()
  const { accountId } = useAccount()
  const [tab, setTab] = useState<TabKey>('topics')
  const [editOpen, setEditOpen] = useState(false)

  const detail = useQuery({
    queryKey: ['gcp', 'managedkafka', 'cluster', location, clusterId, accountId],
    queryFn: () => getCluster(location, clusterId),
    enabled: Boolean(location && clusterId),
  })

  const cluster = detail.data
  const base = `/gcp/managedkafka/clusters/${encodeURIComponent(location)}/${encodeURIComponent(clusterId)}`

  return (
    <Box>
      <GcpPageHeader
        id="managedkafka"
        title={clusterId}
        subtitle={`Managed Kafka cluster · ${location || '—'}`}
        backTo="/gcp/managedkafka/clusters"
        backAriaLabel="Back to clusters"
        actions={
          <Button
            variant="outlined"
            startIcon={<EditOutlinedIcon />}
            onClick={() => setEditOpen(true)}
            disabled={!cluster}
          >
            Edit cluster
          </Button>
        }
      />

      {cluster && (
        <ClusterDialog open={editOpen} onClose={() => setEditOpen(false)} cluster={cluster} />
      )}

      {detail.isError && <Alert severity="error">Failed to load the cluster.</Alert>}
      {detail.isLoading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
          <CircularProgress />
        </Box>
      )}

      {cluster && (
        <>
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(3, 1fr)' },
              gap: 2,
              mb: 3,
            }}
          >
            <Detail label="Location">{cluster.location}</Detail>
            <Detail label="Status">
              <Chip
                size="small"
                label={cluster.state || '—'}
                color={cluster.state === 'ACTIVE' ? 'success' : 'default'}
              />
            </Detail>
            <Detail label="Bootstrap address">
              <Typography variant="body2" sx={{ fontFamily: 'monospace', overflowWrap: 'anywhere' }}>
                {cluster.bootstrapAddress || '—'}
              </Typography>
            </Detail>
            <Detail label="Created">{formatDate(cluster.createTime)}</Detail>
            <Detail label="Updated">{formatDate(cluster.updateTime)}</Detail>
          </Box>

          {cluster.labels && Object.keys(cluster.labels).length > 0 && (
            <>
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Labels
              </Typography>
              <Stack direction="row" spacing={1} sx={{ flexWrap: 'wrap', rowGap: 1, mb: 3 }}>
                {Object.entries(cluster.labels).map(([k, v]) => (
                  <Chip key={k} size="small" variant="outlined" label={`${k}=${v}`} />
                ))}
              </Stack>
            </>
          )}

          {cluster.config != null && (
            <>
              <Divider sx={{ mb: 2 }} />
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Cluster config
              </Typography>
              <JsonBlock value={cluster.config} />
            </>
          )}

          <Divider sx={{ my: 3 }} />

          <GcpTabs
            tabs={TABS}
            value={tab}
            onChange={(next) => setTab(next as TabKey)}
            aria-label="Managed Kafka cluster data"
          />

          {tab === 'topics' && <TopicsTab location={location} cluster={clusterId} base={base} />}
          {tab === 'acls' && <AclsTab location={location} cluster={clusterId} />}
          {tab === 'consumer-groups' && (
            <ConsumerGroupsTab location={location} cluster={clusterId} />
          )}
        </>
      )}
    </Box>
  )
}

function TopicsTab({ location, cluster, base }: { location: string; cluster: string; base: string }) {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<ManagedKafkaTopic | undefined>(undefined)
  const topics = useQuery({
    queryKey: ['gcp', 'managedkafka', 'cluster-topics', location, cluster, accountId],
    queryFn: () => listClusterTopics(location, cluster),
    enabled: Boolean(location && cluster),
  })

  const remove = useMutation({
    mutationFn: (topic: ManagedKafkaTopic) => deleteTopic(location, cluster, topic.id),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'managedkafka'] }),
  })

  // The list row omits `configs`, so fetch the full topic before editing;
  // otherwise the dialog would clear every stored override on save.
  const openEdit = async (row: ManagedKafkaTopic) => {
    const full = await queryClient.fetchQuery({
      queryKey: ['gcp', 'managedkafka', 'topic', location, cluster, row.id, accountId],
      queryFn: () => getTopic(location, cluster, row.id),
    })
    setEditing(full)
    setDialogOpen(true)
  }

  const columns: GcpColumn<ManagedKafkaTopic>[] = [
    {
      key: 'id',
      header: 'Topic',
      sortable: true,
      sortValue: (topic) => topic.id,
      render: (topic) => (
        <Link component={RouterLink} to={`${base}/topics/${encodeURIComponent(topic.id)}`}>
          {topic.id}
        </Link>
      ),
    },
    {
      key: 'partitionCount',
      header: 'Partitions',
      align: 'right',
      sortable: true,
      sortValue: (topic) => topic.partitionCount,
      render: (topic) => topic.partitionCount,
    },
    {
      key: 'replicationFactor',
      header: 'Replication factor',
      align: 'right',
      sortable: true,
      sortValue: (topic) => topic.replicationFactor,
      render: (topic) => topic.replicationFactor,
    },
    {
      key: 'created',
      header: 'Created',
      sortable: true,
      sortValue: (topic) => topic.createTime ?? '',
      render: (topic) => formatDate(topic.createTime),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (topic) => (
        <>
          <Tooltip title="Edit topic">
            <span>
              <IconButton
                size="small"
                aria-label={`Edit ${topic.id}`}
                onClick={() => void openEdit(topic)}
              >
                <EditOutlinedIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
          <Tooltip title="Delete topic">
            <span>
              <IconButton
                size="small"
                aria-label={`Delete ${topic.id}`}
                disabled={remove.isPending}
                onClick={() => remove.mutate(topic)}
              >
                <DeleteOutlineIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
        </>
      ),
    },
  ]

  return (
    <>
      <Stack direction="row" sx={{ justifyContent: 'flex-end', mb: 1 }}>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => {
            setEditing(undefined)
            setDialogOpen(true)
          }}
        >
          Create topic
        </Button>
      </Stack>
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 1 }}>
          {(remove.error as Error).message}
        </Alert>
      )}
      <GcpDataTable
        aria-label="Cluster topics"
        columns={columns}
        rows={topics.data?.topics ?? []}
        getRowKey={(topic) => topic.id}
        loading={topics.isLoading}
        error={topics.isError ? 'Failed to load topics.' : null}
        emptyMessage="No topics on this cluster."
      />
      <TopicDialog
        open={dialogOpen}
        onClose={() => setDialogOpen(false)}
        location={location}
        cluster={cluster}
        topic={editing}
      />
    </>
  )
}

function AclsTab({ location, cluster }: { location: string; cluster: string }) {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<ManagedKafkaAcl | undefined>(undefined)
  const acls = useQuery({
    queryKey: ['gcp', 'managedkafka', 'acls', location, cluster, accountId],
    queryFn: () => listAcls(location, cluster),
    enabled: Boolean(location && cluster),
  })

  const remove = useMutation({
    mutationFn: (acl: ManagedKafkaAcl) => deleteAcl(location, cluster, acl.id),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'managedkafka'] }),
  })

  const columns: GcpColumn<ManagedKafkaAcl>[] = [
    {
      key: 'resourceType',
      header: 'Resource type',
      sortable: true,
      sortValue: (acl) => acl.resourceType ?? '',
      render: (acl) => acl.resourceType || '—',
    },
    {
      key: 'resourceName',
      header: 'Resource pattern',
      sortable: true,
      sortValue: (acl) => acl.resourceName ?? '',
      render: (acl) => acl.resourceName || '—',
    },
    {
      key: 'patternType',
      header: 'Pattern',
      sortable: true,
      sortValue: (acl) => acl.patternType ?? '',
      render: (acl) => acl.patternType || '—',
    },
    {
      key: 'entries',
      header: 'Entries',
      render: (acl) => (
        <Stack spacing={0.5}>
          {acl.aclEntries.map((entry, i) => (
            <Typography key={i} variant="body2">
              {[entry.principal, entry.permissionType, entry.operation, entry.host]
                .filter(Boolean)
                .join(' · ') || '—'}
            </Typography>
          ))}
        </Stack>
      ),
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (acl) => (
        <>
          <Tooltip title="Edit ACL">
            <span>
              <IconButton
                size="small"
                aria-label={`Edit ${acl.id}`}
                onClick={() => {
                  setEditing(acl)
                  setDialogOpen(true)
                }}
              >
                <EditOutlinedIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
          <Tooltip title="Delete ACL">
            <span>
              <IconButton
                size="small"
                aria-label={`Delete ${acl.id}`}
                disabled={remove.isPending}
                onClick={() => remove.mutate(acl)}
              >
                <DeleteOutlineIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
        </>
      ),
    },
  ]

  return (
    <>
      <Stack direction="row" sx={{ justifyContent: 'flex-end', mb: 1 }}>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => {
            setEditing(undefined)
            setDialogOpen(true)
          }}
        >
          Create ACL
        </Button>
      </Stack>
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 1 }}>
          {(remove.error as Error).message}
        </Alert>
      )}
      <GcpDataTable
        aria-label="Cluster ACLs"
        columns={columns}
        rows={acls.data?.acls ?? []}
        getRowKey={(acl) => acl.id}
        loading={acls.isLoading}
        error={acls.isError ? 'Failed to load ACLs.' : null}
        emptyMessage="No ACLs on this cluster."
      />
      <AclDialog
        open={dialogOpen}
        onClose={() => setDialogOpen(false)}
        location={location}
        cluster={cluster}
        acl={editing}
      />
    </>
  )
}

function ConsumerGroupsTab({ location, cluster }: { location: string; cluster: string }) {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [dialogOpen, setDialogOpen] = useState(false)
  const [editing, setEditing] = useState<ManagedKafkaConsumerGroup | undefined>(undefined)
  const groups = useQuery({
    queryKey: ['gcp', 'managedkafka', 'consumer-groups', location, cluster, accountId],
    queryFn: () => listConsumerGroups(location, cluster),
    enabled: Boolean(location && cluster),
  })

  const remove = useMutation({
    mutationFn: (group: ManagedKafkaConsumerGroup) => deleteConsumerGroup(location, cluster, group.id),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'managedkafka'] }),
  })

  const columns: GcpColumn<ManagedKafkaConsumerGroup>[] = [
    {
      key: 'id',
      header: 'Group',
      sortable: true,
      sortValue: (group) => group.id,
      render: (group) => group.id,
    },
    {
      key: 'topics',
      header: 'Topics',
      align: 'right',
      sortable: true,
      sortValue: (group) => new Set(group.offsets.map((o) => o.topic)).size,
      render: (group) => new Set(group.offsets.map((o) => o.topic)).size,
    },
    {
      key: 'partitions',
      header: 'Committed partitions',
      align: 'right',
      sortable: true,
      sortValue: (group) => group.offsets.length,
      render: (group) => group.offsets.length,
    },
    {
      key: 'actions',
      header: 'Actions',
      align: 'right',
      render: (group) => (
        <>
          <Tooltip title="Edit committed offsets">
            <span>
              <IconButton
                size="small"
                aria-label={`Edit ${group.id}`}
                onClick={() => {
                  setEditing(group)
                  setDialogOpen(true)
                }}
              >
                <EditOutlinedIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
          <Tooltip title="Delete consumer group">
            <span>
              <IconButton
                size="small"
                aria-label={`Delete ${group.id}`}
                disabled={remove.isPending}
                onClick={() => remove.mutate(group)}
              >
                <DeleteOutlineIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
        </>
      ),
    },
  ]

  return (
    <>
      <Alert severity="info" sx={{ mb: 2 }}>
        Consumer groups are read from the cluster&apos;s live Kafka broker. With the default mock
        topology (no broker running) the list is empty.
      </Alert>
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 1 }}>
          {(remove.error as Error).message}
        </Alert>
      )}
      <GcpDataTable
        aria-label="Cluster consumer groups"
        columns={columns}
        rows={groups.data?.groups ?? []}
        getRowKey={(group) => group.id}
        loading={groups.isLoading}
        error={groups.isError ? 'Failed to load consumer groups.' : null}
        emptyMessage="No consumer groups on this cluster."
        renderDetail={(group) => <ConsumerGroupOffsets group={group} />}
        detailTitle={(group) => group.id}
      />
      <ConsumerGroupDialog
        open={dialogOpen}
        onClose={() => setDialogOpen(false)}
        location={location}
        cluster={cluster}
        group={editing}
      />
    </>
  )
}

/** shortTopic renders the leaf id of a full topic resource name. */
function shortTopic(topic: string): string {
  const parts = topic.split('/topics/')
  return parts.length > 1 ? (parts[parts.length - 1] ?? topic) : topic
}

function ConsumerGroupOffsets({ group }: { group: ManagedKafkaConsumerGroup }) {
  if (group.offsets.length === 0) {
    return <Typography variant="body2">No committed offsets.</Typography>
  }
  return (
    <Stack spacing={0.5}>
      {group.offsets.map((offset: ManagedKafkaConsumerGroupOffset, i: number) => (
        <Typography key={`${offset.topic}-${offset.partition}-${i}`} variant="body2">
          {shortTopic(offset.topic)} · partition {offset.partition} · offset {offset.offset}
          {offset.metadata ? ` · ${offset.metadata}` : ''}
        </Typography>
      ))}
    </Stack>
  )
}
