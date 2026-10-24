import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, Stack } from '@mui/material'
import { Link as RouterLink } from 'react-router-dom'
import { listTopics, type ManagedKafkaTopic } from '../../api/gcp/managedkafka'
import { formatDate } from '../../lib/date'
import { useAccount } from '../../context/AccountContext'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** Managed Kafka topics across every cluster and location (read-only). */
export function TopicsPage() {
  const { accountId } = useAccount()
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const topics = useQuery({
    queryKey: ['gcp', 'managedkafka', 'topics', accountId],
    queryFn: listTopics,
  })

  const rows = filterRows(topics.data?.topics ?? [], filter, (topic) =>
    [topic.id, topic.cluster, topic.location].join(' '),
  )

  const clusterLink = (topic: ManagedKafkaTopic) =>
    `/gcp/managedkafka/clusters/${encodeURIComponent(topic.location)}/${encodeURIComponent(topic.cluster)}`

  const columns: GcpColumn<ManagedKafkaTopic>[] = [
    {
      key: 'id',
      header: 'Topic',
      sortable: true,
      sortValue: (topic) => topic.id,
      render: (topic) => (
        <Link
          component={RouterLink}
          to={`${clusterLink(topic)}/topics/${encodeURIComponent(topic.id)}`}
        >
          {topic.id}
        </Link>
      ),
    },
    {
      key: 'cluster',
      header: 'Cluster',
      sortable: true,
      sortValue: (topic) => topic.cluster,
      render: (topic) => (
        <Link component={RouterLink} to={clusterLink(topic)}>
          {topic.cluster}
        </Link>
      ),
    },
    {
      key: 'location',
      header: 'Location',
      sortable: true,
      sortValue: (topic) => topic.location,
      render: (topic) => topic.location || '—',
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
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="managedkafka"
        title="Managed Kafka"
        subtitle={`Topics · project ${accountId || '—'}`}
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter topics"
        onRefresh={() => void topics.refetch()}
        refreshing={topics.isFetching}
      />

      <GcpDataTable
        aria-label="Managed Kafka topics"
        columns={columns}
        rows={rows}
        getRowKey={(topic) => `${topic.location}/${topic.cluster}/${topic.id}`}
        loading={topics.isLoading}
        error={topics.isError ? 'Failed to load topics.' : null}
        emptyMessage={filter ? 'No topics match the filter.' : 'No Managed Kafka topics in this project.'}
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(topic) => <GcpRowDetail row={topic} />}
        detailTitle={(topic) => topic.id}
      />
    </Stack>
  )
}
