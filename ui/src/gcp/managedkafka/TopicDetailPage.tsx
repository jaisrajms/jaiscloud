import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Box, Button, CircularProgress, Divider, Stack, Typography } from '@mui/material'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import { useNavigate, useParams } from 'react-router-dom'
import { deleteTopic, getTopic } from '../../api/gcp/managedkafka'
import { formatDate } from '../../lib/date'
import { useAccount } from '../../context/AccountContext'
import { TopicDialog } from './TopicDialog'
import { Detail } from './common'
import { GcpPageHeader } from '../common/GcpPageHeader'

/** A single Managed Kafka topic: settings and property overrides. */
export function TopicDetailPage() {
  const { location = '', cluster = '', topic: topicId = '' } = useParams()
  const { accountId } = useAccount()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [editOpen, setEditOpen] = useState(false)

  const detail = useQuery({
    queryKey: ['gcp', 'managedkafka', 'topic', location, cluster, topicId, accountId],
    queryFn: () => getTopic(location, cluster, topicId),
    enabled: Boolean(location && cluster && topicId),
  })

  const topic = detail.data
  const backTo = `/gcp/managedkafka/clusters/${encodeURIComponent(location)}/${encodeURIComponent(cluster)}`

  const remove = useMutation({
    mutationFn: () => deleteTopic(location, cluster, topicId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'managedkafka'] })
      navigate(backTo)
    },
  })

  return (
    <Box>
      <GcpPageHeader
        id="managedkafka"
        title={topicId}
        subtitle={`Managed Kafka topic · ${cluster || '—'} · ${location || '—'}`}
        backTo={backTo}
        backAriaLabel="Back to cluster"
        actions={
          <>
            <Button
              variant="outlined"
              startIcon={<EditOutlinedIcon />}
              onClick={() => setEditOpen(true)}
              disabled={!topic}
            >
              Edit
            </Button>
            <Button
              variant="outlined"
              color="error"
              startIcon={<DeleteOutlineIcon />}
              onClick={() => remove.mutate()}
              disabled={!topic || remove.isPending}
              sx={{ ml: 1 }}
            >
              Delete
            </Button>
          </>
        }
      />

      {topic && (
        <TopicDialog
          open={editOpen}
          onClose={() => setEditOpen(false)}
          location={location}
          cluster={cluster}
          topic={topic}
        />
      )}

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(remove.error as Error).message}
        </Alert>
      )}

      {detail.isError && <Alert severity="error">Failed to load the topic.</Alert>}
      {detail.isLoading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
          <CircularProgress />
        </Box>
      )}

      {topic && (
        <>
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(3, 1fr)' },
              gap: 2,
              mb: 3,
            }}
          >
            <Detail label="Topic">{topic.id}</Detail>
            <Detail label="Cluster">{topic.cluster}</Detail>
            <Detail label="Location">{topic.location}</Detail>
            <Detail label="Partitions">{topic.partitionCount}</Detail>
            <Detail label="Replication factor">{topic.replicationFactor}</Detail>
            <Detail label="Created">{formatDate(topic.createTime)}</Detail>
            <Detail label="Updated">{formatDate(topic.updateTime)}</Detail>
          </Box>

          {topic.configs && Object.keys(topic.configs).length > 0 && (
            <>
              <Divider sx={{ mb: 2 }} />
              <Typography variant="subtitle1" sx={{ mb: 1 }}>
                Topic configuration
              </Typography>
              <Stack spacing={1}>
                {Object.entries(topic.configs).map(([key, value]) => (
                  <Detail key={key} label={key}>
                    <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                      {value}
                    </Typography>
                  </Detail>
                ))}
              </Stack>
            </>
          )}

          <Divider sx={{ my: 3 }} />
          <Detail label="Resource name">{topic.name}</Detail>
        </>
      )}
    </Box>
  )
}
