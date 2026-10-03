import { useParams, useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
  Box,
  ContentLayout,
  Header,
  Link,
  Table,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import { listLogStreams, type LogStream } from '../../../api/logs'
import { formatDate } from '../../../lib/date'

export function LogGroupDetail() {
  const { name: encodedName } = useParams<{ name: string }>()
  const groupName = encodedName ? decodeURIComponent(encodedName) : ''
  const navigate = useNavigate()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['logs', 'streams', groupName],
    queryFn: () => listLogStreams(groupName),
    enabled: !!groupName,
  })

  const streams: LogStream[] = data?.items ?? []

  return (
    <ContentLayout
      header={
        <Header variant="h1" description="CloudWatch log group">
          {groupName}
        </Header>
      }
    >
      {error ? (
        <ErrorState header="Failed to load log streams" message={(error as Error).message} onRetry={() => void refetch()} />
      ) : (
        <Table
          items={streams}
          loading={isLoading}
          loadingText="Loading log streams"
          trackBy={(s) => s.name}
          header={
            <Header variant="h2" counter={`(${streams.length})`}>
              Log streams
            </Header>
          }
          columnDefinitions={[
            {
              id: 'name',
              header: 'Stream name',
              cell: (s: LogStream) => (
                <Link
                  href={`/ui/aws/logs/groups/${encodeURIComponent(groupName)}/streams/${encodeURIComponent(s.name)}`}
                  onFollow={(event) => {
                    event.preventDefault()
                    navigate(
                      `/aws/logs/groups/${encodeURIComponent(groupName)}/streams/${encodeURIComponent(s.name)}`,
                    )
                  }}
                >
                  {s.name}
                </Link>
              ),
            },
            {
              id: 'firstEvent',
              header: 'First event',
              cell: (s: LogStream) => formatDate(s.firstEventAt),
            },
            {
              id: 'lastEvent',
              header: 'Last event',
              cell: (s: LogStream) => formatDate(s.lastEventAt),
            },
          ]}
          empty={
            <Box textAlign="center" color="inherit">
              <b>No log streams</b>
              <Box variant="p" color="inherit">
                This log group has no streams yet.
              </Box>
            </Box>
          }
        />
      )}
    </ContentLayout>
  )
}
