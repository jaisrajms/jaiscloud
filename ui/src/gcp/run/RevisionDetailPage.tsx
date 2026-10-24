import { useQuery } from '@tanstack/react-query'
import { Alert, Box, CircularProgress, Divider, IconButton, Stack, Typography } from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import { Link as RouterLink, useParams } from 'react-router-dom'
import { getRevision } from '../../api/gcp/run'
import { useAccount } from '../../context/AccountContext'
import { firstImage, shortDate } from './util'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { GcpPageTitle } from '../common/PageTitle'

function asString(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

/** A single Cloud Run revision: metadata plus the full wire object. */
export function RevisionDetailPage() {
  const { region = '', service = '', revision = '' } = useParams()
  const { accountId } = useAccount()

  const query = useQuery({
    queryKey: ['gcp', 'run', 'revision', region, service, revision, accountId],
    queryFn: () => getRevision(region, service, revision),
    enabled: Boolean(region && service && revision),
  })

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton
          component={RouterLink}
          to={`/gcp/run/services/${encodeURIComponent(region)}/${encodeURIComponent(service)}`}
          aria-label="Back to service"
        >
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="run">{revision}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            {service} · {region} · project {accountId || '—'}
          </Typography>
        </Box>
      </Stack>

      {query.isError && <Alert severity="error">Failed to load the revision.</Alert>}
      {query.isLoading && <CircularProgress size={24} />}

      {query.data && (
        <Box sx={{ maxWidth: 900 }}>
          <Stack spacing={2} sx={{ mb: 2 }}>
            <Stack spacing={0.5}>
              <Typography variant="caption" color="text.secondary">
                Image
              </Typography>
              <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
                {firstImage(query.data.containers) || '—'}
              </Typography>
            </Stack>
            <Stack spacing={0.5}>
              <Typography variant="caption" color="text.secondary">
                Full resource name
              </Typography>
              <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
                {asString(query.data.name)}
              </Typography>
            </Stack>
            <Stack spacing={0.5}>
              <Typography variant="caption" color="text.secondary">
                Created {shortDate(asString(query.data.createTime))} · updated{' '}
                {shortDate(asString(query.data.updateTime))}
              </Typography>
            </Stack>
          </Stack>
          <Divider sx={{ mb: 2 }} />
          <Typography variant="caption" color="text.secondary" sx={{ mb: 0.5, display: 'block' }}>
            Revision (google.cloud.run.v2.Revision)
          </Typography>
          <GcpCodeEditor
            value={JSON.stringify(query.data, null, 2)}
            readOnly
            language="json"
            minRows={20}
            ariaLabel="Revision JSON"
          />
        </Box>
      )}
    </Box>
  )
}
