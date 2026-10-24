import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import type { ReactNode } from 'react'
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
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import { deleteService, getService, listRevisions } from '../../api/gcp/run'
import { useAccount } from '../../context/AccountContext'
import { firstImage, lastSegment, shortDate, templateContainers } from './util'
import { GcpPageTitle } from '../common/PageTitle'

function asString(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

/** A small label/value row for the service overview. */
function Detail({ label, children }: { label: string; children: ReactNode }) {
  return (
    <Stack spacing={0.5}>
      <Typography variant="caption" color="text.secondary">
        {label}
      </Typography>
      <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
        {children || '—'}
      </Typography>
    </Stack>
  )
}

/** A single Cloud Run service: overview plus its revisions. */
export function ServiceDetailPage() {
  const { region = '', service = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()

  const detail = useQuery({
    queryKey: ['gcp', 'run', 'service', region, service, accountId],
    queryFn: () => getService(region, service),
    enabled: Boolean(region && service),
  })

  const revisions = useQuery({
    queryKey: ['gcp', 'run', 'revisions', region, service, accountId],
    queryFn: () => listRevisions(region, service),
    enabled: Boolean(region && service),
  })

  const remove = useMutation({
    mutationFn: () => deleteService(region, service),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'run'] })
      navigate('/gcp/run/services')
    },
  })

  const ready = asString(
    (detail.data?.terminalCondition as Record<string, unknown> | undefined)?.state,
  ) === 'CONDITION_SUCCEEDED'

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton component={RouterLink} to="/gcp/run/services" aria-label="Back to services">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="run">{service}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            {region} · project {accountId || '—'}
          </Typography>
        </Box>
        <Button
          color="error"
          startIcon={<DeleteOutlineIcon />}
          disabled={remove.isPending}
          onClick={() => remove.mutate()}
        >
          Delete
        </Button>
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the service.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      {detail.isLoading && <CircularProgress size={24} />}

      {detail.data && (
        <Box sx={{ maxWidth: 1100, mb: 3 }}>
          <Stack
            direction="row"
            spacing={4}
            sx={{ flexWrap: 'wrap', rowGap: 2, mb: 2 }}
            useFlexGap
          >
            <Detail label="Status">
              <Chip size="small" label={ready ? 'Ready' : 'Not ready'} color={ready ? 'success' : 'warning'} />
            </Detail>
            <Detail label="Region">{region}</Detail>
            <Detail label="Image">{firstImage(templateContainers(detail.data))}</Detail>
            <Detail label="Created">{shortDate(asString(detail.data.createTime))}</Detail>
            <Detail label="Updated">{shortDate(asString(detail.data.updateTime))}</Detail>
          </Stack>
          <Stack spacing={0.5}>
            <Detail label="Service URL">
              {asString(detail.data.uri) ? (
                <Link href={asString(detail.data.uri)} target="_blank" rel="noreferrer">
                  {asString(detail.data.uri)}
                </Link>
              ) : (
                ''
              )}
            </Detail>
            <Detail label="Full resource name">{asString(detail.data.name)}</Detail>
          </Stack>
          <Divider sx={{ mt: 2 }} />
        </Box>
      )}

      <Typography variant="h6" sx={{ mb: 1 }}>
        Revisions
      </Typography>
      {revisions.isError && <Alert severity="error">Failed to load revisions.</Alert>}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Revision</TableCell>
              <TableCell>Image</TableCell>
              <TableCell>Created</TableCell>
              <TableCell>Updated</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {revisions.isLoading && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!revisions.isLoading && (revisions.data?.revisions.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No revisions.
                </TableCell>
              </TableRow>
            )}
            {revisions.data?.revisions.map((revision) => {
              const id = lastSegment(asString(revision.name))
              return (
                <TableRow key={id} hover>
                  <TableCell>
                    <Link
                      component={RouterLink}
                      to={`/gcp/run/services/${encodeURIComponent(region)}/${encodeURIComponent(service)}/revisions/${encodeURIComponent(id)}`}
                    >
                      {id}
                    </Link>
                  </TableCell>
                  <TableCell>{firstImage(revision.containers)}</TableCell>
                  <TableCell>{shortDate(asString(revision.createTime))}</TableCell>
                  <TableCell>{shortDate(asString(revision.updateTime))}</TableCell>
                </TableRow>
              )
            })}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}
