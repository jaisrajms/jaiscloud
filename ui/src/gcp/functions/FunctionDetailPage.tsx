import { useState } from 'react'
import type { ReactNode } from 'react'
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
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import {
  deleteFunction,
  getFunction,
  getFunctionIam,
  listDeliveries,
  putFunctionIam,
} from '../../api/gcp/functions'
import { useAccount } from '../../context/AccountContext'
import { IamPolicyPanel } from '../common/IamPolicyPanel'
import { FunctionTestDialog } from './FunctionTestDialog'
import { deliveryStatusColor, functionStateColor, memoryLabel, shortDate, triggerSummary } from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** A small label/value row for the function overview. */
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

/** A single function: overview, source, test, executions and IAM. */
export function FunctionDetailPage() {
  const { location = '', function: fnID = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [testOpen, setTestOpen] = useState(false)

  const key = ['gcp', 'functions', 'function', location, fnID, accountId]

  const detail = useQuery({
    queryKey: key,
    queryFn: () => getFunction(location, fnID),
    enabled: Boolean(location && fnID),
  })

  const deliveries = useQuery({
    queryKey: ['gcp', 'functions', 'function', location, fnID, 'deliveries', accountId],
    queryFn: () => listDeliveries(location, fnID),
    enabled: Boolean(location && fnID),
  })

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'functions'] })

  const remove = useMutation({
    mutationFn: () => deleteFunction(location, fnID),
    onSuccess: () => {
      invalidate()
      navigate('/gcp/functions')
    },
  })

  const fn = detail.data

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}>
        <IconButton component={RouterLink} to="/gcp/functions" aria-label="Back to functions">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="functions">{fnID}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Cloud Functions · {location || '—'}
          </Typography>
        </Box>
        {fn && (
          <>
            <Button startIcon={<PlayArrowIcon />} onClick={() => setTestOpen(true)}>
              Test
            </Button>
            <Button
              color="error"
              startIcon={<DeleteOutlineIcon />}
              disabled={remove.isPending}
              onClick={() => remove.mutate()}
            >
              Delete
            </Button>
          </>
        )}
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the function.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(remove.error as Error).message}
        </Alert>
      )}
      {detail.isLoading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
          <CircularProgress />
        </Box>
      )}

      {fn && (
        <>
          <Stack direction="row" spacing={1} sx={{ mb: 2 }}>
            <Chip size="small" label={fn.status || '—'} color={functionStateColor(fn.status)} />
            <Chip size="small" variant="outlined" label={fn.triggerType === 'event' ? 'Event' : 'HTTP'} />
          </Stack>
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(3, 1fr)' },
              gap: 2,
            }}
          >
            <Detail label="Runtime">{fn.runtime}</Detail>
            <Detail label="Entry point">{fn.entryPoint}</Detail>
            <Detail label="Memory">{memoryLabel(fn.availableMemoryMB)}</Detail>
            <Detail label="Timeout">{fn.timeout}</Detail>
            <Detail label="Min instances">
              {fn.minInstanceCount ? String(fn.minInstanceCount) : '0'}
            </Detail>
            <Detail label="Max instances">{fn.maxInstanceCount ? String(fn.maxInstanceCount) : '—'}</Detail>
            <Detail label="Max concurrency">{fn.maxInstanceRequestConcurrency}</Detail>
            <Detail label="CPU">{fn.availableCpu}</Detail>
            <Detail label="Revision">{fn.revision ? String(fn.revision) : '—'}</Detail>
            <Detail label="Created">{shortDate(fn.createTime)}</Detail>
            <Detail label="Updated">{shortDate(fn.updateTime)}</Detail>
            <Detail label="Description">{fn.description}</Detail>
          </Box>

          <Divider sx={{ my: 3 }} />

          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            Trigger
          </Typography>
          <Box sx={{ mb: 1 }}>
            <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
              {triggerSummary(fn)}
            </Typography>
          </Box>
          {fn.triggerType === 'http' && fn.url && (
            <Link href={fn.url} target="_blank" rel="noreferrer" sx={{ overflowWrap: 'anywhere' }}>
              {fn.url}
            </Link>
          )}
          {fn.triggerType === 'event' && fn.eventTrigger && (
            <Box
              sx={{
                display: 'grid',
                gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)' },
                gap: 2,
                mt: 1,
              }}
            >
              <Detail label="Event type">{fn.eventTrigger.eventType}</Detail>
              <Detail label="Event resource">{fn.eventTrigger.resource}</Detail>
              <Detail label="Retry">
                {fn.eventTrigger.retry
                  ? fn.eventTrigger.retryPolicy || 'Retry'
                  : fn.eventTrigger.retryPolicy || 'Do not retry'}
              </Detail>
              <Detail label="Backing trigger">{fn.eventTrigger.trigger}</Detail>
            </Box>
          )}

          <Divider sx={{ my: 3 }} />

          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            Source
          </Typography>
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)' },
              gap: 2,
            }}
          >
            <Detail label="Archive URL">{fn.sourceArchiveUrl || fn.sourceUploadUrl}</Detail>
            <Detail label="Revision hash">{fn.sourceSha256}</Detail>
            <Detail label="Size">{fn.sourceSize ? `${fn.sourceSize} bytes` : '—'}</Detail>
            <Detail label="Environment variables">
              {fn.environmentVariables ? JSON.stringify(fn.environmentVariables) : ''}
            </Detail>
          </Box>

          <Divider sx={{ my: 3 }} />

          <Stack direction="row" sx={{ alignItems: 'center', justifyContent: 'space-between', mb: 1 }}>
            <Typography variant="subtitle1">Deliveries (emulator diagnostic)</Typography>
            <Button size="small" startIcon={<PlayArrowIcon />} onClick={() => setTestOpen(true)}>
              Test function
            </Button>
          </Stack>
          <Typography variant="caption" color="text.secondary">
            Real Cloud Functions has no execution list; these are the emulator&apos;s persisted event-delivery
            records (status, attempts, dead-letter outcome).
          </Typography>
          {deliveries.isError && <Alert severity="error">Failed to load deliveries.</Alert>}
          <TableContainer sx={{ mt: 1, border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Delivery</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell>Event type</TableCell>
                  <TableCell>Attempts</TableCell>
                  <TableCell>Error</TableCell>
                  <TableCell>Updated</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {deliveries.isLoading && (
                  <TableRow>
                    <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                      <CircularProgress size={24} />
                    </TableCell>
                  </TableRow>
                )}
                {!deliveries.isLoading && (deliveries.data?.deliveries.length ?? 0) === 0 && (
                  <TableRow>
                    <TableCell colSpan={6} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                      No event deliveries recorded.
                    </TableCell>
                  </TableRow>
                )}
                {deliveries.data?.deliveries.map((record) => (
                  <TableRow key={record.id} hover>
                    <TableCell sx={{ fontFamily: 'monospace' }}>{record.id}</TableCell>
                    <TableCell>
                      <Chip
                        size="small"
                        label={record.status || '—'}
                        color={deliveryStatusColor(record.status)}
                      />
                    </TableCell>
                    <TableCell sx={{ maxWidth: 260, overflowWrap: 'anywhere' }}>{record.eventType || '—'}</TableCell>
                    <TableCell>{record.attempts ?? 0}</TableCell>
                    <TableCell sx={{ maxWidth: 260, overflowWrap: 'anywhere' }}>{record.error || '—'}</TableCell>
                    <TableCell>{shortDate(record.updateTime)}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>

          <Divider sx={{ my: 3 }} />

          <IamPolicyPanel
            title="Function IAM policy"
            queryKey={['gcp', 'functions', 'function', location, fnID, 'iam', accountId]}
            load={() => getFunctionIam(location, fnID)}
            save={(policy) => putFunctionIam(location, fnID, policy)}
            defaultRole="roles/cloudfunctions.invoker"
          />

          <FunctionTestDialog
            open={testOpen}
            onClose={() => setTestOpen(false)}
            location={location}
            fn={fnID}
          />
        </>
      )}
    </Box>
  )
}
