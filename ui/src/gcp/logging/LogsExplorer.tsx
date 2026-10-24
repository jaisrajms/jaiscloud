import { useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useSearchParams } from 'react-router-dom'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Collapse,
  MenuItem,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from '@mui/material'
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown'
import KeyboardArrowRightIcon from '@mui/icons-material/KeyboardArrowRight'
import { listEntries, listLogs, type LogEntry } from '../../api/gcp/logging'
import { useAccount } from '../../context/AccountContext'
import { lastSegment, payloadPreview, severityColor, shortDate } from './util'
import { GcpPageHeader } from '../common/GcpPageHeader'

/** Cloud Logging Logs Explorer: filter, browse and inspect log entries. A
 * `?logName=` parameter (used by the global search's Log results) presets the
 * filter to that log. */
export function LogsExplorer() {
  const { accountId } = useAccount()
  const [params] = useSearchParams()
  const logName = params.get('logName')
  const [filter, setFilter] = useState(logName ? `logName="${logName}"` : '')
  const [orderBy, setOrderBy] = useState('timestamp desc')
  const [expanded, setExpanded] = useState<number | null>(null)

  const logs = useQuery({
    queryKey: ['gcp', 'logging', 'logs', accountId],
    queryFn: () => listLogs(),
  })

  const entries = useQuery({
    queryKey: ['gcp', 'logging', 'entries', accountId, filter, orderBy],
    queryFn: () => listEntries({ filter, orderBy, pageSize: 100 }),
  })

  const rows = useMemo(() => entries.data?.entries ?? [], [entries.data])
  const logNames = useMemo(() => logs.data?.logNames ?? [], [logs.data])

  return (
    <Box>
      <GcpPageHeader
        id="logging"
        title="Cloud Logging"
        subtitle={`Logs explorer · project ${accountId || '—'}`}
      />

      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} sx={{ mb: 2 }}>
        <TextField
          label="Filter"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder='severity>=ERROR AND resource.type="gce_instance"'
          size="small"
          fullWidth
        />
        <TextField
          select
          label="Order"
          value={orderBy}
          onChange={(e) => setOrderBy(e.target.value)}
          size="small"
          sx={{ minWidth: 200 }}
        >
          <MenuItem value="timestamp desc">Newest first</MenuItem>
          <MenuItem value="timestamp asc">Oldest first</MenuItem>
        </TextField>
        <Button variant="outlined" onClick={() => void entries.refetch()}>
          Refresh
        </Button>
      </Stack>

      {logs.data && logNames.length > 0 && (
        <Stack direction="row" spacing={1} sx={{ mb: 1, flexWrap: 'wrap', rowGap: 1 }}>
          {logNames.slice(0, 8).map((name) => (
            <Chip
              key={name}
              size="small"
              label={lastSegment(name)}
              variant={filter.includes(name) ? 'filled' : 'outlined'}
              onClick={() => setFilter(`logName="${name}"`)}
            />
          ))}
        </Stack>
      )}

      {entries.isError && <Alert severity="error">Failed to load log entries.</Alert>}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell width={32} />
              <TableCell>Timestamp</TableCell>
              <TableCell>Severity</TableCell>
              <TableCell>Log</TableCell>
              <TableCell>Resource</TableCell>
              <TableCell>Payload</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {entries.isLoading && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!entries.isLoading && rows.length === 0 && (
              <TableRow>
                <TableCell colSpan={6} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No log entries match this filter.
                </TableCell>
              </TableRow>
            )}
            {rows.map((entry, i) => (
              <LogRow
                key={`${entry.insertId ?? ''}-${i}`}
                entry={entry}
                open={expanded === i}
                onToggle={() => setExpanded(expanded === i ? null : i)}
              />
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  )
}

function LogRow({ entry, open, onToggle }: { entry: LogEntry; open: boolean; onToggle: () => void }) {
  const hasDetail = Boolean(entry.jsonPayload) || Boolean(entry.labels)
  return (
    <>
      <TableRow hover sx={{ '& > *': { borderBottom: 'unset' } }}>
        <TableCell>
          {hasDetail && (
            <Button size="small" sx={{ minWidth: 0, p: 0.5 }} onClick={onToggle} aria-label="Toggle details">
              {open ? <KeyboardArrowDownIcon fontSize="small" /> : <KeyboardArrowRightIcon fontSize="small" />}
            </Button>
          )}
        </TableCell>
        <TableCell sx={{ whiteSpace: 'nowrap' }}>{shortDate(entry.timestamp)}</TableCell>
        <TableCell>
          <Chip size="small" label={entry.severity || 'DEFAULT'} color={severityColor(entry.severity)} />
        </TableCell>
        <TableCell>{lastSegment(entry.logName) || '—'}</TableCell>
        <TableCell>{entry.resource?.type || '—'}</TableCell>
        <TableCell
          sx={{ maxWidth: 420, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}
        >
          {payloadPreview(entry)}
        </TableCell>
      </TableRow>
      {hasDetail && (
        <TableRow>
          <TableCell colSpan={6} sx={{ py: 0 }}>
            <Collapse in={open} timeout="auto" unmountOnExit>
              <Box sx={{ py: 2 }}>
                <Typography variant="subtitle2" gutterBottom>
                  Payload
                </Typography>
                <Box
                  component="pre"
                  sx={{
                    m: 0,
                    p: 1.5,
                    bgcolor: 'action.hover',
                    borderRadius: 1,
                    fontSize: 12,
                    overflow: 'auto',
                    maxHeight: 320,
                  }}
                >
                  {JSON.stringify(entry.jsonPayload ?? entry, null, 2)}
                </Box>
              </Box>
            </Collapse>
          </TableCell>
        </TableRow>
      )}
    </>
  )
}
