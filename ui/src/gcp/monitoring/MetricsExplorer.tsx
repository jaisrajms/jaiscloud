import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
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
import { listMetricDescriptors, listTimeSeries } from '../../api/gcp/monitoring'
import { useAccount } from '../../context/AccountContext'
import { formatTypedValue, seriesLabel, shortDate } from './util'
import { GcpPageHeader } from '../common/GcpPageHeader'

/** Cloud Monitoring Metrics Explorer: browse metric descriptors and their series. */
export function MetricsExplorer() {
  const { accountId } = useAccount()
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState('')

  const descriptors = useQuery({
    queryKey: ['gcp', 'monitoring', 'descriptors', accountId, filter],
    queryFn: () => listMetricDescriptors(filter),
  })

  const seriesFilter = selected ? `metric.type="${selected}"` : ''
  const series = useQuery({
    queryKey: ['gcp', 'monitoring', 'timeseries', accountId, seriesFilter],
    queryFn: () => listTimeSeries({ filter: seriesFilter, pageSize: 100 }),
    enabled: Boolean(seriesFilter),
  })

  const descriptorRows = descriptors.data?.metricDescriptors ?? []
  const seriesRows = series.data?.timeSeries ?? []

  return (
    <Box>
      <GcpPageHeader
        id="monitoring"
        title="Metrics explorer"
        subtitle={`Metric descriptors and stored time series · project ${accountId || '—'}`}
      />

      <Stack direction={{ xs: 'column', sm: 'row' }} spacing={1} sx={{ mb: 2 }}>
        <TextField
          label="Filter descriptors"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder='metric.type="custom.googleapis.com/x"'
          size="small"
          fullWidth
        />
        <Button variant="outlined" onClick={() => void descriptors.refetch()}>
          Refresh
        </Button>
      </Stack>

      {descriptors.isError && <Alert severity="error">Failed to load metric descriptors.</Alert>}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Metric type</TableCell>
              <TableCell>Kind</TableCell>
              <TableCell>Value type</TableCell>
              <TableCell>Unit</TableCell>
              <TableCell>Monitored resources</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {descriptors.isLoading && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!descriptors.isLoading && descriptorRows.length === 0 && (
              <TableRow>
                <TableCell colSpan={5} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No metric descriptors matched.
                </TableCell>
              </TableRow>
            )}
            {descriptorRows.map((d) => (
              <TableRow
                key={d.type}
                hover
                selected={selected === d.type}
                onClick={() => setSelected(d.type === selected ? '' : d.type)}
                sx={{ cursor: 'pointer' }}
              >
                <TableCell sx={{ fontFamily: 'monospace', fontSize: 12 }}>{d.type}</TableCell>
                <TableCell>{d.metricKind || '—'}</TableCell>
                <TableCell>{d.valueType || '—'}</TableCell>
                <TableCell>{d.unit || '—'}</TableCell>
                <TableCell>{(d.monitoredResourceTypes ?? []).join(', ') || '—'}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      {selected && (
        <Box sx={{ mt: 3 }}>
          <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 1 }}>
            <Typography variant="h6">Time series</Typography>
            <Chip size="small" label={selected} variant="outlined" />
          </Stack>
          {series.isError && <Alert severity="error">Failed to load time series.</Alert>}
          <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
            <Table size="small">
              <TableHead>
                <TableRow>
                  <TableCell>Series</TableCell>
                  <TableCell>Points</TableCell>
                  <TableCell>Latest value</TableCell>
                  <TableCell>Latest point</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {series.isLoading && (
                  <TableRow>
                    <TableCell colSpan={4} align="center" sx={{ py: 4 }}>
                      <CircularProgress size={24} />
                    </TableCell>
                  </TableRow>
                )}
                {!series.isLoading && seriesRows.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={4} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                      No time series written for this metric.
                    </TableCell>
                  </TableRow>
                )}
                {seriesRows.map((ts, i) => {
                  const latest = ts.points?.[ts.points.length - 1]
                  return (
                    <TableRow key={`${seriesLabel(ts)}-${i}`} hover>
                      <TableCell>{seriesLabel(ts)}</TableCell>
                      <TableCell>{ts.points?.length ?? 0}</TableCell>
                      <TableCell>{formatTypedValue(latest?.value)}</TableCell>
                      <TableCell>{shortDate(latest?.interval?.endTime)}</TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </TableContainer>
        </Box>
      )}
    </Box>
  )
}
