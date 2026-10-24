import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  IconButton,
  Link,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import { Link as RouterLink, useParams } from 'react-router-dom'
import {
  deleteTable,
  getDataset,
  listTables,
  type BigQueryTable,
} from '../../api/gcp/bigquery'
import { useAccount } from '../../context/AccountContext'
import { CreateTableDialog } from './CreateTableDialog'
import { formatMillis } from './util'
import { GcpPageTitle } from '../common/PageTitle'

function str(detail: Record<string, unknown> | undefined, key: string): string {
  const v = detail?.[key]
  return typeof v === 'string' ? v : ''
}

/** One BigQuery dataset: its metadata and its tables. */
export function DatasetDetailPage() {
  const { dataset = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)

  const detail = useQuery({
    queryKey: ['gcp', 'bigquery', 'dataset', dataset, accountId],
    queryFn: () => getDataset(dataset),
    enabled: Boolean(dataset),
  })

  const tables = useQuery({
    queryKey: ['gcp', 'bigquery', 'tables', dataset, accountId],
    queryFn: () => listTables(dataset),
    enabled: Boolean(dataset),
  })

  const remove = useMutation({
    mutationFn: (table: BigQueryTable) => deleteTable(dataset, table.tableId),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'bigquery'] }),
  })

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton component={RouterLink} to="/gcp/bigquery/datasets" aria-label="Back to datasets">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="bigquery">{dataset}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            BigQuery dataset · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create table
        </Button>
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the dataset.</Alert>}
      {detail.data && (
        <Box
          sx={{
            border: '1px solid',
            borderColor: 'divider',
            borderRadius: 2,
            p: 2,
            mb: 3,
          }}
        >
          <Stack spacing={0.5}>
            <Typography variant="body2" color="text.secondary">
              Location: {str(detail.data, 'location') || '—'}
            </Typography>
            {str(detail.data, 'friendlyName') && (
              <Typography variant="body2">Friendly name: {str(detail.data, 'friendlyName')}</Typography>
            )}
            {str(detail.data, 'description') && (
              <Typography variant="body2">Description: {str(detail.data, 'description')}</Typography>
            )}
            <Typography variant="body2" color="text.secondary">
              Created: {formatMillis(str(detail.data, 'creationTime'))}
            </Typography>
          </Stack>
        </Box>
      )}

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Table ID</TableCell>
              <TableCell>Type</TableCell>
              <TableCell>Created</TableCell>
              <TableCell align="right">Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {tables.isLoading && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!tables.isLoading && (tables.data?.tables.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No tables in this dataset.
                </TableCell>
              </TableRow>
            )}
            {tables.data?.tables.map((table) => (
              <TableRow key={table.tableId} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/bigquery/datasets/${encodeURIComponent(dataset)}/tables/${encodeURIComponent(table.tableId)}`}
                  >
                    {table.tableId}
                  </Link>
                </TableCell>
                <TableCell>{table.type || 'TABLE'}</TableCell>
                <TableCell>{formatMillis(table.creationTime)}</TableCell>
                <TableCell align="right">
                  <Tooltip title="Delete table">
                    <span>
                      <IconButton
                        size="small"
                        disabled={remove.isPending}
                        onClick={() => remove.mutate(table)}
                        aria-label={`Delete ${table.tableId}`}
                      >
                        <DeleteOutlineIcon fontSize="small" />
                      </IconButton>
                    </span>
                  </Tooltip>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <CreateTableDialog open={createOpen} onClose={() => setCreateOpen(false)} dataset={dataset} />
    </Box>
  )
}
