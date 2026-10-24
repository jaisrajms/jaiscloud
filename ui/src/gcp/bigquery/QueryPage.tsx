import { useEffect, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  FormControl,
  FormControlLabel,
  InputLabel,
  Link,
  MenuItem,
  Select,
  Stack,
  Switch,
  Typography,
} from '@mui/material'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import { Link as RouterLink, useSearchParams } from 'react-router-dom'
import {
  listDatasets,
  runQuery,
  type QueryResponse,
  type QueryRequest,
} from '../../api/gcp/bigquery'
import { useAccount } from '../../context/AccountContext'
import { BigQueryTabs } from './BigQueryTabs'
import { cellValue, schemaFields, type SchemaField } from './util'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'

function cellsOf(row: Record<string, unknown>): unknown[] {
  return Array.isArray(row.f) ? row.f : []
}

/** One result row with its stable index (TableRows carry no id of their own). */
interface ResultRow {
  index: number
  cells: unknown[]
}

/** The result set's columns: the response schema, or f0..fN inferred from the
 * first row when the query returned no schema. */
function resultColumns(result: QueryResponse | undefined): {
  names: string[]
  fields: SchemaField[]
} {
  const fields = result?.schema ? schemaFields({ schema: result.schema }) : []
  if (fields.length > 0) {
    return { names: fields.map((f) => f.name), fields }
  }
  const first = result?.rows?.[0]
  const count = first ? cellsOf(first).length : 0
  return { names: Array.from({ length: count }, (_unused, i) => `f${i}`), fields: [] }
}

/** BigQuery SQL workspace: run standard SQL via jobs.query. */
export function QueryPage() {
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const [searchParams] = useSearchParams()
  const [sql, setSql] = useState('SELECT 1')
  const [dataset, setDataset] = useState('')
  const [dryRun, setDryRun] = useState(false)
  const applied = useRef<string | null>(null)

  const datasets = useQuery({
    queryKey: ['gcp', 'bigquery', 'datasets', accountId],
    queryFn: () => listDatasets(),
  })

  // Deep link from a table page (`?datasetId=&tableId=`): seed the editor and
  // default dataset. Keyed by the param value so a new deep link re-applies.
  const deepLinkKey = `${searchParams.get('datasetId') ?? ''}|${searchParams.get('tableId') ?? ''}`
  useEffect(() => {
    const ds = searchParams.get('datasetId') ?? ''
    const table = searchParams.get('tableId') ?? ''
    if (!ds && !table) return
    if (applied.current === deepLinkKey) return
    applied.current = deepLinkKey
    if (ds) setDataset(ds)
    if (ds && table) setSql(`SELECT * FROM \`${ds}.${table}\` LIMIT 100`)
  }, [searchParams, deepLinkKey])

  const execute = useMutation({
    mutationFn: (body: QueryRequest) => runQuery(body),
    // A non-dry-run query creates a job and DML/DDL mutates data, so refresh
    // the BigQuery caches; a dry run changes nothing.
    onSuccess: (_data, variables) => {
      if (!variables.dryRun) {
        void queryClient.invalidateQueries({ queryKey: ['gcp', 'bigquery'] })
      }
    },
  })

  const result = execute.data
  const rows: ResultRow[] = (result?.rows ?? []).map((row, index) => ({
    index,
    cells: cellsOf(row),
  }))
  const { names, fields } = resultColumns(result)
  const isSelect = fields.length > 0 || rows.length > 0
  // A SELECT (even a zero-row one) carries a schema; DDL/DML responses do not.
  const hasResultSet = Boolean(result && (result.schema || rows.length > 0))
  // The run that produced the current result — the dry-run toggle may have
  // changed since, and a dry run never persists its job.
  const ranDryRun = Boolean(execute.variables?.dryRun)

  const columns: GcpColumn<ResultRow>[] = names.map((name, index) => ({
    // Key by position: duplicate aliases (`SELECT 1 AS x, 2 AS x`) are legal.
    key: `f${index}`,
    header: name || `f${index}`,
    render: (row) => (
      <Typography
        variant="body2"
        sx={{ fontFamily: 'monospace', fontSize: 12, overflowWrap: 'anywhere' }}
      >
        {cellValue(row.cells[index])}
      </Typography>
    ),
  }))

  const jobId = result?.jobReference?.jobId

  return (
    <Stack>
      <GcpPageHeader
        id="bigquery"
        title="Query"
        subtitle={`SQL workspace · project ${accountId || '—'}`}
      >
        <BigQueryTabs />
      </GcpPageHeader>

      <GcpCodeEditor
        label="SQL query (standard SQL)"
        value={sql}
        onChange={setSql}
        language="text"
        minRows={6}
        ariaLabel="SQL query"
      />

      {execute.isError && (
        <Alert severity="error" sx={{ mt: 2 }}>
          Query failed: {(execute.error as Error).message}
        </Alert>
      )}

      <Stack
        direction={{ xs: 'column', sm: 'row' }}
        spacing={2}
        sx={{ alignItems: { sm: 'center' }, my: 2 }}
      >
        <Button
          variant="contained"
          startIcon={<PlayArrowIcon />}
          onClick={() =>
            execute.mutate({
              query: sql,
              defaultDataset: dataset ? { datasetId: dataset } : undefined,
              dryRun,
            })
          }
          disabled={execute.isPending || sql.trim() === ''}
        >
          Run query
        </Button>
        <FormControl size="small" sx={{ minWidth: 200 }}>
          <InputLabel id="bq-query-dataset-label">Default dataset</InputLabel>
          <Select
            labelId="bq-query-dataset-label"
            label="Default dataset"
            value={dataset}
            onChange={(event) => setDataset(event.target.value)}
          >
            <MenuItem value="">
              <em>None</em>
            </MenuItem>
            {(datasets.data?.datasets ?? []).map((item) => (
              <MenuItem key={item.datasetId} value={item.datasetId}>
                {item.datasetId}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
        <FormControlLabel
          control={<Switch checked={dryRun} onChange={(e) => setDryRun(e.target.checked)} />}
          label="Dry run"
        />
      </Stack>

      {result && (
        <Stack direction="row" spacing={2} sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap' }}>
          <Typography variant="body2" color="text.secondary">
            {result.statementType || 'SELECT'}
            {isSelect ? ` · ${result.totalRows ?? rows.length} row(s)` : ''}
            {result.numDmlAffectedRows ? ` · ${result.numDmlAffectedRows} row(s) affected` : ''}
            {ranDryRun ? ' · dry run (not executed)' : ''}
          </Typography>
          {jobId && !ranDryRun && (
            <Link component={RouterLink} to={`/gcp/bigquery/jobs/${encodeURIComponent(jobId)}`}>
              Job {jobId}
            </Link>
          )}
        </Stack>
      )}

      {hasResultSet && (
        <GcpDataTable
          aria-label="Query results"
          columns={columns}
          rows={rows}
          getRowKey={(row) => String(row.index)}
          emptyMessage="The query returned no rows."
          rowsPerPage={0}
        />
      )}
    </Stack>
  )
}
