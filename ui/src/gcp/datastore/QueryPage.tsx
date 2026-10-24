import { useEffect, useRef, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { Alert, Button, Link, Stack, Typography } from '@mui/material'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import { Link as RouterLink, useSearchParams } from 'react-router-dom'
import { runQuery, type DatastoreEntity } from '../../api/gcp/datastore'
import { useAccount } from '../../context/AccountContext'
import { DatastoreTabs } from './DatastoreTabs'
import { entityHref } from './entityRoute'
import { keyPathToString, propertiesSummary } from './key'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'

/** Datastore GQL query runner. */
export function QueryPage() {
  const { accountId } = useAccount()
  const [searchParams] = useSearchParams()
  const [gql, setGql] = useState('SELECT * FROM Task')
  const [allowLiterals, setAllowLiterals] = useState(true)

  // Deep link from a kind page (`?kind=Task`): pre-fill the query once.
  const kindParam = searchParams.get('kind')
  const appliedKind = useRef<string | null>(null)
  useEffect(() => {
    if (kindParam && appliedKind.current !== kindParam) {
      appliedKind.current = kindParam
      setGql(`SELECT * FROM ${kindParam}`)
    }
  }, [kindParam])

  const execute = useMutation({
    mutationFn: () =>
      runQuery({ queryString: gql, allowLiterals }),
  })

  const result = execute.data
  const entities = result?.entities ?? []

  const columns: GcpColumn<DatastoreEntity>[] = [
    {
      key: 'key',
      header: 'Key',
      render: (entity) => (
        <Link component={RouterLink} to={entityHref(entity.key)} sx={{ overflowWrap: 'anywhere' }}>
          {keyPathToString(entity.key)}
        </Link>
      ),
    },
    {
      key: 'properties',
      header: 'Properties',
      render: (entity) => (
        <Typography
          variant="body2"
          sx={{ fontFamily: 'monospace', fontSize: 12, overflowWrap: 'anywhere' }}
        >
          {propertiesSummary(entity.properties)}
        </Typography>
      ),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="datastore"
        title="Query"
        subtitle={`GQL query runner · project ${accountId || '—'}`}
      >
        <DatastoreTabs />
      </GcpPageHeader>

      <GcpCodeEditor
        label="GQL query"
        value={gql}
        onChange={setGql}
        language="text"
        minRows={4}
      />

      {execute.isError && (
        <Alert severity="error" sx={{ mt: 2 }}>
          Query failed: {(execute.error as Error).message}
        </Alert>
      )}

      <Stack direction="row" spacing={2} sx={{ alignItems: 'center', my: 2 }}>
        <Button
          variant="contained"
          startIcon={<PlayArrowIcon />}
          onClick={() => execute.mutate()}
          disabled={execute.isPending || gql.trim() === ''}
        >
          Run query
        </Button>
        <Button
          size="small"
          onClick={() => setAllowLiterals((current) => !current)}
          variant={allowLiterals ? 'outlined' : 'text'}
        >
          Allow literals: {allowLiterals ? 'on' : 'off'}
        </Button>
        {result && (
          <Typography variant="body2" color="text.secondary">
            {entities.length} entit{entities.length === 1 ? 'y' : 'ies'}
            {result.skippedResults ? ` · ${result.skippedResults} skipped` : ''}
            {result.moreResults ? ' · more results available' : ''}
          </Typography>
        )}
      </Stack>

      <GcpDataTable
        aria-label="Query results"
        columns={columns}
        rows={entities}
        getRowKey={(entity) => JSON.stringify(entity.key)}
        emptyMessage={execute.isSuccess ? 'No entities matched the query.' : 'Run a query to see results.'}
        rowsPerPage={0}
      />
    </Stack>
  )
}
