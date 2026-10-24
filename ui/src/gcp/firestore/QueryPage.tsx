import { useEffect, useRef, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Alert,
  Autocomplete,
  Box,
  Button,
  Checkbox,
  FormControl,
  FormControlLabel,
  IconButton,
  InputLabel,
  Link,
  MenuItem,
  Paper,
  Select,
  Stack,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Tooltip,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import { Link as RouterLink, useSearchParams } from 'react-router-dom'
import {
  listCollections,
  listSubcollections,
  runQuery,
  type FirestoreDocument,
  type RunQueryRequest,
} from '../../api/gcp/firestore'
import { useAccount } from '../../context/AccountContext'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { FirestoreTabs } from './FirestoreTabs'
import {
  buildStructuredQuery,
  collectionQueryTarget,
  FILTER_OPERATORS,
  firestoreValueToText,
  scopeDocumentTarget,
  type FilterOperator,
  type QueryFilter,
  type QueryOrder,
} from './query'
import { encodeFirestoreId, encodeFirestorePath, parseJsonObject } from './util'

const DEFAULT_RAW = `{
  "from": [
    { "collectionId": "users" }
  ]
}`

/** Firestore query runner: a structured builder over the core `RunQuery` plus a
 * raw StructuredQuery editor, rendering the matching documents. */
export function QueryPage() {
  const { accountId } = useAccount()
  const [searchParams] = useSearchParams()
  const [mode, setMode] = useState<'builder' | 'raw'>('builder')
  const [scope, setScope] = useState('')
  const [collectionId, setCollectionId] = useState('')
  const [allDescendants, setAllDescendants] = useState(false)
  const [filters, setFilters] = useState<QueryFilter[]>([])
  const [orderBy, setOrderBy] = useState<QueryOrder[]>([])
  const [limit, setLimit] = useState('')
  const [offset, setOffset] = useState('')
  const [raw, setRaw] = useState(DEFAULT_RAW)
  const [fieldsView, setFieldsView] = useState<'formatted' | 'raw'>('formatted')
  const [formError, setFormError] = useState<string | null>(null)

  // Deep link from a collection (`?collection=users`, or a nested
  // `users/alice/orders`): pre-fill the builder's collection id and parent
  // scope. Re-applies only when the param value changes, so subsequent edits in
  // the form are not clobbered.
  const collectionParam = searchParams.get('collection')
  const appliedTarget = useRef<string | null>(null)
  useEffect(() => {
    if (!collectionParam || appliedTarget.current === collectionParam) return
    appliedTarget.current = collectionParam
    const target = collectionQueryTarget(collectionParam)
    if (!target) return
    setMode('builder')
    setCollectionId(target.collectionId)
    setScope(target.scope)
  }, [collectionParam])

  const execute = useMutation({
    mutationFn: (body: RunQueryRequest) => runQuery(body),
  })

  // Collection-id suggestions: the root collections, or the parent scope's
  // subcollections once a document scope is set. `freeSolo` keeps the field
  // typable so a not-yet-created id (and a collection group) still works.
  const scopeTarget = scopeDocumentTarget(scope.trim())
  const rootCollections = useQuery({
    queryKey: ['gcp', 'firestore', 'collections', accountId],
    queryFn: () => listCollections(),
    enabled: !scopeTarget,
  })
  const subcollections = useQuery({
    queryKey: [
      'gcp',
      'firestore',
      'subcollections',
      scopeTarget?.collection,
      scopeTarget?.document,
      accountId,
    ],
    queryFn: () => listSubcollections(scopeTarget!.collection, scopeTarget!.document),
    enabled: Boolean(scopeTarget),
  })
  const collectionOptions =
    (scopeTarget ? subcollections.data?.collections : rootCollections.data?.collections)?.map(
      (collection) => collection.id,
    ) ?? []

  const run = () => {
    setFormError(null)
    if (mode === 'raw') {
      const parsed = parseJsonObject(raw)
      if (parsed.error || !parsed.value) {
        setFormError(parsed.error ?? 'Invalid structured query')
        return
      }
      execute.mutate({ scope: scope.trim() || undefined, structuredQuery: parsed.value })
      return
    }
    const { structuredQuery, error } = buildStructuredQuery({
      collectionId,
      allDescendants,
      filters,
      orderBy,
      limit,
      offset,
    })
    if (error || !structuredQuery) {
      setFormError(error ?? 'Invalid query')
      return
    }
    execute.mutate({ scope: scope.trim() || undefined, structuredQuery })
  }

  const addFilter = () =>
    setFilters((current) => [...current, { field: '', operator: 'EQUAL', value: '' }])
  const updateFilter = (index: number, patch: Partial<QueryFilter>) =>
    setFilters((current) => current.map((f, i) => (i === index ? { ...f, ...patch } : f)))
  const removeFilter = (index: number) =>
    setFilters((current) => current.filter((_f, i) => i !== index))

  const addOrder = () =>
    setOrderBy((current) => [...current, { field: '', direction: 'ASCENDING' }])
  const updateOrder = (index: number, patch: Partial<QueryOrder>) =>
    setOrderBy((current) => current.map((o, i) => (i === index ? { ...o, ...patch } : o)))
  const removeOrder = (index: number) =>
    setOrderBy((current) => current.filter((_o, i) => i !== index))

  const result = execute.data
  const docs = result?.documents ?? []

  const columns: GcpColumn<FirestoreDocument>[] = [
    {
      key: 'id',
      header: 'Document ID',
      sortable: true,
      sortValue: (document) => document.id,
      render: (document) => (
        <Link
          component={RouterLink}
          to={`/gcp/firestore/collections/${encodeFirestorePath(document.collection)}/documents/${encodeFirestoreId(document.id)}`}
        >
          {document.id}
        </Link>
      ),
    },
    {
      key: 'collection',
      header: 'Collection',
      sortable: true,
      sortValue: (document) => document.collection,
      render: (document) => document.collection,
    },
    {
      key: 'fields',
      header: 'Fields',
      render: (document) =>
        fieldsView === 'raw' ? (
          <Typography
            variant="body2"
            sx={{
              fontFamily: 'monospace',
              fontSize: 12,
              maxWidth: 560,
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {JSON.stringify(document.fields)}
          </Typography>
        ) : (
          <Stack spacing={0.25}>
            {Object.entries(document.fields)
              .slice(0, 6)
              .map(([key, value]) => (
                <Typography
                  key={key}
                  variant="body2"
                  sx={{
                    maxWidth: 560,
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                  }}
                >
                  <Box component="span" sx={{ color: 'text.secondary' }}>
                    {key}:
                  </Box>{' '}
                  {firestoreValueToText(value)}
                </Typography>
              ))}
            {Object.keys(document.fields).length === 0 && (
              <Typography variant="body2" color="text.secondary">
                (no fields)
              </Typography>
            )}
            {Object.keys(document.fields).length > 6 && (
              <Typography variant="caption" color="text.secondary">
                +{Object.keys(document.fields).length - 6} more
              </Typography>
            )}
          </Stack>
        ),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="firestore"
        title="Query"
        subtitle={`Firestore query runner · project ${accountId || '—'}`}
      >
        <FirestoreTabs />
      </GcpPageHeader>

      <Paper variant="outlined" sx={{ p: 2, mb: 2 }}>
        <Stack spacing={2}>
          <ToggleButtonGroup
            exclusive
            size="small"
            value={mode}
            onChange={(_event, next: 'builder' | 'raw' | null) => {
              if (next) setMode(next)
            }}
            aria-label="Query mode"
          >
            <ToggleButton value="builder">Structured</ToggleButton>
            <ToggleButton value="raw">Raw JSON</ToggleButton>
          </ToggleButtonGroup>

          <TextField
            size="small"
            label="Parent scope (optional)"
            value={scope}
            onChange={(event) => setScope(event.target.value)}
            placeholder="cities/SF"
            helperText="Document path to scope a subcollection or collection-group query. Leave blank for the whole database."
            fullWidth
          />

          {mode === 'builder' ? (
            <>
              <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
                <Autocomplete
                  freeSolo
                  autoHighlight
                  options={collectionOptions}
                  inputValue={collectionId}
                  onInputChange={(_event, value) => setCollectionId(value)}
                  sx={{ flex: 1 }}
                  renderInput={(params) => (
                    <TextField
                      {...params}
                      size="small"
                      label="Collection ID"
                      placeholder="users"
                      helperText={
                        scopeTarget
                          ? 'Subcollections of the parent scope'
                          : 'Existing root collections (or type a new id)'
                      }
                    />
                  )}
                />
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={allDescendants}
                      onChange={(event) => setAllDescendants(event.target.checked)}
                    />
                  }
                  label="Collection group (all descendants)"
                />
              </Stack>

              <Stack spacing={1}>
                <Typography variant="subtitle2">Filters</Typography>
                {filters.length === 0 && (
                  <Typography variant="body2" color="text.secondary">
                    No filters — all documents in the collection are returned.
                  </Typography>
                )}
                {filters.map((filter, index) => (
                  <Stack key={index} direction="row" spacing={1} sx={{ alignItems: 'center' }}>
                    <TextField
                      size="small"
                      label="Field"
                      value={filter.field}
                      onChange={(event) => updateFilter(index, { field: event.target.value })}
                      sx={{ flex: 1 }}
                    />
                    <FormControl size="small" sx={{ minWidth: 160 }}>
                      <InputLabel id={`op-${index}`}>Operator</InputLabel>
                      <Select
                        labelId={`op-${index}`}
                        label="Operator"
                        value={filter.operator}
                        onChange={(event) =>
                          updateFilter(index, { operator: event.target.value as FilterOperator })
                        }
                      >
                        {FILTER_OPERATORS.map((op) => (
                          <MenuItem key={op.value} value={op.value}>
                            {op.label}
                          </MenuItem>
                        ))}
                      </Select>
                    </FormControl>
                    <TextField
                      size="small"
                      label="Value"
                      value={filter.value}
                      onChange={(event) => updateFilter(index, { value: event.target.value })}
                      helperText='JSON, e.g. 42, "Ada", ["a","b"]'
                      sx={{ flex: 1 }}
                    />
                    <Tooltip title="Remove filter">
                      <IconButton size="small" aria-label="Remove filter" onClick={() => removeFilter(index)}>
                        <DeleteOutlineIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </Stack>
                ))}
                <Button size="small" startIcon={<AddIcon />} onClick={addFilter} sx={{ alignSelf: 'flex-start' }}>
                  Add filter
                </Button>
              </Stack>

              <Stack spacing={1}>
                <Typography variant="subtitle2">Order by</Typography>
                {orderBy.map((order, index) => (
                  <Stack key={index} direction="row" spacing={1} sx={{ alignItems: 'center' }}>
                    <TextField
                      size="small"
                      label="Field"
                      value={order.field}
                      onChange={(event) => updateOrder(index, { field: event.target.value })}
                      sx={{ flex: 1 }}
                    />
                    <FormControl size="small" sx={{ minWidth: 160 }}>
                      <InputLabel id={`dir-${index}`}>Direction</InputLabel>
                      <Select
                        labelId={`dir-${index}`}
                        label="Direction"
                        value={order.direction}
                        onChange={(event) =>
                          updateOrder(index, {
                            direction: event.target.value as QueryOrder['direction'],
                          })
                        }
                      >
                        <MenuItem value="ASCENDING">Ascending</MenuItem>
                        <MenuItem value="DESCENDING">Descending</MenuItem>
                      </Select>
                    </FormControl>
                    <Tooltip title="Remove order">
                      <IconButton size="small" aria-label="Remove order" onClick={() => removeOrder(index)}>
                        <DeleteOutlineIcon fontSize="small" />
                      </IconButton>
                    </Tooltip>
                  </Stack>
                ))}
                <Button size="small" startIcon={<AddIcon />} onClick={addOrder} sx={{ alignSelf: 'flex-start' }}>
                  Add order
                </Button>
              </Stack>

              <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
                <TextField
                  size="small"
                  label="Limit"
                  value={limit}
                  onChange={(event) => setLimit(event.target.value)}
                  placeholder="10"
                  fullWidth
                />
                <TextField
                  size="small"
                  label="Offset"
                  value={offset}
                  onChange={(event) => setOffset(event.target.value)}
                  placeholder="0"
                  fullWidth
                />
              </Stack>
            </>
          ) : (
            <GcpCodeEditor
              label="StructuredQuery (JSON)"
              value={raw}
              onChange={setRaw}
              language="json"
              minRows={10}
            />
          )}

          {formError && <Alert severity="error">{formError}</Alert>}
          {execute.isError && (
            <Alert severity="error">
              Query failed: {(execute.error as Error).message}
            </Alert>
          )}

          <Stack direction="row" spacing={1} sx={{ alignItems: 'center' }}>
            <Button variant="contained" startIcon={<PlayArrowIcon />} onClick={run} disabled={execute.isPending}>
              Run query
            </Button>
            {result && (
              <Typography variant="body2" color="text.secondary">
                {docs.length} document{docs.length === 1 ? '' : 's'}
                {result.skippedResults ? ` · ${result.skippedResults} skipped` : ''}
                {result.readTime ? ` · read at ${result.readTime}` : ''}
              </Typography>
            )}
          </Stack>
        </Stack>
      </Paper>

      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 1 }}>
        <Typography variant="body2" color="text.secondary">
          Fields:
        </Typography>
        <ToggleButtonGroup
          exclusive
          size="small"
          value={fieldsView}
          onChange={(_event, next: 'formatted' | 'raw' | null) => {
            if (next) setFieldsView(next)
          }}
          aria-label="Fields display"
        >
          <ToggleButton value="formatted">Formatted</ToggleButton>
          <ToggleButton value="raw">Raw JSON</ToggleButton>
        </ToggleButtonGroup>
      </Stack>

      <GcpDataTable
        aria-label="Query results"
        columns={columns}
        rows={docs}
        getRowKey={(document) => document.name || `${document.collection}/${document.id}`}
        emptyMessage={
          execute.isSuccess ? 'No documents matched the query.' : 'Run a query to see results.'
        }
        rowsPerPage={0}
        renderDetail={(document) => (
          <Stack spacing={1}>
            {Object.entries(document.fields).map(([key, value]) => (
              <Typography key={key} variant="body2" sx={{ overflowWrap: 'anywhere' }}>
                <Box component="span" sx={{ color: 'text.secondary' }}>
                  {key}:
                </Box>{' '}
                {firestoreValueToText(value)}
              </Typography>
            ))}
            {Object.keys(document.fields).length === 0 && (
              <Typography variant="body2" color="text.secondary">
                (no fields)
              </Typography>
            )}
            <Typography variant="caption" color="text.secondary" sx={{ pt: 1 }}>
              Raw
            </Typography>
            <Box
              component="pre"
              sx={{
                m: 0,
                p: 1,
                bgcolor: 'action.hover',
                borderRadius: 1,
                fontSize: 12,
                overflow: 'auto',
              }}
            >
              {JSON.stringify(document.fields, null, 2)}
            </Box>
          </Stack>
        )}
        detailTitle={(document) => document.id}
      />
    </Stack>
  )
}
