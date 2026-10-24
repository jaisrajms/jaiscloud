import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Divider,
  Stack,
  Typography,
} from '@mui/material'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import SaveOutlinedIcon from '@mui/icons-material/SaveOutlined'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { APIError } from '../../api/client'
import {
  deleteEntity,
  getEntity,
  upsertEntity,
  type DatastoreValue,
} from '../../api/gcp/datastore'
import { useAccount } from '../../context/AccountContext'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { parseKeyParam } from './entityRoute'
import { keyPathToString } from './key'

/** A single Datastore entity: edit the properties as JSON, save or delete. */
export function EntityDetailPage() {
  const [searchParams] = useSearchParams()
  const key = parseKeyParam(searchParams.get('key'))
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [text, setText] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [saved, setSaved] = useState(false)

  const query = useQuery({
    queryKey: ['gcp', 'datastore', 'entity', key ? JSON.stringify(key) : '', accountId],
    queryFn: () => getEntity(key!),
    enabled: Boolean(key),
  })

  useEffect(() => {
    if (query.data) {
      setText(JSON.stringify(query.data.properties ?? {}, null, 2))
      setError(null)
      setSaved(false)
    }
  }, [query.data])

  const save = useMutation({
    mutationFn: (properties: Record<string, DatastoreValue>) =>
      upsertEntity({ key: key!, properties }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'datastore'] })
      setSaved(true)
    },
  })

  const remove = useMutation({
    mutationFn: () => deleteEntity(key!),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'datastore'] })
      const kind = key?.path[key.path.length - 1]?.kind
      navigate(kind ? `/gcp/datastore/kinds/${encodeURIComponent(kind)}` : '/gcp/datastore/kinds')
    },
  })

  const submit = () => {
    let parsed: unknown
    try {
      parsed = JSON.parse(text)
    } catch {
      setError('Properties must be valid JSON.')
      setSaved(false)
      return
    }
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
      setError('Properties must be a JSON object of Datastore values.')
      setSaved(false)
      return
    }
    setError(null)
    setSaved(false)
    save.mutate(parsed as Record<string, DatastoreValue>)
  }

  if (!key) {
    return <Alert severity="error">Missing or malformed entity key.</Alert>
  }

  const kind = key.path[key.path.length - 1]?.kind ?? ''
  const conflicted = save.error instanceof APIError && save.error.status === 409

  return (
    <Box>
      <GcpPageHeader
        id="datastore"
        title={keyPathToString(key)}
        subtitle={`${kind} · project ${accountId || '—'}`}
        backTo={`/gcp/datastore/kinds/${encodeURIComponent(kind)}`}
        backAriaLabel="Back to entities"
        actions={
          <>
            <Button
              variant="contained"
              startIcon={<SaveOutlinedIcon />}
              disabled={save.isPending || query.isLoading || !query.data}
              onClick={submit}
            >
              Save
            </Button>
            <Button
              color="error"
              startIcon={<DeleteOutlineIcon />}
              disabled={remove.isPending || !query.data}
              onClick={() => remove.mutate()}
            >
              Delete
            </Button>
          </>
        }
      />

      {query.isLoading && <CircularProgress size={24} />}
      {query.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Failed to load the entity.
        </Alert>
      )}
      {conflicted && (
        <Alert severity="warning" sx={{ mb: 2 }}>
          This entity changed since it was loaded. Reload before saving.
        </Alert>
      )}
      {save.isError && !conflicted && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Save failed: {(save.error as Error).message}
        </Alert>
      )}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}
      {saved && !save.isPending && (
        <Alert severity="success" sx={{ mb: 2 }}>
          Entity saved.
        </Alert>
      )}

      <Stack spacing={0.5} sx={{ mb: 2 }}>
        <Typography variant="caption" color="text.secondary">
          Key path
        </Typography>
        <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
          {keyPathToString(key)}
        </Typography>
        <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
          {query.data?.version ? `Version ${query.data.version}` : ''}
          {query.data?.updateTime ? ` · updated ${query.data.updateTime}` : ''}
        </Typography>
        <Divider sx={{ mt: 1 }} />
      </Stack>

      <Box sx={{ maxWidth: 900 }}>
        <GcpCodeEditor
          label="Properties (Datastore value encoding)"
          value={text}
          onChange={setText}
          language="json"
          minRows={16}
          error={error}
        />
        <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
          Saving replaces all properties with the encoded object above.
        </Typography>
      </Box>
    </Box>
  )
}
