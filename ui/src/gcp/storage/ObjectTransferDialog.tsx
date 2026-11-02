import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Stack,
  TextField,
} from '@mui/material'
import {
  composeObject,
  copyObject,
  moveObject,
  rewriteObjectFully,
  type RewriteResponse,
} from '../../api/gcp/storage'

export type TransferMode = 'copy' | 'rewrite' | 'move' | 'compose'

export interface ObjectTransferDialogProps {
  open: boolean
  onClose: () => void
  bucket: string
  mode: TransferMode
  /** Source object for copy/rewrite/move and the preselected compose source. */
  sourceObject?: string
}

function titleFor(mode: TransferMode): string {
  switch (mode) {
    case 'copy':
      return 'Copy object'
    case 'rewrite':
      return 'Rewrite object'
    case 'move':
      return 'Move object'
    default:
      return 'Compose object'
  }
}

function buttonFor(mode: TransferMode): string {
  switch (mode) {
    case 'copy':
      return 'Copy'
    case 'rewrite':
      return 'Rewrite'
    case 'move':
      return 'Move'
    default:
      return 'Compose'
  }
}

/**
 * Copy, rewrite, move or compose an object. Rewrite follows rewriteToken until
 * the provider reports done; move uses the provider's objects.move verb (not a
 * console-side copy+delete).
 */
export function ObjectTransferDialog({
  open,
  onClose,
  bucket,
  mode,
  sourceObject = '',
}: ObjectTransferDialogProps) {
  const queryClient = useQueryClient()
  const [destinationBucket, setDestinationBucket] = useState(bucket)
  const [destinationObject, setDestinationObject] = useState(sourceObject)
  const [sources, setSources] = useState(sourceObject)
  const [contentType, setContentType] = useState('')
  const [ifGenerationMatch, setIfGenerationMatch] = useState('')
  const [progress, setProgress] = useState<RewriteResponse | null>(null)
  const [error, setError] = useState('')

  useEffect(() => {
    if (!open) return
    setDestinationBucket(bucket)
    setDestinationObject(sourceObject)
    setSources(sourceObject)
    setContentType('')
    setIfGenerationMatch('')
    setProgress(null)
    setError('')
  }, [open, bucket, sourceObject])

  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'objects'] })
    void queryClient.invalidateQueries({ queryKey: ['gcp', 'storage', 'object'] })
  }

  const run = useMutation({
    mutationFn: async () => {
      if (mode === 'compose') {
        const sourceObjects = sources
          .split('\n')
          .map((s) => s.trim())
          .filter(Boolean)
          .map((name) => ({ name }))
        if (!destinationObject.trim() || sourceObjects.length === 0) {
          throw new Error('Destination and at least one source object are required.')
        }
        return composeObject(bucket, {
          destinationObject: destinationObject.trim(),
          sourceObjects,
          destination: contentType.trim() ? { contentType: contentType.trim() } : undefined,
          ifGenerationMatch: ifGenerationMatch.trim() || undefined,
        })
      }
      if (!sourceObject.trim() || !destinationObject.trim()) {
        throw new Error('Source and destination object names are required.')
      }
      const body = {
        sourceObject,
        destinationBucket: destinationBucket.trim() || bucket,
        destinationObject: destinationObject.trim(),
        ifGenerationMatch: ifGenerationMatch.trim() || undefined,
      }
      return mode === 'move'
        ? moveObject(bucket, body)
        : mode === 'copy'
          ? copyObject(bucket, body)
          : rewriteObjectFully(bucket, body, setProgress)
    },
    onSuccess: () => {
      invalidate()
      onClose()
    },
  })

  const handleSubmit = () => {
    setError('')
    run.mutate()
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>{titleFor(mode)}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {run.isError && (
            <Alert severity="error">{(run.error as Error).message}</Alert>
          )}
          {error && <Alert severity="error">{error}</Alert>}
          {progress && (
            <Alert severity={progress.done ? 'success' : 'info'}>
              {progress.done
                ? `Rewrite complete (${progress.objectSize ?? '?'} bytes).`
                : `Rewriting ${progress.totalBytesRewritten ?? '0'} bytes…`}
            </Alert>
          )}

          {mode === 'compose' ? (
            <>
              <TextField
                label="Source objects (one per line)"
                value={sources}
                onChange={(e) => setSources(e.target.value)}
                multiline
                minRows={3}
                helperText="All sources must be in this bucket."
                fullWidth
              />
              <TextField
                label="Destination object"
                value={destinationObject}
                onChange={(e) => setDestinationObject(e.target.value)}
                fullWidth
              />
              <TextField
                label="Destination content type (optional)"
                value={contentType}
                onChange={(e) => setContentType(e.target.value)}
                placeholder="application/octet-stream"
                fullWidth
              />
            </>
          ) : (
            <>
              <TextField
                label="Source object"
                value={sourceObject}
                disabled
                fullWidth
              />
              <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
                <TextField
                  label="Destination bucket"
                  value={destinationBucket}
                  onChange={(e) => setDestinationBucket(e.target.value)}
                  fullWidth
                />
                <TextField
                  label="Destination object"
                  value={destinationObject}
                  onChange={(e) => setDestinationObject(e.target.value)}
                  fullWidth
                />
              </Stack>
            </>
          )}

          <TextField
            label="ifGenerationMatch (optional)"
            value={ifGenerationMatch}
            onChange={(e) => setIfGenerationMatch(e.target.value)}
            helperText="Fail with 412 when the destination generation does not match."
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={run.isPending} onClick={handleSubmit}>
          {buttonFor(mode)}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
