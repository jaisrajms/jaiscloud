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
import { updateCryptoKey, type CryptoKey, type UpdateCryptoKeyRequest } from '../../api/gcp/kms'

export interface EditCryptoKeyDialogProps {
  open: boolean
  onClose: () => void
  location: string
  keyRing: string
  cryptoKeyID: string
  cryptoKey: CryptoKey
}

const labelLines = (labels?: Record<string, string>) =>
  labels ? Object.entries(labels).map(([k, v]) => `${k}=${v}`).join('\n') : ''

/** parses `key=value` lines, throwing on a line without `=`. */
function parseKeyValueLines(text: string): Record<string, string> {
  const out: Record<string, string> = {}
  for (const raw of text.split('\n')) {
    const line = raw.trim()
    if (!line || line.startsWith('#')) continue
    const at = line.indexOf('=')
    if (at <= 0) throw new Error(`Expected key=value, got "${line}"`)
    out[line.slice(0, at).trim()] = line.slice(at + 1).trim()
  }
  return out
}

/** rotationPeriod is a protobuf duration (e.g. 7776000s); render it as days. */
function daysFromDuration(value?: string): string {
  const match = /^(\d+(?:\.\d+)?)s$/.exec(value ?? '')
  return match ? String(Number(match[1]) / 86400) : ''
}

function durationFromDays(value: string): string {
  return `${Math.round(Number(value) * 86400)}s`
}

/** datetime-local value from an RFC3339 timestamp. */
function localInput(value?: string): string {
  return value ? value.slice(0, 16) : ''
}

/**
 * Edit a crypto key's labels and rotation schedule. The mask carries exactly the
 * fields the user touched; the core validates the rotation invariants and its
 * InvalidArgument messages surface verbatim.
 */
export function EditCryptoKeyDialog({
  open,
  onClose,
  location,
  keyRing,
  cryptoKeyID,
  cryptoKey,
}: EditCryptoKeyDialogProps) {
  const queryClient = useQueryClient()
  const originalLabels = labelLines(cryptoKey.labels)
  const originalPeriod = daysFromDuration(cryptoKey.rotationPeriod)
  const originalNext = localInput(cryptoKey.nextRotationTime)
  const [labelsText, setLabelsText] = useState(originalLabels)
  const [periodDays, setPeriodDays] = useState(originalPeriod)
  const [nextRotation, setNextRotation] = useState(originalNext)
  const [error, setError] = useState('')

  useEffect(() => {
    if (!open) return
    setLabelsText(labelLines(cryptoKey.labels))
    setPeriodDays(daysFromDuration(cryptoKey.rotationPeriod))
    setNextRotation(localInput(cryptoKey.nextRotationTime))
    setError('')
  }, [open, cryptoKey])

  const save = useMutation({
    mutationFn: (body: UpdateCryptoKeyRequest) =>
      updateCryptoKey(location, keyRing, cryptoKeyID, body),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'kms'] })
      onClose()
    },
  })

  const rotates = cryptoKey.purpose === 'ENCRYPT_DECRYPT'

  const handleSave = () => {
    setError('')
    const mask: string[] = []
    const body: UpdateCryptoKeyRequest = {}
    try {
      if (labelsText !== originalLabels) {
        body.labels = labelsText.trim() ? parseKeyValueLines(labelsText) : {}
        mask.push('labels')
      }
    } catch (err) {
      setError((err as Error).message)
      return
    }
    if (rotates) {
      if (periodDays !== originalPeriod) {
        if (periodDays.trim()) {
          const days = Number(periodDays)
          if (!Number.isFinite(days) || days < 1) {
            setError('Rotation period must be at least 1 day (Cloud KMS requires 24h).')
            return
          }
          body.rotationPeriod = durationFromDays(periodDays)
        }
        mask.push('rotationPeriod')
      }
      if (nextRotation !== originalNext) {
        if (nextRotation) {
          const date = new Date(nextRotation)
          if (Number.isNaN(date.getTime())) {
            setError('Next rotation time is invalid.')
            return
          }
          body.nextRotationTime = date.toISOString()
        }
        mask.push('nextRotationTime')
      }
    }
    if (mask.length === 0) {
      setError('No changes to save.')
      return
    }
    body.updateMask = mask
    save.mutate(body)
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Edit {cryptoKeyID}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">
              Could not update the crypto key: {(save.error as Error).message}
            </Alert>
          )}
          {error && <Alert severity="error">{error}</Alert>}
          <TextField
            label="Labels (key=value per line)"
            value={labelsText}
            onChange={(e) => setLabelsText(e.target.value)}
            multiline
            minRows={2}
            fullWidth
          />
          {rotates ? (
            <>
              <TextField
                label="Rotation period (days)"
                value={periodDays}
                onChange={(e) => setPeriodDays(e.target.value)}
                helperText="Minimum 24 hours. Leave empty with no next rotation to turn rotation off."
                fullWidth
              />
              <TextField
                label="Next rotation time"
                type="datetime-local"
                value={nextRotation}
                onChange={(e) => setNextRotation(e.target.value)}
                slotProps={{ inputLabel: { shrink: true } }}
                fullWidth
              />
            </>
          ) : (
            <Alert severity="info">Only ENCRYPT_DECRYPT keys can rotate.</Alert>
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={save.isPending} onClick={handleSave}>
          Save
        </Button>
      </DialogActions>
    </Dialog>
  )
}
