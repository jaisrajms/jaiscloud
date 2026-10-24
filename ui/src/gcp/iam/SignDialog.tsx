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
  Typography,
} from '@mui/material'
import ContentCopyIcon from '@mui/icons-material/ContentCopy'
import { signServiceAccountBlob, signServiceAccountJwt } from '../../api/gcp/iam'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { base64Encode } from './util'

/** A read-only, copyable signing result with the signing key id. */
function SigningResult({ label, value, keyId }: { label: string; value: string; keyId?: string }) {
  return (
    <Stack spacing={1}>
      <Typography variant="subtitle2">{label}</Typography>
      {keyId && (
        <Typography variant="caption" color="text.secondary">
          Key ID: {keyId}
        </Typography>
      )}
      <GcpCodeEditor value={value} readOnly language="text" minRows={3} ariaLabel={label} />
      <Stack direction="row" sx={{ justifyContent: 'flex-end' }}>
        <Button
          size="small"
          startIcon={<ContentCopyIcon />}
          onClick={() => void navigator.clipboard.writeText(value)}
        >
          Copy
        </Button>
      </Stack>
    </Stack>
  )
}

export interface SignBlobDialogProps {
  open: boolean
  email: string
  onClose: () => void
}

/**
 * Sign an arbitrary payload with the service account's key (IAM signBlob).
 * The payload is base64-encoded client-side; the response is the base64
 * signature the API returns.
 */
export function SignBlobDialog({ open, email, onClose }: SignBlobDialogProps) {
  const queryClient = useQueryClient()
  const [text, setText] = useState('')
  const [result, setResult] = useState<{ signature: string; keyId?: string } | null>(null)

  const sign = useMutation({
    mutationFn: () => signServiceAccountBlob(email, base64Encode(text)),
    // A sign lazily creates the account's key, so refresh the console's view.
    onSuccess: (response) => {
      setResult({ signature: response.signature, keyId: response.keyId })
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'iam'] })
    },
  })

  useEffect(() => {
    if (open) {
      setText('')
      setResult(null)
      sign.reset()
    }
    // Reset only when the dialog opens; the mutation object is stable.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  const submit = () => {
    setResult(null)
    sign.mutate()
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Sign blob</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          <Typography variant="body2" color="text.secondary">
            The payload is signed with the service account&apos;s key and the
            base64 signature is returned (SHA-256 with RSA).
          </Typography>
          {sign.isError && (
            <Alert severity="error">Could not sign: {(sign.error as Error).message}</Alert>
          )}
          <TextField
            autoFocus
            label="Payload"
            value={text}
            onChange={(e) => setText(e.target.value)}
            multiline
            minRows={4}
            fullWidth
          />
          {result && (
            <SigningResult
              label="Signature (base64)"
              value={result.signature}
              keyId={result.keyId}
            />
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
        <Button
          variant="contained"
          disabled={sign.isPending || text.length === 0}
          onClick={submit}
        >
          Sign
        </Button>
      </DialogActions>
    </Dialog>
  )
}

export interface SignJwtDialogProps {
  open: boolean
  email: string
  onClose: () => void
}

/** A claim set with no `exp`: it is optional, and a client-generated one would
 * be validated against the emulator's (possibly frozen) clock. */
function defaultClaims(email: string): string {
  return JSON.stringify({ iss: email, sub: email, aud: 'https://example.googleapis.com/' }, null, 2)
}

/** Mint a signed JWT from a JSON claim set (IAM signJwt). */
export function SignJwtDialog({ open, email, onClose }: SignJwtDialogProps) {
  const queryClient = useQueryClient()
  const [payload, setPayload] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<{ signedJwt: string; keyId?: string } | null>(null)

  const sign = useMutation({
    mutationFn: (claims: string) => signServiceAccountJwt(email, claims),
    onSuccess: (response) => {
      setResult({ signedJwt: response.signedJwt, keyId: response.keyId })
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'iam'] })
    },
  })

  useEffect(() => {
    if (open) {
      setPayload(defaultClaims(email))
      setError(null)
      setResult(null)
      sign.reset()
    }
    // Reset only when the dialog opens; the mutation object is stable.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, email])

  const submit = () => {
    setError(null)
    setResult(null)
    try {
      const parsed: unknown = JSON.parse(payload)
      if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
        setError('Claims must be a JSON object.')
        return
      }
    } catch {
      setError('Claims must be valid JSON.')
      return
    }
    sign.mutate(payload)
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Sign JWT</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          <Typography variant="body2" color="text.secondary">
            Claim set (a JSON object). An optional <code>exp</code> (Unix
            seconds) must be in the future and within 12 hours.
          </Typography>
          {sign.isError && (
            <Alert severity="error">Could not sign: {(sign.error as Error).message}</Alert>
          )}
          <GcpCodeEditor
            autoFocus
            label="Claims"
            value={payload}
            onChange={(value) => {
              setPayload(value)
              setResult(null)
              setError(null)
            }}
            error={error}
            language="json"
            minRows={8}
            ariaLabel="JWT claims"
          />
          {result && <SigningResult label="Signed JWT" value={result.signedJwt} keyId={result.keyId} />}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
        <Button variant="contained" disabled={sign.isPending} onClick={submit}>
          Sign
        </Button>
      </DialogActions>
    </Dialog>
  )
}
