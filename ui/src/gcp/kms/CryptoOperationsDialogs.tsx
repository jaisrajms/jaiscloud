import { useEffect, useState, type ReactNode } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Alert,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  MenuItem,
  Stack,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material'
import ContentCopyIcon from '@mui/icons-material/ContentCopy'
import DownloadIcon from '@mui/icons-material/Download'
import {
  asymmetricDecrypt,
  asymmetricSign,
  decryptCryptoKey,
  encryptCryptoKey,
  getCryptoKeyVersionPublicKey,
  macSign,
  macVerify,
} from '../../api/gcp/kms'
import {
  base64Decode,
  base64Encode,
  digestAlgorithmFor,
  digestFieldFor,
  downloadText,
  shaDigestBase64,
  type DigestAlgorithm,
} from './util'

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** A read-only, copyable result field for a base64/token value. */
function ResultField({
  label,
  value,
  onDownload,
}: {
  label: string
  value: string
  onDownload?: () => void
}) {
  return (
    <Stack spacing={0.5}>
      <Typography variant="subtitle2">{label}</Typography>
      <TextField
        value={value}
        fullWidth
        multiline
        maxRows={10}
        slotProps={{
          input: {
            readOnly: true,
            sx: { fontFamily: 'monospace', fontSize: 12 },
            endAdornment: (
              <Stack direction="row" sx={{ alignItems: 'flex-start' }}>
                <Tooltip title="Copy">
                  <IconButton
                    size="small"
                    aria-label={`Copy ${label}`}
                    onClick={() => void navigator.clipboard.writeText(value)}
                  >
                    <ContentCopyIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
                {onDownload && (
                  <Tooltip title="Download">
                    <IconButton
                      size="small"
                      aria-label={`Download ${label}`}
                      onClick={onDownload}
                    >
                      <DownloadIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                )}
              </Stack>
            ),
          },
        }}
      />
    </Stack>
  )
}

interface CryptoDialogProps {
  open: boolean
  title: string
  description?: string
  submitLabel: string
  pending: boolean
  error: unknown
  submitDisabled?: boolean
  onClose: () => void
  onSubmit: () => void
  result?: ReactNode
  children: ReactNode
}

/** Shared shell for the KMS crypto-operation dialogs. */
function CryptoDialog({
  open,
  title,
  description,
  submitLabel,
  pending,
  error,
  submitDisabled,
  onClose,
  onSubmit,
  result,
  children,
}: CryptoDialogProps) {
  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>{title}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {description && (
            <Typography variant="body2" color="text.secondary">
              {description}
            </Typography>
          )}
          {Boolean(error) && <Alert severity="error">{errorMessage(error)}</Alert>}
          {children}
          {result}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
        <Button variant="contained" disabled={pending || submitDisabled} onClick={onSubmit}>
          {submitLabel}
        </Button>
      </DialogActions>
    </Dialog>
  )
}

export interface KeyOpDialogProps {
  open: boolean
  location: string
  keyRing: string
  cryptoKey: string
  onClose: () => void
}

/** Encrypt symmetric plaintext with the key's primary version. */
export function EncryptDialog({ open, location, keyRing, cryptoKey, onClose }: KeyOpDialogProps) {
  const [plaintext, setPlaintext] = useState('')
  const [aad, setAad] = useState('')
  const [result, setResult] = useState<string | null>(null)

  const encrypt = useMutation({
    mutationFn: () =>
      encryptCryptoKey(location, keyRing, cryptoKey, {
        plaintext: base64Encode(plaintext),
        ...(aad ? { additionalAuthenticatedData: base64Encode(aad) } : {}),
      }),
    onSuccess: (response) => setResult(response.ciphertext),
  })

  useEffect(() => {
    if (open) {
      setPlaintext('')
      setAad('')
      setResult(null)
      encrypt.reset()
    }
    // Reset only when the dialog opens; the mutation object is stable.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  return (
    <CryptoDialog
      open={open}
      title="Encrypt"
      description="The plaintext is UTF-8 encoded and encrypted with the key's primary version; the base64 ciphertext is returned."
      submitLabel="Encrypt"
      pending={encrypt.isPending}
      error={encrypt.isError ? encrypt.error : null}
      submitDisabled={plaintext.length === 0}
      onClose={onClose}
      onSubmit={() => {
        setResult(null)
        encrypt.mutate()
      }}
      result={result !== null ? <ResultField label="Ciphertext (base64)" value={result} /> : undefined}
    >
      <TextField
        autoFocus
        label="Plaintext"
        value={plaintext}
        onChange={(e) => setPlaintext(e.target.value)}
        multiline
        minRows={3}
        fullWidth
      />
      <TextField
        label="Additional authenticated data (optional)"
        value={aad}
        onChange={(e) => setAad(e.target.value)}
        fullWidth
      />
    </CryptoDialog>
  )
}

/** Decrypt base64 ciphertext with the key's primary version. */
export function DecryptDialog({ open, location, keyRing, cryptoKey, onClose }: KeyOpDialogProps) {
  const [ciphertext, setCiphertext] = useState('')
  const [aad, setAad] = useState('')
  const [result, setResult] = useState<string | null>(null)

  const decrypt = useMutation({
    mutationFn: () =>
      decryptCryptoKey(location, keyRing, cryptoKey, {
        ciphertext,
        ...(aad ? { additionalAuthenticatedData: base64Encode(aad) } : {}),
      }),
    onSuccess: (response) => setResult(base64Decode(response.plaintext)),
  })

  useEffect(() => {
    if (open) {
      setCiphertext('')
      setAad('')
      setResult(null)
      decrypt.reset()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  return (
    <CryptoDialog
      open={open}
      title="Decrypt"
      description="The base64 ciphertext is decrypted; the recovered plaintext is shown as UTF-8 text."
      submitLabel="Decrypt"
      pending={decrypt.isPending}
      error={decrypt.isError ? decrypt.error : null}
      submitDisabled={ciphertext.length === 0}
      onClose={onClose}
      onSubmit={() => {
        setResult(null)
        decrypt.mutate()
      }}
      result={result !== null ? <ResultField label="Plaintext" value={result} /> : undefined}
    >
      <TextField
        autoFocus
        label="Ciphertext (base64)"
        value={ciphertext}
        onChange={(e) => setCiphertext(e.target.value)}
        fullWidth
      />
      <TextField
        label="Additional authenticated data (optional)"
        value={aad}
        onChange={(e) => setAad(e.target.value)}
        fullWidth
      />
    </CryptoDialog>
  )
}

export interface VersionOpDialogProps {
  open: boolean
  location: string
  keyRing: string
  cryptoKey: string
  version: string
  /** The version's algorithm, used to pick the digest and gate operations. */
  algorithm?: string
  /** The selected project, part of the public-key query key. */
  accountId?: string
  onClose: () => void
}

/** Sign a message with an asymmetric-signing version. */
export function AsymmetricSignDialog({
  open,
  location,
  keyRing,
  cryptoKey,
  version,
  algorithm,
  onClose,
}: VersionOpDialogProps) {
  const fixed = digestAlgorithmFor(algorithm)
  const [message, setMessage] = useState('')
  const [digestAlgorithm, setDigestAlgorithm] = useState<DigestAlgorithm>(fixed ?? 'SHA-256')
  const [result, setResult] = useState<string | null>(null)

  const sign = useMutation({
    mutationFn: async () => {
      const digest = await shaDigestBase64(message, digestAlgorithm)
      return asymmetricSign(location, keyRing, cryptoKey, version, {
        digest: { [digestFieldFor(digestAlgorithm)]: digest },
      })
    },
    onSuccess: (response) => setResult(response.signature),
  })

  useEffect(() => {
    if (open) {
      setMessage('')
      setDigestAlgorithm(fixed ?? 'SHA-256')
      setResult(null)
      sign.reset()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, fixed])

  return (
    <CryptoDialog
      open={open}
      title={`Sign · v${version}`}
      description="The message is hashed with the key's digest algorithm; the base64 signature is returned."
      submitLabel="Sign"
      pending={sign.isPending}
      error={sign.isError ? sign.error : null}
      submitDisabled={message.length === 0}
      onClose={onClose}
      onSubmit={() => {
        setResult(null)
        sign.mutate()
      }}
      result={result !== null ? <ResultField label="Signature (base64)" value={result} /> : undefined}
    >
      <TextField
        autoFocus
        label="Message"
        value={message}
        onChange={(e) => setMessage(e.target.value)}
        multiline
        minRows={3}
        fullWidth
      />
      <TextField
        select
        label="Digest algorithm"
        value={digestAlgorithm}
        onChange={(e) => setDigestAlgorithm(e.target.value as DigestAlgorithm)}
        disabled={Boolean(fixed)}
        fullWidth
      >
        {(fixed ? [fixed] : (['SHA-256', 'SHA-384', 'SHA-512'] as DigestAlgorithm[])).map((alg) => (
          <MenuItem key={alg} value={alg}>
            {alg}
          </MenuItem>
        ))}
      </TextField>
    </CryptoDialog>
  )
}

/** Decrypt ciphertext with an RSA_DECRYPT version. */
export function AsymmetricDecryptDialog({
  open,
  location,
  keyRing,
  cryptoKey,
  version,
  onClose,
}: VersionOpDialogProps) {
  const [ciphertext, setCiphertext] = useState('')
  const [result, setResult] = useState<string | null>(null)

  const decrypt = useMutation({
    mutationFn: () => asymmetricDecrypt(location, keyRing, cryptoKey, version, { ciphertext }),
    onSuccess: (response) => setResult(base64Decode(response.plaintext)),
  })

  useEffect(() => {
    if (open) {
      setCiphertext('')
      setResult(null)
      decrypt.reset()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  return (
    <CryptoDialog
      open={open}
      title={`Asymmetric decrypt · v${version}`}
      description="The base64 ciphertext is decrypted with the version's RSA private key."
      submitLabel="Decrypt"
      pending={decrypt.isPending}
      error={decrypt.isError ? decrypt.error : null}
      submitDisabled={ciphertext.length === 0}
      onClose={onClose}
      onSubmit={() => {
        setResult(null)
        decrypt.mutate()
      }}
      result={result !== null ? <ResultField label="Plaintext" value={result} /> : undefined}
    >
      <TextField
        autoFocus
        label="Ciphertext (base64)"
        value={ciphertext}
        onChange={(e) => setCiphertext(e.target.value)}
        fullWidth
      />
    </CryptoDialog>
  )
}

/** Compute an HMAC tag over a message with a MAC version. */
export function MacSignDialog({ open, location, keyRing, cryptoKey, version, onClose }: VersionOpDialogProps) {
  const [data, setData] = useState('')
  const [result, setResult] = useState<string | null>(null)

  const sign = useMutation({
    mutationFn: () => macSign(location, keyRing, cryptoKey, version, { data: base64Encode(data) }),
    onSuccess: (response) => setResult(response.mac),
  })

  useEffect(() => {
    if (open) {
      setData('')
      setResult(null)
      sign.reset()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  return (
    <CryptoDialog
      open={open}
      title={`MAC sign · v${version}`}
      description="The message is HMAC-ed with the version's key; the base64 tag is returned."
      submitLabel="Sign"
      pending={sign.isPending}
      error={sign.isError ? sign.error : null}
      submitDisabled={data.length === 0}
      onClose={onClose}
      onSubmit={() => {
        setResult(null)
        sign.mutate()
      }}
      result={result !== null ? <ResultField label="MAC (base64)" value={result} /> : undefined}
    >
      <TextField
        autoFocus
        label="Message"
        value={data}
        onChange={(e) => setData(e.target.value)}
        multiline
        minRows={3}
        fullWidth
      />
    </CryptoDialog>
  )
}

/** Verify an HMAC tag against a message with a MAC version. */
export function MacVerifyDialog({ open, location, keyRing, cryptoKey, version, onClose }: VersionOpDialogProps) {
  const [data, setData] = useState('')
  const [mac, setMac] = useState('')
  const [result, setResult] = useState<boolean | null>(null)

  const verify = useMutation({
    mutationFn: () => macVerify(location, keyRing, cryptoKey, version, { data: base64Encode(data), mac }),
    onSuccess: (response) => setResult(response.success),
  })

  useEffect(() => {
    if (open) {
      setData('')
      setMac('')
      setResult(null)
      verify.reset()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  return (
    <CryptoDialog
      open={open}
      title={`MAC verify · v${version}`}
      description="Both the message and the base64 tag are checked against the version's key."
      submitLabel="Verify"
      pending={verify.isPending}
      error={verify.isError ? verify.error : null}
      submitDisabled={data.length === 0 || mac.length === 0}
      onClose={onClose}
      onSubmit={() => {
        setResult(null)
        verify.mutate()
      }}
      result={
        result === null ? undefined : (
          <Alert severity={result ? 'success' : 'error'}>
            {result ? 'The MAC is valid.' : 'The MAC is not valid for this message.'}
          </Alert>
        )
      }
    >
      <TextField
        autoFocus
        label="Message"
        value={data}
        onChange={(e) => setData(e.target.value)}
        multiline
        minRows={3}
        fullWidth
      />
      <TextField
        label="MAC (base64)"
        value={mac}
        onChange={(e) => setMac(e.target.value)}
        fullWidth
      />
    </CryptoDialog>
  )
}

/** View and download a version's public key (PEM). */
export function PublicKeyDialog({ open, location, keyRing, cryptoKey, version, accountId, onClose }: VersionOpDialogProps) {
  const publicKey = useQuery({
    queryKey: ['gcp', 'kms', 'publicKey', location, keyRing, cryptoKey, version, accountId],
    queryFn: () => getCryptoKeyVersionPublicKey(location, keyRing, cryptoKey, version),
    enabled: open,
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Public key · v{version}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {publicKey.isLoading && (
            <Stack sx={{ alignItems: 'center', py: 3 }}>
              <CircularProgress size={24} />
            </Stack>
          )}
          {publicKey.isError && (
            <Alert severity="error">
              Could not load the public key: {errorMessage(publicKey.error)}
            </Alert>
          )}
          {publicKey.data && (
            <ResultField
              label={`Public key (${publicKey.data.algorithm || 'PEM'})`}
              value={publicKey.data.pem}
              onDownload={() => downloadText(`${cryptoKey}-v${version}-public.pem`, publicKey.data.pem)}
            />
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
      </DialogActions>
    </Dialog>
  )
}
