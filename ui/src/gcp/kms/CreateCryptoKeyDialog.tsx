import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  MenuItem,
  Stack,
  TextField,
} from '@mui/material'
import { createCryptoKey } from '../../api/gcp/kms'

export interface CreateCryptoKeyDialogProps {
  open: boolean
  location: string
  keyRing: string
  onClose: () => void
}

const PURPOSES = ['ENCRYPT_DECRYPT', 'ASYMMETRIC_SIGN', 'ASYMMETRIC_DECRYPT', 'MAC']

/** Create a KMS crypto key in a key ring. */
export function CreateCryptoKeyDialog({
  open,
  location,
  keyRing,
  onClose,
}: CreateCryptoKeyDialogProps) {
  const queryClient = useQueryClient()
  const [cryptoKeyId, setCryptoKeyId] = useState('')
  const [purpose, setPurpose] = useState('ENCRYPT_DECRYPT')
  const [algorithm, setAlgorithm] = useState('')
  const [rotationPeriod, setRotationPeriod] = useState('')

  useEffect(() => {
    if (open) {
      setCryptoKeyId('')
      setPurpose('ENCRYPT_DECRYPT')
      setAlgorithm('')
      setRotationPeriod('')
    }
  }, [open])

  const create = useMutation({
    mutationFn: () =>
      createCryptoKey(location, keyRing, {
        cryptoKeyId,
        purpose,
        algorithm: algorithm || undefined,
        rotationPeriod: rotationPeriod || undefined,
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'kms'] })
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Create crypto key</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {create.isError && (
            <Alert severity="error">
              Could not create the crypto key: {(create.error as Error).message}
            </Alert>
          )}
          <TextField
            autoFocus
            label="Crypto key ID"
            value={cryptoKeyId}
            onChange={(e) => setCryptoKeyId(e.target.value)}
            placeholder="my-key"
            fullWidth
          />
          <TextField
            select
            label="Purpose"
            value={purpose}
            onChange={(e) => setPurpose(e.target.value)}
            fullWidth
          >
            {PURPOSES.map((p) => (
              <MenuItem key={p} value={p}>
                {p}
              </MenuItem>
            ))}
          </TextField>
          <TextField
            label="Algorithm (optional)"
            value={algorithm}
            onChange={(e) => setAlgorithm(e.target.value)}
            placeholder="GOOGLE_SYMMETRIC_ENCRYPTION"
            fullWidth
          />
          <TextField
            label="Rotation period (optional)"
            value={rotationPeriod}
            onChange={(e) => setRotationPeriod(e.target.value)}
            placeholder="7776000s"
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={!cryptoKeyId || create.isPending}
          onClick={() => create.mutate()}
        >
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
