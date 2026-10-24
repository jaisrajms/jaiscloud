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
import { createKeyRing } from '../../api/gcp/kms'

export interface CreateKeyRingDialogProps {
  open: boolean
  location: string
  onClose: () => void
}

/** Create a KMS key ring in the selected location. */
export function CreateKeyRingDialog({ open, location, onClose }: CreateKeyRingDialogProps) {
  const queryClient = useQueryClient()
  const [keyRingId, setKeyRingId] = useState('')

  useEffect(() => {
    if (open) setKeyRingId('')
  }, [open])

  const create = useMutation({
    mutationFn: () => createKeyRing(location, keyRingId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'kms'] })
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Create key ring</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {create.isError && (
            <Alert severity="error">
              Could not create the key ring: {(create.error as Error).message}
            </Alert>
          )}
          <TextField
            autoFocus
            label="Key ring ID"
            value={keyRingId}
            onChange={(e) => setKeyRingId(e.target.value)}
            placeholder="my-keyring"
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={!keyRingId || create.isPending}
          onClick={() => create.mutate()}
        >
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
