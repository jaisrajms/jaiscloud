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
import { addSecretVersion } from '../../api/gcp/secretmanager'
import { toBase64 } from './util'

export interface AddVersionDialogProps {
  open: boolean
  secret: string
  onClose: () => void
}

/** Add a new version to a secret; the payload is base64-encoded on the wire. */
export function AddVersionDialog({ open, secret, onClose }: AddVersionDialogProps) {
  const queryClient = useQueryClient()
  const [payload, setPayload] = useState('')

  useEffect(() => {
    if (open) setPayload('')
  }, [open])

  const add = useMutation({
    mutationFn: () => addSecretVersion(secret, toBase64(payload)),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'secretmanager'] })
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Add version</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {add.isError && (
            <Alert severity="error">Could not add the version: {(add.error as Error).message}</Alert>
          )}
          <TextField
            autoFocus
            label="Secret payload"
            value={payload}
            onChange={(e) => setPayload(e.target.value)}
            multiline
            minRows={4}
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={!payload || add.isPending}
          onClick={() => add.mutate()}
        >
          Add version
        </Button>
      </DialogActions>
    </Dialog>
  )
}
