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
import { updateServiceAccount } from '../../api/gcp/iam'

export interface EditServiceAccountDialogProps {
  open: boolean
  email: string
  displayName: string
  description: string
  etag?: string
  onClose: () => void
}

/** Edit a service account's display name and description. */
export function EditServiceAccountDialog({
  open,
  email,
  displayName,
  description,
  etag,
  onClose,
}: EditServiceAccountDialogProps) {
  const queryClient = useQueryClient()
  const [name, setName] = useState(displayName)
  const [desc, setDesc] = useState(description)

  useEffect(() => {
    if (open) {
      setName(displayName)
      setDesc(description)
    }
  }, [open, displayName, description])

  const update = useMutation({
    mutationFn: () =>
      updateServiceAccount(email, { displayName: name, description: desc, etag }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'iam'] })
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Edit service account</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {update.isError && (
            <Alert severity="error">
              Could not update: {(update.error as Error).message}
            </Alert>
          )}
          <TextField
            autoFocus
            label="Display name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            fullWidth
          />
          <TextField
            label="Description"
            value={desc}
            onChange={(e) => setDesc(e.target.value)}
            multiline
            minRows={2}
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={update.isPending} onClick={() => update.mutate()}>
          Save
        </Button>
      </DialogActions>
    </Dialog>
  )
}
