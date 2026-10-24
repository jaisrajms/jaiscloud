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
import { createServiceAccount } from '../../api/gcp/iam'

export interface CreateServiceAccountDialogProps {
  open: boolean
  onClose: () => void
}

/** Create an IAM service account. */
export function CreateServiceAccountDialog({ open, onClose }: CreateServiceAccountDialogProps) {
  const queryClient = useQueryClient()
  const [accountId, setAccountId] = useState('')
  const [displayName, setDisplayName] = useState('')
  const [description, setDescription] = useState('')

  useEffect(() => {
    if (open) {
      setAccountId('')
      setDisplayName('')
      setDescription('')
    }
  }, [open])

  const create = useMutation({
    mutationFn: () =>
      createServiceAccount({
        accountId,
        displayName: displayName || undefined,
        description: description || undefined,
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'iam'] })
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Create service account</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {create.isError && (
            <Alert severity="error">
              Could not create the service account: {(create.error as Error).message}
            </Alert>
          )}
          <TextField
            autoFocus
            label="Service account ID"
            value={accountId}
            onChange={(e) => setAccountId(e.target.value)}
            placeholder="my-service-account"
            fullWidth
          />
          <TextField
            label="Display name (optional)"
            value={displayName}
            onChange={(e) => setDisplayName(e.target.value)}
            fullWidth
          />
          <TextField
            label="Description (optional)"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            multiline
            minRows={2}
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={!accountId || create.isPending}
          onClick={() => create.mutate()}
        >
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
