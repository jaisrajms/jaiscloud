import { useState } from 'react'
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
import { deleteProject, type Project } from '../../api/gcp/resourcemanager'
import { useAccount } from '../../context/AccountContext'
import { invalidateProjects } from './queries'

export interface DeleteProjectDialogProps {
  project: Project
  onClose: () => void
}

/**
 * Confirm a project delete. Real GCP requires acknowledging the recovery window
 * and typing the project ID, so marking a project for deletion (which hides it
 * from the picker) is never a single mis-click.
 */
export function DeleteProjectDialog({ project, onClose }: DeleteProjectDialogProps) {
  const queryClient = useQueryClient()
  const { accountId } = useAccount()
  const [confirm, setConfirm] = useState('')

  const remove = useMutation({
    mutationFn: (id: string) => deleteProject(id),
    onSuccess: () => {
      invalidateProjects(queryClient)
      onClose()
    },
  })

  const id = project.projectId
  const isCurrent = id === accountId

  return (
    <Dialog open onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Delete project</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {remove.isError && (
            <Alert severity="error">
              Could not delete the project: {(remove.error as Error).message}
            </Alert>
          )}
          <Alert severity="warning">
            Project <strong>{id}</strong> will be marked for deletion (DELETE_REQUESTED) and
            hidden from the project picker. It can be restored with Undelete.
            {isCurrent && ' This is the project currently selected in the console.'}
          </Alert>
          <Typography variant="body2" color="text.secondary">
            Type the project ID to confirm.
          </Typography>
          <TextField
            autoFocus
            label="Project ID"
            value={confirm}
            onChange={(e) => setConfirm(e.target.value.trim())}
            placeholder={id}
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          color="error"
          variant="contained"
          disabled={confirm !== id || remove.isPending}
          onClick={() => remove.mutate(id)}
        >
          Delete
        </Button>
      </DialogActions>
    </Dialog>
  )
}
