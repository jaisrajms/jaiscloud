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
} from '@mui/material'
import { createProject } from '../../api/gcp/resourcemanager'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { invalidateProjects } from './queries'
import { parseKV, validProjectId } from './util'

export interface CreateProjectDialogProps {
  onClose: () => void
}

/**
 * Create a Cloud Resource Manager project. Mounted only while open, so a failed
 * attempt never carries stale error state into a reopened dialog.
 */
export function CreateProjectDialog({ onClose }: CreateProjectDialogProps) {
  const queryClient = useQueryClient()
  const [projectId, setProjectId] = useState('')
  const [displayName, setDisplayName] = useState('')
  const [labelsText, setLabelsText] = useState('')

  const create = useMutation({
    mutationFn: () =>
      createProject({ projectId, displayName: displayName || undefined, labels: parseKV(labelsText) }),
    onSuccess: () => {
      invalidateProjects(queryClient)
      onClose()
    },
  })

  const valid = validProjectId(projectId)

  return (
    <Dialog open onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Create project</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {create.isError && (
            <Alert severity="error">
              Could not create the project: {(create.error as Error).message}
            </Alert>
          )}
          <TextField
            autoFocus
            label="Project ID"
            value={projectId}
            onChange={(e) => setProjectId(e.target.value.trim())}
            placeholder="my-project"
            helperText="6–30 lowercase letters, digits, or hyphens; must start with a letter and not end with a hyphen."
            error={projectId !== '' && !valid}
            fullWidth
          />
          <TextField
            label="Project name"
            value={displayName}
            onChange={(e) => setDisplayName(e.target.value)}
            placeholder="My Project"
            fullWidth
          />
          <GcpCodeEditor
            label="Labels (key=value per line)"
            value={labelsText}
            onChange={setLabelsText}
            language="text"
            minRows={3}
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={!valid || create.isPending} onClick={() => create.mutate()}>
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
