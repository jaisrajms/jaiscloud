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
import { createDataset } from '../../api/gcp/bigquery'

export interface CreateDatasetDialogProps {
  open: boolean
  onClose: () => void
  onCreated?: (datasetId: string) => void
}

const LOCATIONS = ['US', 'EU', 'us-central1', 'us-east1', 'europe-west1']

/** Create a BigQuery dataset. */
export function CreateDatasetDialog({ open, onClose, onCreated }: CreateDatasetDialogProps) {
  const queryClient = useQueryClient()
  const [datasetId, setDatasetId] = useState('')
  const [location, setLocation] = useState('US')
  const [friendlyName, setFriendlyName] = useState('')
  const [description, setDescription] = useState('')

  useEffect(() => {
    if (open) {
      setDatasetId('')
      setLocation('US')
      setFriendlyName('')
      setDescription('')
    }
  }, [open])

  const create = useMutation({
    mutationFn: () =>
      createDataset({
        datasetId,
        location: location || undefined,
        friendlyName: friendlyName || undefined,
        description: description || undefined,
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'bigquery'] })
      onCreated?.(datasetId)
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Create dataset</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {create.isError && (
            <Alert severity="error">
              Could not create the dataset: {(create.error as Error).message}
            </Alert>
          )}
          <TextField
            autoFocus
            label="Dataset ID"
            value={datasetId}
            onChange={(e) => setDatasetId(e.target.value)}
            placeholder="analytics"
            fullWidth
          />
          <TextField
            select
            label="Location"
            value={location}
            onChange={(e) => setLocation(e.target.value)}
            fullWidth
          >
            {LOCATIONS.map((l) => (
              <MenuItem key={l} value={l}>
                {l}
              </MenuItem>
            ))}
          </TextField>
          <TextField
            label="Friendly name (optional)"
            value={friendlyName}
            onChange={(e) => setFriendlyName(e.target.value)}
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
        <Button variant="contained" disabled={!datasetId || create.isPending} onClick={() => create.mutate()}>
          Create
        </Button>
      </DialogActions>
    </Dialog>
  )
}
