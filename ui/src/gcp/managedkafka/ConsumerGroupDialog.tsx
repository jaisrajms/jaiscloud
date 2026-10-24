import { useEffect, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  Stack,
  TextField,
  Typography,
} from '@mui/material'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import {
  updateConsumerGroup,
  type ManagedKafkaConsumerGroup,
  type ManagedKafkaConsumerGroupOffset,
} from '../../api/gcp/managedkafka'

export interface ConsumerGroupDialogProps {
  open: boolean
  onClose: () => void
  location: string
  cluster: string
  group?: ManagedKafkaConsumerGroup
  onSaved?: () => void
}

const DEFAULT_OFFSET: ManagedKafkaConsumerGroupOffset = { topic: '', partition: 0, offset: 0 }

/** Edit a consumer group's committed offsets. */
export function ConsumerGroupDialog({
  open,
  onClose,
  location,
  cluster,
  group,
  onSaved,
}: ConsumerGroupDialogProps) {
  const queryClient = useQueryClient()
  const [offsets, setOffsets] = useState<ManagedKafkaConsumerGroupOffset[]>([])

  useEffect(() => {
    if (!open) return
    setOffsets(
      group && group.offsets.length > 0
        ? group.offsets.map((o) => ({ ...o }))
        : [{ ...DEFAULT_OFFSET }],
    )
  }, [open, group])

  const save = useMutation({
    mutationFn: (next: ManagedKafkaConsumerGroupOffset[]) =>
      updateConsumerGroup(location, cluster, group!.id, { offsets: next }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'managedkafka'] })
      onSaved?.()
      onClose()
    },
  })

  const setOffset = (index: number, patch: Partial<ManagedKafkaConsumerGroupOffset>) =>
    setOffsets((cur) => cur.map((o, i) => (i === index ? { ...o, ...patch } : o)))

  const valid = offsets.length > 0 && offsets.every((o) => o.topic.trim())
  const canSave = Boolean(group && valid) && !save.isPending

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Edit {group?.id}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">
              Could not update the consumer group: {(save.error as Error).message}
            </Alert>
          )}
          <Typography variant="body2" color="text.secondary">
            Set the committed offsets to write to the cluster&apos;s broker. The topic may be a bare
            id or the full resource name.
          </Typography>
          {offsets.map((offset, index) => (
            <Box
              key={index}
              sx={{ display: 'grid', gridTemplateColumns: '2fr 1fr 1fr 1fr auto', gap: 1, alignItems: 'center' }}
            >
              <TextField
                size="small"
                label="Topic"
                value={offset.topic}
                onChange={(e) => setOffset(index, { topic: e.target.value })}
                placeholder="orders"
              />
              <TextField
                size="small"
                label="Partition"
                type="number"
                value={offset.partition}
                onChange={(e) => setOffset(index, { partition: Number(e.target.value) })}
              />
              <TextField
                size="small"
                label="Offset"
                type="number"
                value={offset.offset}
                onChange={(e) => setOffset(index, { offset: Number(e.target.value) })}
              />
              <TextField
                size="small"
                label="Metadata"
                value={offset.metadata ?? ''}
                onChange={(e) => setOffset(index, { metadata: e.target.value })}
              />
              <IconButton
                size="small"
                onClick={() => setOffsets((cur) => cur.filter((_, i) => i !== index))}
                disabled={offsets.length === 1}
                aria-label="Remove offset"
              >
                <DeleteOutlineIcon fontSize="small" />
              </IconButton>
            </Box>
          ))}
          <Box>
            <Button size="small" onClick={() => setOffsets((cur) => [...cur, { ...DEFAULT_OFFSET }])}>
              Add offset
            </Button>
          </Box>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button variant="contained" disabled={!canSave} onClick={() => save.mutate(offsets)}>
          Save
        </Button>
      </DialogActions>
    </Dialog>
  )
}
