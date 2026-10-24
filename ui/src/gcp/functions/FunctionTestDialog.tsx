import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Stack,
  Typography,
} from '@mui/material'
import { callFunction } from '../../api/gcp/functions'
import { GcpCodeEditor } from '../common/GcpCodeEditor'

export interface FunctionTestDialogProps {
  open: boolean
  onClose: () => void
  location: string
  fn: string
}

/** Invoke a function synchronously (the console "Test" action). */
export function FunctionTestDialog({ open, onClose, location, fn }: FunctionTestDialogProps) {
  const [data, setData] = useState('{}')

  const invoke = useMutation({
    mutationFn: () => callFunction(location, fn, data),
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Test {fn}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          <GcpCodeEditor
            label="Event / payload (JSON)"
            value={data}
            onChange={setData}
            language="json"
            minRows={4}
          />
          {invoke.isError && <Alert severity="error">Invocation failed: {(invoke.error as Error).message}</Alert>}
          {invoke.isSuccess && (
            <Box>
              <Typography variant="caption" color="text.secondary">
                Execution {invoke.data.executionId}
              </Typography>
              {invoke.data.error ? (
                <Alert severity="error" sx={{ mt: 1 }}>
                  {invoke.data.error}
                </Alert>
              ) : (
                <Box
                  component="pre"
                  sx={{
                    m: 0,
                    mt: 1,
                    p: 2,
                    bgcolor: 'action.hover',
                    borderRadius: 1,
                    overflow: 'auto',
                    fontSize: 13,
                    fontFamily: 'monospace',
                    maxHeight: 240,
                  }}
                >
                  {invoke.data.result || '(empty result)'}
                </Box>
              )}
            </Box>
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Close</Button>
        <Button variant="contained" disabled={invoke.isPending} onClick={() => invoke.mutate()}>
          Invoke
        </Button>
      </DialogActions>
    </Dialog>
  )
}
