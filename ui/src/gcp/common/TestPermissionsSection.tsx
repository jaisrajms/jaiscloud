import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { Alert, Button, Stack, TextField, Typography } from '@mui/material'

export interface TestPermissionsSectionProps {
  /** Permissions prefilled in the textarea, one per line. */
  defaultPermissions?: string[]
  /** Calls the resource's testIamPermissions endpoint. */
  run: (permissions: string[]) => Promise<{ permissions: string[] }>
}

/**
 * Inline testIamPermissions probe: the resource's granted subset of the
 * requested permissions is shown back. Used by the Eventarc trigger/channel
 * pages (and any other detail page that only needs the read-only probe).
 */
export function TestPermissionsSection({ defaultPermissions = [], run }: TestPermissionsSectionProps) {
  const [text, setText] = useState(defaultPermissions.join('\n'))
  const [granted, setGranted] = useState<string[] | null>(null)

  const test = useMutation({
    mutationFn: () =>
      run(
        text
          .split('\n')
          .map((p) => p.trim())
          .filter(Boolean),
      ),
    onSuccess: (resp) => setGranted(resp.permissions),
  })

  return (
    <Stack spacing={1} sx={{ maxWidth: 640 }}>
      <Typography variant="subtitle1">Test IAM permissions</Typography>
      {test.isError && <Alert severity="error">{(test.error as Error).message}</Alert>}
      <TextField
        label="Permissions (one per line)"
        value={text}
        onChange={(e) => setText(e.target.value)}
        multiline
        minRows={2}
        fullWidth
      />
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center' }}>
        <Button
          variant="outlined"
          disabled={test.isPending}
          onClick={() => {
            setGranted(null)
            test.mutate()
          }}
        >
          Test
        </Button>
        {granted && (
          <Typography variant="body2">
            Granted: {granted.length > 0 ? granted.join(', ') : 'none'}
          </Typography>
        )}
      </Stack>
    </Stack>
  )
}
