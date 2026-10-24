import type { ReactNode } from 'react'
import { Box, Stack, Typography } from '@mui/material'

/** A small label/value row used across the Managed Kafka detail pages. */
export function Detail({ label, children }: { label: string; children: ReactNode }) {
  return (
    <Stack spacing={0.5}>
      <Typography variant="caption" color="text.secondary">
        {label}
      </Typography>
      <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
        {children || '—'}
      </Typography>
    </Stack>
  )
}

/** JsonBlock pretty-prints a stored wire object (cluster config, topic configs). */
export function JsonBlock({ value }: { value: unknown }) {
  if (value === undefined || value === null) {
    return <Typography variant="body2">—</Typography>
  }
  return (
    <Box
      component="pre"
      sx={{
        m: 0,
        p: 2,
        borderRadius: 2,
        border: '1px solid',
        borderColor: 'divider',
        bgcolor: 'action.hover',
        overflow: 'auto',
        fontSize: 12,
      }}
    >
      {JSON.stringify(value, null, 2)}
    </Box>
  )
}
