import type { ReactNode } from 'react'
import { Box, Stack, Typography } from '@mui/material'
import { GcpServiceIcon } from '../icons/GcpServiceIcon'
import { serviceAccent } from '../icons/serviceIcons'

export interface GcpPageTitleProps {
  /** Service descriptor id driving the leading product glyph. */
  id: string
  /** Page title text (may be a long resource name). */
  children: ReactNode
}

/**
 * Page heading with the service product glyph. Superseded by `GcpPageHeader`
 * (pin + docs + actions); retained until the remaining pages are migrated
 * onto the shared scaffold.
 */
export function GcpPageTitle({ id, children }: GcpPageTitleProps) {
  return (
    <Stack
      direction="row"
      spacing={1.25}
      sx={{ alignItems: 'center', minWidth: 0, flexGrow: 1 }}
    >
      <Box sx={{ display: 'flex', color: serviceAccent(id) }}>
        <GcpServiceIcon id={id} size={28} />
      </Box>
      <Typography variant="h5" sx={{ overflowWrap: 'anywhere', minWidth: 0 }}>
        {children}
      </Typography>
    </Stack>
  )
}
