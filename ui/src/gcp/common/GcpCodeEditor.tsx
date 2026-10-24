import { lazy, Suspense } from 'react'
import { Box, CircularProgress } from '@mui/material'
import type { GcpCodeEditorImplProps } from './GcpCodeEditorImpl'

const Impl = lazy(() =>
  import('./GcpCodeEditorImpl').then((module) => ({ default: module.GcpCodeEditorImpl })),
)

/**
 * Shared GCP console code editor: an MUI-styled, ace-backed JSON/YAML/text
 * editor with syntax highlighting, folding and bracket matching. ace is
 * fetched lazily on first render so it stays out of the initial GCP bundle.
 * Cloudscape-free by design.
 */
export function GcpCodeEditor(props: GcpCodeEditorImplProps) {
  return (
    <Suspense
      fallback={
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 3 }}>
          <CircularProgress size={24} />
        </Box>
      }
    >
      <Impl {...props} />
    </Suspense>
  )
}
