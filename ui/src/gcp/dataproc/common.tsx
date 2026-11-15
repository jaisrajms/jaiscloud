import { useEffect, useState, type ReactNode } from 'react'
import {
  Alert,
  Box,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  IconButton,
  Link,
  Stack,
  Tooltip,
  Typography,
} from '@mui/material'
import ContentCopyIcon from '@mui/icons-material/ContentCopy'
import { Link as RouterLink } from 'react-router-dom'
import { useGcpSnackbar } from '../common/SnackbarProvider'
import {
  GS_URI_LABELS,
  buildStatusTimeline,
  formatDelta,
  gcsBucketPath,
  parseGsUri,
  shortDate,
  type ChipColor,
  type GsUri,
} from './util'

/** A small label/value row used across the Dataproc detail pages. */
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

/** JsonBlock pretty-prints a stored wire object (config, job body, definition). */
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

/**
 * A gs:// URI linked to the containing Cloud Storage bucket. The bucket page is
 * opened at the object's folder (`?prefix=`) so the referenced object and its
 * siblings are visible, rather than the empty bucket root.
 */
export function GsUriLink({ uri }: { uri?: string | null }) {
  const parsed = parseGsUri(uri)
  if (!parsed) {
    return <>{uri || '—'}</>
  }
  return (
    <Link
      component={RouterLink}
      to={gcsBucketPath(parsed.bucket, parsed.object)}
      target="_blank"
      rel="noreferrer"
      sx={{ overflowWrap: 'anywhere' }}
    >
      {parsed.uri}
    </Link>
  )
}

/**
 * A labelled list of the known gs:// fields on a job body, cluster config or
 * workflow definition. The labels and wrapping live here so every detail page
 * renders the same list.
 */
export function GsUriList({ resources }: { resources: GsUri[] }) {
  if (resources.length === 0) return null
  return (
    <Stack spacing={1} sx={{ mb: 3 }}>
      {resources.map((resource) => (
        <Stack
          key={resource.uri}
          direction="row"
          spacing={1}
          sx={{ alignItems: 'baseline', flexWrap: 'wrap', rowGap: 0.5 }}
        >
          <Typography variant="body2" color="text.secondary" sx={{ minWidth: 130 }}>
            {GS_URI_LABELS[resource.key] ?? resource.key}
          </Typography>
          <GsUriLink uri={resource.uri} />
        </Stack>
      ))}
    </Stack>
  )
}

/** A one-click copy control for a name, UUID or URI. */
export function CopyButton({ value, label }: { value?: string; label: string }) {
  const { notify } = useGcpSnackbar()
  if (!value) return null
  const text = value

  const copy = () => {
    if (!navigator.clipboard) {
      notify('Clipboard is unavailable in this browser', { severity: 'error' })
      return
    }
    void navigator.clipboard.writeText(text).then(
      () => notify(`${label} copied`),
      () => notify(`Could not copy ${label}`, { severity: 'error' }),
    )
  }

  return (
    <Tooltip title={`Copy ${label}`}>
      <IconButton size="small" aria-label={`Copy ${label}`} onClick={copy}>
        <ContentCopyIcon sx={{ fontSize: 16 }} />
      </IconButton>
    </Tooltip>
  )
}

/** A short confirmation for a destructive action; the caller owns the mutation. */
export function ConfirmDialog({
  open,
  title,
  body,
  confirmLabel = 'Delete',
  busy = false,
  error,
  onConfirm,
  onClose,
}: {
  open: boolean
  title: string
  body: ReactNode
  confirmLabel?: string
  busy?: boolean
  error?: string
  onConfirm: () => void
  onClose: () => void
}) {
  return (
    <Dialog open={open} onClose={busy ? undefined : onClose} maxWidth="xs" fullWidth>
      <DialogTitle>{title}</DialogTitle>
      <DialogContent>
        <DialogContentText component="div">{body}</DialogContentText>
        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={busy}>
          Cancel
        </Button>
        <Button color="error" variant="contained" onClick={onConfirm} disabled={busy}>
          {confirmLabel}
        </Button>
      </DialogActions>
    </Dialog>
  )
}

/** One status-timeline entry (a previous state or the current one). */
export interface TimelineEvent {
  state: string
  detail?: string
  substate?: string
  stateStartTime?: string
}

/** A one-second clock so the current row's "for Ns" keeps advancing. */
function useNow(intervalMs = 1000): number {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs)
    return () => clearInterval(id)
  }, [intervalMs])
  return now
}

/**
 * The Dataproc status timeline: every previous status, plus the current status
 * as its own clearly-marked row. The wire `statusHistory` is never mutated; the
 * current row is presentation-only (Discovery calls statusHistory "the previous
 * status"). Durations are relative, e.g. "0.4s" or "for 1m 5s".
 */
export function StatusTimeline({
  history,
  current,
  colorFor,
}: {
  history: TimelineEvent[]
  current: TimelineEvent
  colorFor: (state: string) => ChipColor
}) {
  const now = useNow()
  const rows = buildStatusTimeline(history, current, now)
  return (
    <Stack spacing={1}>
      {rows.map((row, i) => (
        <Stack
          key={`${row.state}-${i}`}
          direction="row"
          spacing={2}
          sx={{ alignItems: 'baseline', flexWrap: 'wrap', rowGap: 0.5 }}
        >
          <Chip
            size="small"
            label={row.state}
            color={colorFor(row.state)}
            variant={row.current ? 'filled' : 'outlined'}
          />
          {row.current && <Chip size="small" variant="outlined" label="current" />}
          {row.substate && <Chip size="small" variant="outlined" label={row.substate} />}
          {row.durationMs !== undefined &&
            (row.stateStartTime ? (
              <Tooltip title={shortDate(row.stateStartTime)}>
                <Typography variant="body2" color="text.secondary">
                  {row.current ? `for ${formatDelta(row.durationMs)}` : `lasted ${formatDelta(row.durationMs)}`}
                </Typography>
              </Tooltip>
            ) : (
              <Typography variant="body2" color="text.secondary">
                {row.current ? `for ${formatDelta(row.durationMs)}` : `lasted ${formatDelta(row.durationMs)}`}
              </Typography>
            ))}
          {row.detail && <Typography variant="body2">{row.detail}</Typography>}
        </Stack>
      ))}
    </Stack>
  )
}
