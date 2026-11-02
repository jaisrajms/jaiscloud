import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
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
import { listClusters, submitJob } from '../../api/gcp/dataproc'
import { buildJobBody, JOB_TYPE_FIELDS, JOB_TYPE_OPTIONS, jobTypeLabel } from './util'

export interface JobDialogProps {
  open: boolean
  onClose: () => void
  /** Region preselected when the dialog is opened from a cluster page. */
  defaultRegion?: string
  /** Cluster preselected when the dialog is opened from a cluster page. */
  defaultCluster?: string
  onSubmitted?: () => void
}

/** Submit a Dataproc job: cluster picker, job type and type-specific fields. */
export function JobDialog({
  open,
  onClose,
  defaultRegion = '',
  defaultCluster = '',
  onSubmitted,
}: JobDialogProps) {
  const queryClient = useQueryClient()
  const clusters = useQuery({
    queryKey: ['gcp', 'dataproc', 'clusters'],
    queryFn: listClusters,
    enabled: open,
  })
  const [region, setRegion] = useState(defaultRegion || 'us-central1')
  const [cluster, setCluster] = useState(defaultCluster)
  const [jobID, setJobID] = useState('')
  const [type, setType] = useState('sparkJob')
  const [fields, setFields] = useState<Record<string, string>>({})
  const [args, setArgs] = useState('')
  const [properties, setProperties] = useState('')
  const [error, setError] = useState('')

  const available = useMemo(() => clusters.data?.clusters ?? [], [clusters.data])
  const regions = [...new Set(available.map((c) => c.region).filter(Boolean))].sort()
  const inRegion = available.filter((c) => c.region === region)

  useEffect(() => {
    if (!open) return
    setRegion(defaultRegion || 'us-central1')
    setCluster(defaultCluster)
    setJobID('')
    setType('sparkJob')
    setFields({})
    setArgs('')
    setProperties('')
    setError('')
  }, [open, defaultRegion, defaultCluster])

  // Default to the first region/cluster once the list arrives, so the selects
  // are never empty when the project has clusters.
  useEffect(() => {
    if (!open || defaultRegion || region !== 'us-central1') return
    const first = available[0]
    if (first) setRegion(first.region)
  }, [open, defaultRegion, region, available])

  useEffect(() => {
    if (!open || defaultCluster || !region) return
    const first = available.find((c) => c.region === region)
    if (first) setCluster(first.id)
  }, [open, defaultCluster, region, available])

  const submit = useMutation({
    mutationFn: (job: Record<string, unknown>) => submitJob({ region: region.trim(), job }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'dataproc'] })
      onSubmitted?.()
      onClose()
    },
  })

  const requiredMissing = (JOB_TYPE_FIELDS[type] ?? []).some(
    (field) => field.required && !(fields[field.key] ?? '').trim(),
  )
  const canSubmit = Boolean(region.trim() && cluster.trim()) && !requiredMissing

  const handleSubmit = () => {
    setError('')
    try {
      // Build (and validate) the body before the request, so a properties typo
      // is reported next to the field instead of as a server error.
      const job = buildJobBody({ clusterName: cluster, type, fields, args, properties })
      if (jobID.trim()) job.reference = { jobId: jobID.trim() }
      submit.mutate(job)
    } catch (err) {
      setError((err as Error).message)
    }
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>Submit job</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {submit.isError && (
            <Alert severity="error">Could not submit the job: {(submit.error as Error).message}</Alert>
          )}
          {error && <Alert severity="error">{error}</Alert>}
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              select
              label="Region"
              value={region}
              onChange={(e) => {
                setRegion(e.target.value)
                setCluster('')
              }}
              fullWidth
            >
              {(regions.length ? regions : [region]).map((r) => (
                <MenuItem key={r} value={r}>
                  {r}
                </MenuItem>
              ))}
            </TextField>
            <TextField
              select
              label="Cluster"
              value={cluster}
              onChange={(e) => setCluster(e.target.value)}
              fullWidth
              helperText={clusters.isError ? 'Failed to load clusters.' : undefined}
            >
              {inRegion.map((c) => (
                <MenuItem key={c.id} value={c.id}>
                  {c.id} ({c.status})
                </MenuItem>
              ))}
            </TextField>
          </Stack>
          <TextField
            label="Job ID (optional)"
            value={jobID}
            onChange={(e) => setJobID(e.target.value)}
            helperText="Leave empty to let Dataproc generate one."
            fullWidth
          />
          <TextField
            select
            label="Job type"
            value={type}
            onChange={(e) => {
              setType(e.target.value)
              setFields({})
            }}
            fullWidth
          >
            {JOB_TYPE_OPTIONS.map((option) => (
              <MenuItem key={option} value={option}>
                {jobTypeLabel(option)}
              </MenuItem>
            ))}
          </TextField>

          {(JOB_TYPE_FIELDS[type] ?? []).map((field) => (
            <TextField
              key={field.key}
              label={field.label}
              value={fields[field.key] ?? ''}
              onChange={(e) => setFields((prev) => ({ ...prev, [field.key]: e.target.value }))}
              placeholder={field.hint}
              required={field.required}
              multiline={field.multiline}
              minRows={field.multiline ? 4 : undefined}
              fullWidth
            />
          ))}

          <TextField
            label="Arguments (one per line)"
            value={args}
            onChange={(e) => setArgs(e.target.value)}
            multiline
            minRows={2}
            fullWidth
          />
          <TextField
            label="Spark properties (key=value per line)"
            value={properties}
            onChange={(e) => setProperties(e.target.value)}
            placeholder={'spark.executor.memory=2g'}
            multiline
            minRows={2}
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={!canSubmit || submit.isPending}
          onClick={handleSubmit}
        >
          Submit
        </Button>
      </DialogActions>
    </Dialog>
  )
}
