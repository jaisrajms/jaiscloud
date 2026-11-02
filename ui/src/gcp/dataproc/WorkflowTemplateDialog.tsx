import { useEffect, useState } from 'react'
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
import {
  createWorkflowTemplate,
  instantiateWorkflowTemplate,
  updateWorkflowTemplate,
  type DataprocWorkflowTemplate,
} from '../../api/gcp/dataproc'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { parseJsonObject, parseKeyValueLines } from './util'

export interface WorkflowTemplateDialogProps {
  open: boolean
  onClose: () => void
  /** Existing template to edit; omit to create. */
  template?: DataprocWorkflowTemplate | null
  /** Region for a create opened from a region-scoped page. */
  defaultRegion?: string
  onSaved?: () => void
}

const DEFAULT_JOBS = `[
  {
    "stepId": "job-1",
    "sparkJob": {
      "mainClass": "com.example.Main",
      "jarFileUris": ["gs://my-bucket/app.jar"]
    }
  }
]`

/**
 * Create or edit a Dataproc workflow template. The structured fields cover the
 * common case (placement cluster, jobs, parameters); the raw definition editor
 * is the escape hatch and, when filled, is sent verbatim.
 */
export function WorkflowTemplateDialog({
  open,
  onClose,
  template,
  defaultRegion = '',
  onSaved,
}: WorkflowTemplateDialogProps) {
  const queryClient = useQueryClient()
  const [id, setID] = useState('')
  const [region, setRegion] = useState(defaultRegion || 'us-central1')
  const [cluster, setCluster] = useState('')
  const [jobsText, setJobsText] = useState(DEFAULT_JOBS)
  const [parametersText, setParametersText] = useState('')
  const [raw, setRaw] = useState('')
  const [error, setError] = useState('')

  useEffect(() => {
    if (!open) return
    const definition = (template?.definition ?? {}) as Record<string, unknown>
    const placement = definition.placement as Record<string, unknown> | undefined
    const managed = placement?.managedCluster as Record<string, unknown> | undefined
    setID(template?.id ?? '')
    setRegion(template?.region ?? defaultRegion ?? 'us-central1')
    setCluster(typeof managed?.clusterName === 'string' ? managed.clusterName : '')
    setJobsText(definition.jobs ? JSON.stringify(definition.jobs, null, 2) : DEFAULT_JOBS)
    const params = definition.parameters as Record<string, string> | undefined
    setParametersText(
      params
        ? Object.entries(params)
            .map(([k, v]) => `${k}=${v}`)
            .join('\n')
        : '',
    )
    setRaw('')
    setError('')
  }, [open, template, defaultRegion])

  const save = useMutation({
    mutationFn: (definition: Record<string, unknown>) => {
      const input = { region: region.trim(), id: id.trim(), definition }
      return template
        ? updateWorkflowTemplate(region.trim(), template.id, { ...input, version: template.version })
        : createWorkflowTemplate(input)
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'dataproc'] })
      onSaved?.()
      onClose()
    },
  })

  const handleSave = () => {
    setError('')
    try {
      const base = (template?.definition ?? {}) as Record<string, unknown>
      let definition: Record<string, unknown>
      if (raw.trim()) {
        const parsed = parseJsonObject(raw)
        if (parsed.error) throw new Error(parsed.error)
        definition = parsed.value!
      } else {
        const jobs = JSON.parse(jobsText) as unknown
        if (!Array.isArray(jobs)) throw new Error('Jobs must be a JSON array.')
        definition = { ...base, jobs, parameters: parseKeyValueLines(parametersText) }
        if (cluster.trim()) {
          definition.placement = {
            ...((base.placement as Record<string, unknown> | undefined) ?? {}),
            managedCluster: { clusterName: cluster.trim() },
          }
        }
      }
      save.mutate(definition)
    } catch (err) {
      setError((err as Error).message)
    }
  }

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>{template ? `Edit ${template.id}` : 'Create workflow template'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">
              Could not save the template: {(save.error as Error).message}
            </Alert>
          )}
          {error && <Alert severity="error">{error}</Alert>}
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              autoFocus={!template}
              label="Template ID"
              value={id}
              onChange={(e) => setID(e.target.value)}
              disabled={Boolean(template)}
              fullWidth
            />
            <TextField
              label="Region"
              value={region}
              onChange={(e) => setRegion(e.target.value)}
              disabled={Boolean(template)}
              fullWidth
            />
          </Stack>
          <TextField
            label="Managed cluster"
            value={cluster}
            onChange={(e) => setCluster(e.target.value)}
            placeholder="my-cluster"
            helperText="The template runs its jobs on this Dataproc cluster."
            fullWidth
          />
          <GcpCodeEditor
            label="Jobs (JSON array of WorkflowTemplate jobs)"
            value={jobsText}
            onChange={setJobsText}
            language="json"
            minRows={8}
          />
          <TextField
            label="Parameters (key=value per line)"
            value={parametersText}
            onChange={(e) => setParametersText(e.target.value)}
            multiline
            minRows={2}
            fullWidth
          />
          <GcpCodeEditor
            label="Advanced: raw definition override (JSON)"
            value={raw}
            onChange={setRaw}
            language="json"
            minRows={6}
            helperText="Leave empty to use the fields above. A non-empty object is sent verbatim."
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={save.isPending || !id.trim() || !region.trim()}
          onClick={handleSave}
        >
          {template ? 'Save' : 'Create'}
        </Button>
      </DialogActions>
    </Dialog>
  )
}

export interface InstantiateTemplateDialogProps {
  open: boolean
  onClose: () => void
  region: string
  templateID: string
  version: number
}

/** Run a stored workflow template, optionally substituting parameters. */
export function InstantiateTemplateDialog({
  open,
  onClose,
  region,
  templateID,
  version,
}: InstantiateTemplateDialogProps) {
  const [parametersText, setParametersText] = useState('')
  const [result, setResult] = useState('')
  const [error, setError] = useState('')

  useEffect(() => {
    if (!open) return
    setParametersText('')
    setResult('')
    setError('')
  }, [open])

  const run = useMutation({
    mutationFn: (parameters: Record<string, string>) =>
      instantiateWorkflowTemplate(region, templateID, { version, parameters }),
    onSuccess: (operation) => {
      const name = (operation as { name?: string } | undefined)?.name
      setResult(name ? `Started ${name}` : 'Workflow started.')
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>Run {templateID}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {run.isError && (
            <Alert severity="error">Could not start the workflow: {(run.error as Error).message}</Alert>
          )}
          {error && <Alert severity="error">{error}</Alert>}
          {result && <Alert severity="success">{result}</Alert>}
          <TextField
            label="Parameters (key=value per line)"
            value={parametersText}
            onChange={(e) => setParametersText(e.target.value)}
            multiline
            minRows={3}
            fullWidth
          />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>{result ? 'Done' : 'Cancel'}</Button>
        <Button
          variant="contained"
          disabled={run.isPending || Boolean(result)}
          onClick={() => {
            setError('')
            try {
              run.mutate(parseKeyValueLines(parametersText))
            } catch (err) {
              setError((err as Error).message)
            }
          }}
        >
          Run
        </Button>
      </DialogActions>
    </Dialog>
  )
}
