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
import {
  deployService,
  updateService,
  type RunServiceDetail,
} from '../../api/gcp/run'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import {
  applyFormToService,
  buildServiceBody,
  editableService,
  firstImage,
  parseJsonObject,
  serviceUpdateMask,
  type ServiceForm,
} from './util'

export interface DeployServiceDialogProps {
  open: boolean
  onClose: () => void
  /** Existing service to edit; omit to deploy a new one. */
  service?: RunServiceDetail | null
  region?: string
  serviceID?: string
  onSaved?: () => void
}

function formFromService(service: RunServiceDetail): ServiceForm {
  const template = (service.template as Record<string, unknown> | undefined) ?? {}
  const containers = Array.isArray(template.containers)
    ? (template.containers as Record<string, unknown>[])
    : []
  const container = containers[0] ?? {}
  const limits = ((container.resources as Record<string, unknown> | undefined)?.limits ??
    {}) as Record<string, string>
  const env = Array.isArray(container.env)
    ? (container.env as Array<Record<string, unknown>>)
        .map((e) => `${String(e.name ?? '')}=${String(e.value ?? '')}`)
        .join('\n')
    : ''
  const ports = Array.isArray(container.ports)
    ? (container.ports as Array<Record<string, unknown>>)
        .map((p) => String(p.containerPort ?? ''))
        .join(',')
    : ''
  return {
    image: firstImage(template.containers),
    command: Array.isArray(container.command) ? container.command.join('\n') : '',
    args: Array.isArray(container.args) ? container.args.join('\n') : '',
    env,
    ports,
    cpu: limits.cpu ?? '',
    memory: limits.memory ?? '',
    serviceAccount: typeof template.serviceAccount === 'string' ? template.serviceAccount : '',
    ingress: typeof service.ingress === 'string' ? service.ingress : '',
  }
}

const EMPTY_FORM: ServiceForm = {
  image: '',
  command: '',
  args: '',
  env: '',
  ports: '',
  cpu: '',
  memory: '',
  serviceAccount: '',
  ingress: 'INGRESS_TRAFFIC_ALL',
}

/**
 * Deploy or edit a Cloud Run service. Edit merges the form onto the stored
 * template (preserving traffic, volumes and probes) and sends only the changed
 * top-level field mask.
 */
export function DeployServiceDialog({
  open,
  onClose,
  service,
  region: initialRegion = '',
  serviceID: initialID = '',
  onSaved,
}: DeployServiceDialogProps) {
  const queryClient = useQueryClient()
  const [region, setRegion] = useState(initialRegion || 'us-central1')
  const [serviceID, setServiceID] = useState(initialID)
  const [form, setForm] = useState<ServiceForm>(EMPTY_FORM)
  const [raw, setRaw] = useState('')
  const [error, setError] = useState('')

  useEffect(() => {
    if (!open) return
    setRegion(initialRegion || 'us-central1')
    setServiceID(initialID)
    setForm(service ? formFromService(service) : EMPTY_FORM)
    setRaw('')
    setError('')
  }, [open, service, initialRegion, initialID])

  const save = useMutation({
    mutationFn: (payload: { body: Record<string, unknown>; mask?: string[] }) => {
      if (service) {
        return updateService(initialRegion, initialID, {
          body: payload.body,
          updateMask: payload.mask,
        })
      }
      return deployService({ region: region.trim(), id: serviceID.trim(), body: payload.body })
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'run'] })
      onSaved?.()
      onClose()
    },
  })

  const handleSave = () => {
    setError('')
    try {
      let body: Record<string, unknown>
      if (raw.trim()) {
        const parsed = parseJsonObject(raw)
        if (parsed.error) throw new Error(parsed.error)
        body = parsed.value!
      } else if (service) {
        body = applyFormToService(service, form)
      } else {
        if (!form.image.trim()) throw new Error('An image is required.')
        if (!serviceID.trim()) throw new Error('A service id is required.')
        body = buildServiceBody(form)
      }
      if (service) {
        const mask = serviceUpdateMask(editableService(service), body)
        if (mask.length === 0) throw new Error('No changes to save.')
        save.mutate({ body, mask })
        return
      }
      save.mutate({ body })
    } catch (err) {
      setError((err as Error).message)
    }
  }

  const update = <K extends keyof ServiceForm>(key: K, value: ServiceForm[K]) =>
    setForm((prev) => ({ ...prev, [key]: value }))

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle>{service ? `Edit ${initialID}` : 'Deploy service'}</DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">Could not save the service: {(save.error as Error).message}</Alert>
          )}
          {error && <Alert severity="error">{error}</Alert>}
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              label="Region"
              value={region}
              onChange={(e) => setRegion(e.target.value)}
              disabled={Boolean(service)}
              fullWidth
            />
            <TextField
              label="Service ID"
              value={serviceID}
              onChange={(e) => setServiceID(e.target.value)}
              disabled={Boolean(service)}
              fullWidth
            />
          </Stack>
          <TextField
            autoFocus={!service}
            label="Container image"
            value={form.image}
            onChange={(e) => update('image', e.target.value)}
            placeholder="us-docker.pkg.dev/project/repo/image:tag"
            required
            fullWidth
          />
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              label="Command (one per line)"
              value={form.command}
              onChange={(e) => update('command', e.target.value)}
              multiline
              minRows={2}
              fullWidth
            />
            <TextField
              label="Arguments (one per line)"
              value={form.args}
              onChange={(e) => update('args', e.target.value)}
              multiline
              minRows={2}
              fullWidth
            />
          </Stack>
          <TextField
            label="Environment variables (key=value per line)"
            value={form.env}
            onChange={(e) => update('env', e.target.value)}
            multiline
            minRows={2}
            fullWidth
          />
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              label="Container ports (comma separated)"
              value={form.ports}
              onChange={(e) => update('ports', e.target.value)}
              placeholder="8080"
              fullWidth
            />
            <TextField
              label="CPU limit"
              value={form.cpu}
              onChange={(e) => update('cpu', e.target.value)}
              placeholder="1"
              fullWidth
            />
            <TextField
              label="Memory limit"
              value={form.memory}
              onChange={(e) => update('memory', e.target.value)}
              placeholder="512Mi"
              fullWidth
            />
          </Stack>
          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              label="Service account"
              value={form.serviceAccount}
              onChange={(e) => update('serviceAccount', e.target.value)}
              placeholder="service-account@project.iam.gserviceaccount.com"
              fullWidth
            />
            <TextField
              select
              label="Ingress"
              value={form.ingress}
              onChange={(e) => update('ingress', e.target.value)}
              fullWidth
            >
              <MenuItem value="INGRESS_TRAFFIC_ALL">All</MenuItem>
              <MenuItem value="INGRESS_TRAFFIC_INTERNAL_ONLY">Internal only</MenuItem>
              <MenuItem value="INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER">Internal and load balancer</MenuItem>
            </TextField>
          </Stack>
          <GcpCodeEditor
            label="Advanced: Service body override (JSON)"
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
        <Button variant="contained" disabled={save.isPending} onClick={handleSave}>
          {service ? 'Save' : 'Deploy'}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
