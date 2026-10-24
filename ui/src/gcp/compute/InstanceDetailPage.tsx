import type { ReactNode } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  IconButton,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import StopIcon from '@mui/icons-material/Stop'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import { deleteInstance, getInstance, startInstance, stopInstance } from '../../api/gcp/compute'
import { useAccount } from '../../context/AccountContext'
import { asArray, asRecord, shortDate, statusColor, text } from './util'
import { GcpPageTitle } from '../common/PageTitle'

interface NetworkInterface {
  name?: string
  network?: string
  networkIP?: string
  accessConfigs?: Array<{ natIP?: string; name?: string; type?: string }>
}

interface Disk {
  deviceName?: string
  source?: string
  sizeGb?: string
  boot?: boolean
}

interface MetadataItem {
  key?: string
  value?: string
}

function lastSegment(name?: string): string {
  if (!name) return ''
  const i = name.lastIndexOf('/')
  return i >= 0 ? name.slice(i + 1) : name
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <Box>
      <Typography variant="caption" color="text.secondary">
        {label}
      </Typography>
      <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
        {children}
      </Typography>
    </Box>
  )
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <Box sx={{ mt: 3 }}>
      <Typography variant="subtitle1" sx={{ mb: 1 }}>
        {title}
      </Typography>
      {children}
    </Box>
  )
}

/** A single Compute Engine instance: status, network, disks and metadata. */
export function InstanceDetailPage() {
  const { zone = '', instance: instanceName = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()

  const query = useQuery({
    queryKey: ['gcp', 'compute', 'instance', zone, instanceName, accountId],
    queryFn: () => getInstance(zone, instanceName),
    enabled: Boolean(zone && instanceName),
  })

  const invalidate = () => void queryClient.invalidateQueries({ queryKey: ['gcp', 'compute'] })

  const start = useMutation({
    mutationFn: () => startInstance(zone, instanceName),
    onSuccess: invalidate,
  })
  const stop = useMutation({
    mutationFn: () => stopInstance(zone, instanceName),
    onSuccess: invalidate,
  })
  const remove = useMutation({
    mutationFn: () => deleteInstance(zone, instanceName),
    onSuccess: () => {
      invalidate()
      navigate('/gcp/compute/instances')
    },
  })

  const data = query.data
  const busy = start.isPending || stop.isPending || remove.isPending
  const nics = asArray<NetworkInterface>(data?.networkInterfaces)
  const disks = asArray<Disk>(data?.disks)
  const metadataItems = asArray<MetadataItem>(asRecord(data?.metadata).items)

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton component={RouterLink} to="/gcp/compute/instances" aria-label="Back to instances">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="compute">{instanceName}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            {zone} · project {accountId || '—'}
          </Typography>
        </Box>
        <Button
          variant="contained"
          startIcon={<PlayArrowIcon />}
          disabled={busy || query.isLoading || data?.status === 'RUNNING'}
          onClick={() => start.mutate()}
        >
          Start
        </Button>
        <Button
          variant="outlined"
          startIcon={<StopIcon />}
          disabled={busy || query.isLoading || data?.status !== 'RUNNING'}
          onClick={() => stop.mutate()}
        >
          Stop
        </Button>
        <Button
          color="error"
          startIcon={<DeleteOutlineIcon />}
          disabled={busy}
          onClick={() => remove.mutate()}
        >
          Delete
        </Button>
      </Stack>

      {query.isError && <Alert severity="error">Failed to load the instance.</Alert>}
      {query.isLoading && <CircularProgress size={24} />}
      {start.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Start failed.
        </Alert>
      )}
      {stop.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Stop failed.
        </Alert>
      )}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Delete failed.
        </Alert>
      )}

      {data && (
        <Box sx={{ maxWidth: 1000 }}>
          <Divider />
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
              gap: 2,
              mt: 2,
            }}
          >
            <Field label="Status">
              <Chip size="small" label={data.status || 'UNKNOWN'} color={statusColor(data.status)} />
            </Field>
            <Field label="Machine type">{data.machineType || '—'}</Field>
            <Field label="CPU platform">{data.cpuPlatform || '—'}</Field>
            <Field label="Zone">{data.zone || zone}</Field>
            <Field label="Internal IP">{data.internalIp || '—'}</Field>
            <Field label="External IP">{data.externalIp || '—'}</Field>
            <Field label="Created">{shortDate(data.creationTimestamp)}</Field>
            <Field label="Instance ID">{data.id || '—'}</Field>
          </Box>

          {data.description && (
            <Section title="Description">
              <Typography variant="body2">{data.description}</Typography>
            </Section>
          )}

          <Section title="Labels">
            {data.labels && Object.keys(data.labels).length > 0 ? (
              <Stack direction="row" spacing={0.5} sx={{ flexWrap: 'wrap', gap: 0.5 }}>
                {Object.entries(data.labels).map(([k, v]) => (
                  <Chip key={k} size="small" variant="outlined" label={`${k}=${v}`} />
                ))}
              </Stack>
            ) : (
              <Typography variant="body2" color="text.secondary">
                No labels.
              </Typography>
            )}
          </Section>

          <Section title="Network interfaces">
            {nics.length === 0 ? (
              <Typography variant="body2" color="text.secondary">
                No network interfaces.
              </Typography>
            ) : (
              <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Name</TableCell>
                      <TableCell>Network</TableCell>
                      <TableCell>Internal IP</TableCell>
                      <TableCell>External IP</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {nics.map((nic, i) => {
                      const external = asArray<{ natIP?: string }>(nic.accessConfigs)[0]?.natIP
                      return (
                        <TableRow key={nic.name ?? i}>
                          <TableCell>{nic.name || '—'}</TableCell>
                          <TableCell>{lastSegment(nic.network) || '—'}</TableCell>
                          <TableCell>{nic.networkIP || '—'}</TableCell>
                          <TableCell>{external || '—'}</TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
          </Section>

          <Section title="Disks">
            {disks.length === 0 ? (
              <Typography variant="body2" color="text.secondary">
                No attached disks.
              </Typography>
            ) : (
              <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Device</TableCell>
                      <TableCell>Source</TableCell>
                      <TableCell>Size (GB)</TableCell>
                      <TableCell>Boot</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {disks.map((disk, i) => (
                      <TableRow key={disk.deviceName ?? i}>
                        <TableCell>{disk.deviceName || '—'}</TableCell>
                        <TableCell>{lastSegment(disk.source) || '—'}</TableCell>
                        <TableCell>{text(disk.sizeGb) || '—'}</TableCell>
                        <TableCell>{disk.boot ? 'yes' : 'no'}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
          </Section>

          <Section title="Metadata">
            {metadataItems.length === 0 ? (
              <Typography variant="body2" color="text.secondary">
                No instance metadata.
              </Typography>
            ) : (
              <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>Key</TableCell>
                      <TableCell>Value</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {metadataItems.map((item) => (
                      <TableRow key={item.key}>
                        <TableCell>{item.key || '—'}</TableCell>
                        <TableCell sx={{ overflowWrap: 'anywhere' }}>{item.value || '—'}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TableContainer>
            )}
          </Section>

          {data.selfLink && (
            <Section title="Self link">
              <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
                {data.selfLink}
              </Typography>
            </Section>
          )}
        </Box>
      )}
    </Box>
  )
}
