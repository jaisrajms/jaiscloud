import { useState } from 'react'
import type { ReactNode } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Divider,
  Stack,
  Typography,
} from '@mui/material'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import { useNavigate, useParams } from 'react-router-dom'
import {
  deleteServiceLevelObjective,
  getServiceLevelObjective,
} from '../../api/gcp/monitoring'
import { useAccount } from '../../context/AccountContext'
import { SLODialog } from './SLODialog'
import { formatGoal, periodLabel, sliLabel } from './sloUtil'
import { GcpPageHeader } from '../common/GcpPageHeader'

/** A small label/value row for the SLO overview. */
function Detail({ label, children }: { label: string; children: ReactNode }) {
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

/** A single Service Monitoring SLO: overview, indicator and edit/delete. */
export function SLODetailPage() {
  const { service = '', slo: sloName = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [editOpen, setEditOpen] = useState(false)

  const key = ['gcp', 'monitoring', 'slo', accountId, service, sloName]

  const detail = useQuery({
    queryKey: key,
    queryFn: () => getServiceLevelObjective(service, sloName),
    enabled: Boolean(service && sloName),
  })

  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: ['gcp', 'monitoring', 'slos'] })
    void queryClient.invalidateQueries({ queryKey: ['gcp', 'monitoring', 'slo'] })
  }

  const remove = useMutation({
    mutationFn: () => deleteServiceLevelObjective(service, sloName),
    onSuccess: () => {
      invalidate()
      navigate('/gcp/monitoring/slo')
    },
  })

  const slo = detail.data

  return (
    <Box>
      <GcpPageHeader
        id="monitoring"
        title={slo?.displayName || sloName}
        subtitle={`Service level objective · ${service}`}
        backTo="/gcp/monitoring/slo"
        backAriaLabel="Back to SLOs"
        actions={
          slo ? (
            <>
              <Button startIcon={<EditOutlinedIcon />} onClick={() => setEditOpen(true)}>
                Edit
              </Button>
              <Button
                color="error"
                startIcon={<DeleteOutlineIcon />}
                disabled={remove.isPending}
                onClick={() => remove.mutate()}
              >
                Delete
              </Button>
            </>
          ) : null
        }
      />

      {detail.isError && <Alert severity="error">Failed to load the SLO.</Alert>}
      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          Could not delete the SLO: {(remove.error as Error).message}
        </Alert>
      )}
      {detail.isLoading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
          <CircularProgress />
        </Box>
      )}

      {slo && (
        <>
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(3, 1fr)' },
              gap: 2,
            }}
          >
            <Detail label="Display name">{slo.displayName || '—'}</Detail>
            <Detail label="Goal">{formatGoal(slo.goal)}</Detail>
            <Detail label="Period">{periodLabel(slo)}</Detail>
            <Detail label="SLI">{sliLabel(slo.serviceLevelIndicator)}</Detail>
            <Detail label="Name">{slo.name}</Detail>
          </Box>

          <Divider sx={{ my: 3 }} />
          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            Service level indicator
          </Typography>
          <Box
            component="pre"
            sx={{
              m: 0,
              p: 2,
              borderRadius: 1,
              bgcolor: 'action.hover',
              fontSize: 13,
              overflowX: 'auto',
            }}
          >
            {JSON.stringify(slo.serviceLevelIndicator ?? {}, null, 2)}
          </Box>

          <SLODialog
            open={editOpen}
            onClose={() => setEditOpen(false)}
            service={service}
            initial={slo}
            onSaved={invalidate}
          />
        </>
      )}
    </Box>
  )
}
