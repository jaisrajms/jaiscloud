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
  createServiceLevelObjective,
  updateServiceLevelObjective,
  type ServiceLevelIndicator,
  type ServiceLevelObjective,
  type ServiceLevelObjectiveWriteInput,
} from '../../api/gcp/monitoring'
import { GcpCodeEditor } from '../common/GcpCodeEditor'
import { resourceID } from './util'
import {
  CALENDAR_PERIODS,
  basicSli,
  goalToPercent,
  parseRollingDurationDays,
  percentToGoal,
  rollingDaysToDuration,
  sliFormFromIndicator,
  validateCustomSli,
  validateGoalPercent,
  validateRollingDays,
  type CalendarPeriod,
  type SliMode,
} from './sloUtil'

export interface SLODialogProps {
  open: boolean
  onClose: () => void
  /** The Service Monitoring service that owns the SLO. */
  service: string
  /** When set, the dialog edits this SLO; otherwise it creates a new one. */
  initial?: ServiceLevelObjective
  onSaved?: (slo: ServiceLevelObjective) => void
}

type PeriodMode = 'rolling' | 'calendar'

const DEFAULT_GOAL_PERCENT = 99.9

/** Create or edit a Service Monitoring service level objective. */
export function SLODialog({ open, onClose, service, initial, onSaved }: SLODialogProps) {
  const queryClient = useQueryClient()
  const editing = Boolean(initial)
  const [displayName, setDisplayName] = useState('')
  const [goalPercent, setGoalPercent] = useState(String(DEFAULT_GOAL_PERCENT))
  const [periodMode, setPeriodMode] = useState<PeriodMode>('rolling')
  const [rollingDays, setRollingDays] = useState('1')
  const [calendarPeriod, setCalendarPeriod] = useState<CalendarPeriod>('DAY')
  const [sliMode, setSliMode] = useState<SliMode>('availability')
  const [method, setMethod] = useState('')
  const [threshold, setThreshold] = useState('500ms')
  const [sliJson, setSliJson] = useState('')

  useEffect(() => {
    if (!open) return
    const form = sliFormFromIndicator(initial?.serviceLevelIndicator)
    setDisplayName(initial?.displayName ?? '')
    setGoalPercent(
      String(initial?.goal !== undefined ? goalToPercent(initial.goal) : DEFAULT_GOAL_PERCENT),
    )
    const days = parseRollingDurationDays(initial?.rollingPeriod)
    setPeriodMode(initial?.calendarPeriod ? 'calendar' : 'rolling')
    setRollingDays(String(days ?? 1))
    setCalendarPeriod(
      CALENDAR_PERIODS.find((period) => period === initial?.calendarPeriod) ?? 'DAY',
    )
    setSliMode(form.mode)
    setMethod(form.method)
    setThreshold(form.threshold)
    setSliJson(form.customJson)
  }, [open, initial])

  const initialBasic = initial?.serviceLevelIndicator?.basicSli
  const goalError = validateGoalPercent(Number(goalPercent))
  const rollingError = periodMode === 'rolling' ? validateRollingDays(Number(rollingDays)) : null
  const sliError =
    sliMode === 'custom'
      ? validateCustomSli(sliJson)
      : sliMode === 'latency' && !threshold.trim()
        ? 'Latency threshold is required.'
        : null
  const formError = goalError ?? rollingError ?? sliError

  const save = useMutation({
    mutationFn: () => {
      const percent = Number(goalPercent)
      if (goalError) throw new Error(goalError)

      let indicator: ServiceLevelIndicator
      if (sliMode === 'custom') {
        if (sliError) throw new Error(sliError)
        indicator = JSON.parse(sliJson) as ServiceLevelIndicator
      } else {
        if (sliMode === 'latency' && !threshold.trim()) {
          throw new Error('Latency threshold is required.')
        }
        indicator = { basicSli: basicSli(sliMode, method, threshold, initialBasic) }
      }

      let period: { rollingPeriod?: string; calendarPeriod?: string }
      if (periodMode === 'rolling') {
        const days = Number(rollingDays)
        const error = validateRollingDays(days)
        if (error) throw new Error(error)
        period = { rollingPeriod: rollingDaysToDuration(days) }
      } else {
        period = { calendarPeriod }
      }

      const body: ServiceLevelObjectiveWriteInput = {
        ...(displayName.trim() ? { displayName: displayName.trim() } : {}),
        // The dialog does not edit labels, so an edit must carry the stored ones
        // through the full-replace update rather than dropping them.
        ...(initial?.userLabels ? { userLabels: initial.userLabels } : {}),
        goal: percentToGoal(percent),
        serviceLevelIndicator: indicator,
        ...period,
      }
      return initial
        ? updateServiceLevelObjective(service, resourceID(initial.name), body)
        : createServiceLevelObjective(service, body)
    },
    onSuccess: (saved) => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'monitoring', 'slos'] })
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'monitoring', 'slo'] })
      onSaved?.(saved)
      onClose()
    },
  })

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="sm">
      <DialogTitle>
        {editing ? `Edit ${initial?.displayName || resourceID(initial?.name)}` : 'Create SLO'}
      </DialogTitle>
      <DialogContent>
        <Stack spacing={2} sx={{ mt: 1 }}>
          {save.isError && (
            <Alert severity="error">Could not save the SLO: {(save.error as Error).message}</Alert>
          )}
          <TextField
            autoFocus
            label="Display name (optional)"
            value={displayName}
            onChange={(event) => setDisplayName(event.target.value)}
            placeholder="99.9% availability"
            fullWidth
          />
          <TextField
            label="Goal (%)"
            type="number"
            value={goalPercent}
            onChange={(event) => setGoalPercent(event.target.value)}
            error={Boolean(goalError)}
            helperText={goalError ?? 'A percentage greater than 0 and at most 99.99.'}
            slotProps={{ htmlInput: { min: 0, max: 99.99, step: 0.01 } }}
            fullWidth
          />
          <TextField
            select
            label="Period type"
            value={periodMode}
            onChange={(event) => setPeriodMode(event.target.value as PeriodMode)}
            fullWidth
          >
            <MenuItem value="rolling">Rolling</MenuItem>
            <MenuItem value="calendar">Calendar</MenuItem>
          </TextField>
          {periodMode === 'rolling' ? (
            <TextField
              label="Rolling period (days)"
              type="number"
              value={rollingDays}
              onChange={(event) => setRollingDays(event.target.value)}
              error={Boolean(rollingError)}
              helperText={rollingError ?? 'A whole number of days between 1 and 30.'}
              slotProps={{ htmlInput: { min: 1, max: 30, step: 1 } }}
              fullWidth
            />
          ) : (
            <TextField
              select
              label="Calendar period"
              value={calendarPeriod}
              onChange={(event) => setCalendarPeriod(event.target.value as CalendarPeriod)}
              fullWidth
            >
              {CALENDAR_PERIODS.map((period) => (
                <MenuItem key={period} value={period}>
                  {period}
                </MenuItem>
              ))}
            </TextField>
          )}
          <TextField
            select
            label="SLI type"
            value={sliMode}
            onChange={(event) => {
              const next = event.target.value as SliMode
              // Seed the JSON view from the basic form so switching to Custom
              // shows (rather than blanks) the indicator that would be saved.
              if (next === 'custom' && sliMode !== 'custom' && !sliJson.trim()) {
                setSliJson(
                  JSON.stringify(
                    { basicSli: basicSli(sliMode, method, threshold, initialBasic) },
                    null,
                    2,
                  ),
                )
              }
              setSliMode(next)
            }}
            fullWidth
          >
            <MenuItem value="availability">Basic · availability</MenuItem>
            <MenuItem value="latency">Basic · latency</MenuItem>
            <MenuItem value="custom">Custom (JSON)</MenuItem>
          </TextField>
          {sliMode !== 'custom' && (
            <TextField
              label="Method (optional)"
              value={method}
              onChange={(event) => setMethod(event.target.value)}
              placeholder="GET"
              helperText="Restrict the SLI to one request method, e.g. GET."
              fullWidth
            />
          )}
          {sliMode === 'latency' && (
            <TextField
              label="Latency threshold"
              value={threshold}
              onChange={(event) => setThreshold(event.target.value)}
              error={Boolean(sliError)}
              helperText={sliError ?? 'The latency bound, e.g. 500ms or 1s.'}
              fullWidth
            />
          )}
          {sliMode === 'custom' && (
            <GcpCodeEditor
              label="Service level indicator (JSON)"
              value={sliJson}
              onChange={setSliJson}
              error={sliError}
              language="json"
              minRows={3}
            />
          )}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Cancel</Button>
        <Button
          variant="contained"
          disabled={Boolean(formError) || save.isPending}
          onClick={() => save.mutate()}
        >
          {editing ? 'Save' : 'Create'}
        </Button>
      </DialogActions>
    </Dialog>
  )
}
