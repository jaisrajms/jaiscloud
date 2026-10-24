import type { ReactNode } from 'react'
import {
  Box,
  CircularProgress,
  IconButton,
  InputAdornment,
  Stack,
  TextField,
  Tooltip,
} from '@mui/material'
import RefreshIcon from '@mui/icons-material/Refresh'
import SearchIcon from '@mui/icons-material/Search'

export interface GcpToolbarProps {
  /** Current filter query (renders the filter box when `onFilterChange` is set). */
  filter?: string
  onFilterChange?: (value: string) => void
  filterPlaceholder?: string
  filterAriaLabel?: string
  /** Refresh handler; the refresh button is hidden when omitted. */
  onRefresh?: () => void
  refreshing?: boolean
  /** Trailing controls, typically the primary create action. */
  children?: ReactNode
}

/** List-page toolbar: filter box, refresh control and trailing actions. */
export function GcpToolbar({
  filter,
  onFilterChange,
  filterPlaceholder = 'Filter',
  filterAriaLabel,
  onRefresh,
  refreshing = false,
  children,
}: GcpToolbarProps) {
  return (
    <Stack
      direction="row"
      spacing={1}
      sx={{ alignItems: 'center', mb: 1.5, flexWrap: 'wrap', rowGap: 1 }}
    >
      {onFilterChange && (
        <TextField
          size="small"
          variant="outlined"
          value={filter ?? ''}
          onChange={(event) => onFilterChange(event.target.value)}
          placeholder={filterPlaceholder}
          sx={{ minWidth: 240, flexGrow: { xs: 1, sm: 0 } }}
          slotProps={{
            input: {
              startAdornment: (
                <InputAdornment position="start">
                  <SearchIcon fontSize="small" sx={{ color: 'text.secondary' }} />
                </InputAdornment>
              ),
            },
            htmlInput: { 'aria-label': filterAriaLabel ?? filterPlaceholder },
          }}
        />
      )}
      <Box sx={{ flexGrow: 1 }} />
      {onRefresh && (
        <Tooltip title="Refresh">
          <span>
            <IconButton
              size="small"
              onClick={onRefresh}
              disabled={refreshing}
              aria-label="Refresh"
            >
              {refreshing ? <CircularProgress size={18} /> : <RefreshIcon fontSize="small" />}
            </IconButton>
          </span>
        </Tooltip>
      )}
      {children}
    </Stack>
  )
}
