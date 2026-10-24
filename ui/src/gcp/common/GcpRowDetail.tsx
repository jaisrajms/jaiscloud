import type { ReactNode } from 'react'
import { Stack, Typography } from '@mui/material'

export interface GcpRowDetailField {
  key: string
  label?: string
  render?: (value: unknown) => ReactNode
}

export interface GcpRowDetailProps {
  /** Row object shown in the `GcpDataTable` info panel. */
  row: object
  /** Fields and order; defaults to every primitive scalar field on the row. */
  fields?: GcpRowDetailField[]
}

/** `camelCase`/`snake_case` key -> `Title Case` label. */
function humanize(key: string): string {
  return key
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2')
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase())
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined || value === '') return '—'
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'
  if (value instanceof Date) return value.toLocaleString()
  if (typeof value === 'number') return String(value)
  if (typeof value === 'string') return value
  return JSON.stringify(value)
}

/** True for values rendered directly without JSON stringification. */
function isPrimitive(value: unknown): boolean {
  return (
    value === null ||
    value === undefined ||
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean'
  )
}

/**
 * Generic key/value body for the `GcpDataTable` info panel. Prefer passing an
 * explicit `fields` list; the fallback renders every primitive scalar field.
 */
export function GcpRowDetail({ row, fields }: GcpRowDetailProps) {
  const record = row as Record<string, unknown>
  const resolved: GcpRowDetailField[] =
    fields ??
    Object.keys(record)
      .filter((key) => isPrimitive(record[key]))
      .map((key) => ({ key, label: humanize(key) }))

  return (
    <Stack spacing={1.25}>
      {resolved.map((field) => {
        const value = record[field.key]
        return (
          <Stack key={field.key} spacing={0.25}>
            <Typography variant="caption" color="text.secondary">
              {field.label ?? humanize(field.key)}
            </Typography>
            <Typography variant="body2" sx={{ overflowWrap: 'anywhere' }}>
              {field.render ? field.render(value) : formatValue(value)}
            </Typography>
          </Stack>
        )
      })}
    </Stack>
  )
}
