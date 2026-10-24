import { serviceIconComponent } from './serviceIcons'

export interface GcpServiceIconProps {
  /** Service descriptor id (e.g. `storage`, `bigquery`); unknown ids fall back. */
  id: string
  /** Rendered glyph size in pixels. */
  size?: number
  /** Explicit color; omit to inherit `currentColor`. */
  color?: string
}

/** Renders the bundled product glyph for a GCP service id. */
export function GcpServiceIcon({ id, size = 24, color }: GcpServiceIconProps) {
  const Icon = serviceIconComponent(id)
  return <Icon aria-hidden sx={{ fontSize: size, color }} />
}
