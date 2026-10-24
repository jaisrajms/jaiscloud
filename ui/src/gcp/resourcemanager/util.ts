import type { Project } from '../../api/gcp/resourcemanager'

/** shortDate renders an RFC3339 timestamp for display, or em dash when absent. */
export function shortDate(value?: string): string {
  if (!value) return '—'
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? value : d.toLocaleString()
}

/** stateColor maps a project lifecycle state onto an MUI Chip color. */
export function stateColor(state: string): 'success' | 'default' | 'warning' | 'error' {
  switch (state) {
    case 'ACTIVE':
      return 'success'
    case 'DELETE_REQUESTED':
      return 'warning'
    default:
      return 'default'
  }
}

/** stateLabel renders a lifecycle state for display, or em dash when absent. */
export function stateLabel(project: Project): string {
  return project.state || '—'
}

/**
 * The documented project-id grammar: 6–30 characters, lowercase ASCII letters,
 * digits, or hyphens, starting with a letter and not ending with a hyphen.
 */
export const PROJECT_ID_RE = /^[a-z][a-z0-9-]{4,28}[a-z0-9]$/

/** True when id is a valid Cloud Resource Manager project id. */
export function validProjectId(id: string): boolean {
  return PROJECT_ID_RE.test(id)
}

/** Parse a `key=value`-per-line block into a map, or undefined when empty. */
export function parseKV(text: string): Record<string, string> | undefined {
  const out: Record<string, string> = {}
  for (const raw of text.split('\n')) {
    const line = raw.trim()
    if (!line || line.startsWith('#')) continue
    const i = line.indexOf('=')
    if (i < 0) continue
    const key = line.slice(0, i).trim()
    if (!key) continue
    out[key] = line.slice(i + 1).trim()
  }
  return Object.keys(out).length > 0 ? out : undefined
}

