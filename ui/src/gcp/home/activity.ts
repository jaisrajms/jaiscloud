import type { DiscoveryProject } from '../../api/gcp/projects'
import type { ProjectRowInfo } from '../chrome/navModel'

/** How recent another project's activity must be to raise the banner. */
export const BANNER_WINDOW_MS = 15 * 60_000

function lastSeenMs(project: DiscoveryProject): number {
  const t = project.lastSeen ? Date.parse(project.lastSeen) : NaN
  return Number.isNaN(t) ? -Infinity : t
}

/**
 * Observed projects other than the one in view, newest activity first. The
 * project in view is excluded: its own resources already fill the overview.
 */
export function activityRows(
  projects: DiscoveryProject[],
  current: string,
): ProjectRowInfo[] {
  return projects
    .filter((project) => project.kind === 'observed' && project.id !== current)
    .sort((a, b) => lastSeenMs(b) - lastSeenMs(a) || a.id.localeCompare(b.id))
    .map((project) => ({
      id: project.id,
      kind: 'observed',
      firstSeen: project.firstSeen,
      lastSeen: project.lastSeen,
      services: project.services,
      requests: project.requests,
    }))
}

/**
 * The newest project written to within `windowMs`, or undefined when every
 * other project is older. The caller only shows the banner when the project in
 * view has no resources at all.
 */
export function activeBannerProject(
  projects: DiscoveryProject[],
  current: string,
  now = Date.now(),
  windowMs = BANNER_WINDOW_MS,
): ProjectRowInfo | undefined {
  return activityRows(projects, current).find((project) => {
    const t = project.lastSeen ? Date.parse(project.lastSeen) : NaN
    return !Number.isNaN(t) && now - t <= windowMs
  })
}
