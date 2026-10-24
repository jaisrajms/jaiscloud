/** Display names for the emulated clouds, shared by every console home. */
export const CLOUD_NAMES: Record<string, string> = {
  aws: 'AWS',
  gcp: 'Google Cloud',
  azure: 'Azure',
}

const DEFAULT_CLOUD_NAME = 'AWS'

/** Human-readable cloud name for the active cloud (unknown clouds fall back to AWS). */
export function cloudName(cloud: string | undefined): string {
  return CLOUD_NAMES[cloud ?? ''] ?? DEFAULT_CLOUD_NAME
}
