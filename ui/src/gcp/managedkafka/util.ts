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

/** Render a map as a `key=value`-per-line block. */
export function formatKV(map?: Record<string, string>): string {
  if (!map) return ''
  return Object.entries(map)
    .map(([key, value]) => `${key}=${value}`)
    .join('\n')
}

/**
 * The ACL id vocabulary the Managed Kafka API accepts. Each entry maps a console
 * label to the id prefix ("" for a fixed singleton id, "/" for a name suffix).
 */
export const ACL_RESOURCES: { label: string; prefix: string }[] = [
  { label: 'Cluster', prefix: 'cluster' },
  { label: 'All topics', prefix: 'allTopics' },
  { label: 'All consumer groups', prefix: 'allConsumerGroups' },
  { label: 'All transactional ids', prefix: 'allTransactionalIds' },
  { label: 'Topic', prefix: 'topic/' },
  { label: 'Topic prefix', prefix: 'topicPrefixed/' },
  { label: 'Consumer group', prefix: 'consumerGroup/' },
  { label: 'Consumer group prefix', prefix: 'consumerGroupPrefixed/' },
  { label: 'Transactional id', prefix: 'transactionalId/' },
  { label: 'Transactional id prefix', prefix: 'transactionalIdPrefixed/' },
]

/** The ACL operations the Managed Kafka API accepts. */
export const ACL_OPERATIONS = [
  'ALL',
  'READ',
  'WRITE',
  'CREATE',
  'DELETE',
  'ALTER',
  'DESCRIBE',
  'CLUSTER_ACTION',
  'DESCRIBE_CONFIGS',
  'ALTER_CONFIGS',
  'IDEMPOTENT_WRITE',
]

/** Split an ACL id back into a console resource prefix and an optional name. */
export function splitAclID(id: string): { prefix: string; name: string } {
  for (const resource of ACL_RESOURCES) {
    if (resource.prefix.endsWith('/') && id.startsWith(resource.prefix)) {
      return { prefix: resource.prefix, name: id.slice(resource.prefix.length) }
    }
  }
  return { prefix: id, name: '' }
}

/** Compose an ACL id from a resource prefix and an optional name. */
export function composeAclID(prefix: string, name: string): string {
  return prefix.endsWith('/') ? `${prefix}${name.trim()}` : prefix
}
