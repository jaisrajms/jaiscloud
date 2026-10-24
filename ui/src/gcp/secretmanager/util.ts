/** Parse "key=value" lines into a string map. Returns undefined when empty. */
export function parseKeyValueLines(text: string): Record<string, string> | undefined {
  const entries = text
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const i = line.indexOf('=')
      if (i < 0) return [line, ''] as const
      return [line.slice(0, i).trim(), line.slice(i + 1).trim()] as const
    })
    .filter(([k]) => k.length > 0)
  if (entries.length === 0) return undefined
  return Object.fromEntries(entries)
}

/** Render a string map as "key=value" lines for editing. */
export function formatKeyValueLines(labels?: Record<string, string>): string {
  if (!labels) return ''
  return Object.entries(labels)
    .map(([k, v]) => `${k}=${v}`)
    .join('\n')
}

/** Parse a comma-separated topic list; returns undefined when empty. */
export function parseTopicList(text: string): string[] | undefined {
  const topics = text
    .split(',')
    .map((t) => t.trim())
    .filter(Boolean)
  return topics.length > 0 ? topics : undefined
}

/** Render a topic list as comma-separated text for editing. */
export function formatTopicList(topics?: string[]): string {
  return topics && topics.length > 0 ? topics.join(', ') : ''
}

/** Base64-encode UTF-8 text (the wire payload.data encoding). */
export function toBase64(text: string): string {
  const bytes = new TextEncoder().encode(text)
  let binary = ''
  bytes.forEach((b) => {
    binary += String.fromCharCode(b)
  })
  return btoa(binary)
}

/** Base64-decode to UTF-8 text; falls back to the raw string if not decodable. */
export function fromBase64(data: string): string {
  try {
    const binary = atob(data)
    const bytes = Uint8Array.from(binary, (c) => c.charCodeAt(0))
    return new TextDecoder().decode(bytes)
  } catch {
    return data
  }
}
