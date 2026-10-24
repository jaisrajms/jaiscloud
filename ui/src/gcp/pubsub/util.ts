/** Encode a UTF-8 string as base64, the Pub/Sub wire representation of Data. */
export function encodeMessageData(text: string): string {
  const bytes = new TextEncoder().encode(text)
  let binary = ''
  bytes.forEach((b) => {
    binary += String.fromCharCode(b)
  })
  return btoa(binary)
}

/** Parse a JSON object of message attributes, or undefined when blank. */
export function parseAttributes(text: string): Record<string, string> | undefined {
  const trimmed = text.trim()
  if (!trimmed) return undefined
  return JSON.parse(trimmed) as Record<string, string>
}
