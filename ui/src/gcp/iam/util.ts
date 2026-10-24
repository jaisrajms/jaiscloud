/** Base64-encode UTF-8 text. `btoa` alone mangles any non-Latin-1 byte, so the
 * string is first encoded to UTF-8 bytes. */
export function base64Encode(text: string): string {
  const bytes = new TextEncoder().encode(text)
  let binary = ''
  for (const b of bytes) {
    binary += String.fromCharCode(b)
  }
  return btoa(binary)
}
