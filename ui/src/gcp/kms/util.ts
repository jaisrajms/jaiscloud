/** Helpers for the Cloud KMS console's crypto operations. Payloads on the wire
 * are base64 (Cloud KMS bytes fields), so the dialogs encode/decode text and
 * compute digests client-side before calling the API. */

/** The digest algorithms Cloud KMS accepts for asymmetric signing, named as
 * WebCrypto spells them. */
export type DigestAlgorithm = 'SHA-256' | 'SHA-384' | 'SHA-512'

/** bytesToBase64 encodes raw bytes as standard base64. */
export function bytesToBase64(bytes: Uint8Array): string {
  let binary = ''
  for (const b of bytes) binary += String.fromCharCode(b)
  return btoa(binary)
}

/** base64ToBytes decodes standard base64 into raw bytes. */
export function base64ToBytes(value: string): Uint8Array {
  const binary = atob(value)
  const out = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i)
  return out
}

/** base64Encode encodes a UTF-8 string as base64 (unlike btoa, which mangles
 * code points above U+00FF). */
export function base64Encode(text: string): string {
  return bytesToBase64(new TextEncoder().encode(text))
}

/** base64Decode decodes base64 into a UTF-8 string. */
export function base64Decode(value: string): string {
  return new TextDecoder().decode(base64ToBytes(value))
}

/** digestAlgorithmFor derives the digest hash from a KMS key algorithm's
 * trailing SHA suffix (e.g. RSA_SIGN_PKCS1_2048_SHA256 -> SHA-256). */
export function digestAlgorithmFor(keyAlgorithm?: string): DigestAlgorithm | undefined {
  // HMAC_* also ends in a SHA suffix but is a MAC algorithm, not a signature.
  if (!keyAlgorithm || keyAlgorithm.startsWith('HMAC_')) return undefined
  const match = /SHA(256|384|512)$/.exec(keyAlgorithm)
  if (!match) return undefined
  return `SHA-${match[1]}` as DigestAlgorithm
}

/** digestFieldFor names the Cloud KMS Digest message field for an algorithm
 * (SHA-256 -> sha256). */
export function digestFieldFor(algorithm: DigestAlgorithm): 'sha256' | 'sha384' | 'sha512' {
  return `sha${algorithm.slice(4)}` as 'sha256' | 'sha384' | 'sha512'
}

/** shaDigestBase64 hashes text and returns the base64 digest Cloud KMS's
 * AsymmetricSignRequest expects. */
export async function shaDigestBase64(
  text: string,
  algorithm: DigestAlgorithm,
): Promise<string> {
  const digest = await crypto.subtle.digest(algorithm, new TextEncoder().encode(text))
  return bytesToBase64(new Uint8Array(digest))
}

/** isAsymmetricAlgorithm reports whether a key algorithm uses a public key
 * (RSA/EC), i.e. getPublicKey is meaningful. */
export function isAsymmetricAlgorithm(algorithm?: string): boolean {
  return Boolean(algorithm && (algorithm.startsWith('RSA_') || algorithm.startsWith('EC_')))
}

/** isSignAlgorithm reports whether an algorithm signs with a private key AND
 * uses a digest the console can compute (a trailing SHA-256/384/512). Ed25519
 * (signs the message, not a digest) and RSA_SIGN_RAW_* (the caller supplies an
 * already-encoded digest) are excluded because the console always sends a
 * Cloud KMS Digest, so it cannot drive them correctly. */
export function isSignAlgorithm(algorithm?: string): boolean {
  if (!algorithm) return false
  if (!(algorithm.startsWith('RSA_SIGN') || algorithm.startsWith('EC_SIGN'))) return false
  return digestAlgorithmFor(algorithm) !== undefined
}

/** isAsymmetricDecryptAlgorithm reports whether an algorithm decrypts with a
 * private key (RSA_DECRYPT_OAEP_*). */
export function isAsymmetricDecryptAlgorithm(algorithm?: string): boolean {
  return Boolean(algorithm?.startsWith('RSA_DECRYPT'))
}

/** isMacAlgorithm reports whether an algorithm produces HMAC tags. */
export function isMacAlgorithm(algorithm?: string): boolean {
  return Boolean(algorithm?.startsWith('HMAC_'))
}

/** downloadText triggers a browser download of a text file. The object URL is
 * revoked on the next tick so the (unattached) anchor's click has dispatched
 * before the blob is released. */
export function downloadText(filename: string, text: string): void {
  const url = URL.createObjectURL(new Blob([text], { type: 'application/x-pem-file' }))
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  anchor.click()
  setTimeout(() => URL.revokeObjectURL(url), 0)
}
