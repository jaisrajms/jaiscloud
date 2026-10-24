/** parseJsonObject parses editor text, requiring a JSON object at the top
 * level. It returns either the parsed object or a human-readable error. */
export function parseJsonObject(text: string): {
  value?: Record<string, unknown>
  error?: string
} {
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch (err) {
    return { error: err instanceof Error ? err.message : 'invalid JSON' }
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return { error: 'Document data must be a JSON object' }
  }
  return { value: parsed as Record<string, unknown> }
}

/** pathSegments splits a Firestore collection/document path into its non-empty
 * segments, tolerant of a stray leading/trailing '/'. */
export function pathSegments(path: string): string[] {
  return path.split('/').filter((segment) => segment.length > 0)
}

/** collectionId returns the last (leaf) segment of a collection path, which is
 * the id shown as the page title. */
export function collectionId(collectionPath: string): string {
  const segments = pathSegments(collectionPath)
  return segments[segments.length - 1] ?? collectionPath
}

/** validateCollectionPath returns an error message for an invalid collection
 * path, or null when it is valid. A collection path is a root id (`users`) or
 * an alternating nested path (`users/alice/orders`): non-empty segments, no
 * "." / "..", ending on a collection id (odd segment count). Mirrors the
 * backend `collectionParam` validation. */
export function validateCollectionPath(path: string): string | null {
  if (!path) return 'Collection ID is required'
  const segments = path.split('/')
  for (const segment of segments) {
    if (!segment) return 'Collection path has an empty segment'
    if (segment === '.' || segment === '..') {
      return 'Collection path must not contain "." or ".."'
    }
  }
  if (segments.length % 2 === 0) {
    return 'Collection path must end with a collection ID, e.g. users/alice/orders'
  }
  return null
}

/** isNestedCollection reports whether a collection path is itself under a
 * document (i.e. a subcollection) rather than a root collection. Mirrors the
 * backend rule: a collection path has an odd number of segments and a nested
 * one has more than one. */
export function isNestedCollection(collectionPath: string): boolean {
  const length = pathSegments(collectionPath).length
  return length > 1 && length % 2 === 1
}

/** parentDocument locates the document that owns a nested collection. It returns
 * the parent collection path and document id, or undefined for a root
 * collection. E.g. `users/alice/orders` -> `{ collection: 'users', document:
 * 'alice' }`. */
export function parentDocument(
  collectionPath: string,
): { collection: string; document: string } | undefined {
  const segments = pathSegments(collectionPath)
  if (segments.length < 3) return undefined
  return {
    collection: segments.slice(0, -2).join('/'),
    document: segments[segments.length - 2] as string,
  }
}

/**
 * Firestore path URL codec.
 *
 * A collection/document path is slash-separated, but it travels through React
 * Router as a single route param. The naive `encodeURIComponent(path)` scheme
 * breaks: React Router decodes each matched segment with `decodeURIComponent`
 * and then rewrites any `%2F` back to `/`, so an id containing the literal
 * uppercase sequence `%2F` (encoded as `%252F`) is decoded twice — once to
 * `%2F`, then to `/` — and addresses the wrong path.
 *
 * Instead we use the console-style `~2F` escape (the real Cloud/Firebase
 * Firestore console addresses `/<collection>/<document>` as
 * `/~2F<collection>~2F<document>`), extended with `~7E` for a literal `~`. We
 * apply it *after* percent-encoding so the resulting param contains no `%` and
 * no `/` at all: React Router then leaves it completely untouched. `~` in an id
 * is escaped as `~7E` first, so the `~2F` sequence stays unambiguous.
 *
 * Note: a Firestore id may not contain `/`, so the only `/` any input has are
 * the path separators that `encodeFirestorePath` turns into `~2F`. That is what
 * keeps `decodeFirestorePath`'s `~2F` split unambiguous.
 */

/** encodeFirestoreId makes one collection/document id safe to place in a URL
 * path segment. E.g. `a/b` -> `a~2Fb`, `a%2Fb` -> `a~252Fb`. */
export function encodeFirestoreId(id: string): string {
  return encodeURIComponent(id.replace(/~/g, '~7E')).replace(/%/g, '~')
}

/** decodeFirestoreId reverses encodeFirestoreId. A malformed segment (e.g. a
 * hand-typed URL with a stray `%` or a `~` not followed by two hex digits)
 * decodes to itself rather than throwing. */
export function decodeFirestoreId(encoded: string): string {
  try {
    return decodeURIComponent(encoded.replace(/~/g, '%'))
  } catch {
    return encoded
  }
}

/** encodeFirestorePath encodes a slash-separated collection path into a single
 * URL path segment. E.g. `users/alice/orders` -> `users~2Falice~2Forders`. */
export function encodeFirestorePath(path: string): string {
  return pathSegments(path).map(encodeFirestoreId).join('~2F')
}

/** decodeFirestorePath reverses encodeFirestorePath. */
export function decodeFirestorePath(encoded: string): string {
  return encoded.split('~2F').map(decodeFirestoreId).join('/')
}
