/**
 * Firestore composite-index helpers.
 *
 * The console's index builder edits field rows (a field path plus a mode) and
 * renders stored indexes back as human-readable text. Both directions are pure
 * so they can be unit-tested and shared between the page and the dialog.
 */

import type { FirestoreIndexField } from '../../api/gcp/firestore'

/** The wire order enum plus the array-contains pseudo-mode. */
export type IndexFieldMode = 'ASCENDING' | 'DESCENDING' | 'CONTAINS'

/** One editable field row in the index builder. */
export interface IndexFieldRow {
  fieldPath: string
  mode: IndexFieldMode
}

/** indexFieldFromRow converts a builder row into a wire index field, or null
 * when the field path is empty. `CONTAINS` becomes an `arrayConfig`, every other
 * mode a directional `order`. */
export function indexFieldFromRow(row: IndexFieldRow): FirestoreIndexField | null {
  const fieldPath = row.fieldPath.trim()
  if (!fieldPath) return null
  return row.mode === 'CONTAINS'
    ? { fieldPath, arrayConfig: 'CONTAINS' }
    : { fieldPath, order: row.mode }
}

/** formatIndexField renders one index field as `path ASC` / `path DESC` /
 * `path CONTAINS`. */
export function formatIndexField(field: FirestoreIndexField): string {
  if (field.arrayConfig) return `${field.fieldPath} ${field.arrayConfig}`
  return `${field.fieldPath} ${field.order ?? ''}`.trim()
}

/** formatIndexFields joins an index's fields for table display. */
export function formatIndexFields(fields: FirestoreIndexField[]): string {
  if (fields.length === 0) return '—'
  return fields.map(formatIndexField).join(', ')
}
