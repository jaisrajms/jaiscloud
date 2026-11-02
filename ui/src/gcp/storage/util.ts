import { NOTIFICATION_EVENT_TYPES } from '../../api/gcp/storage'

/** isKnownEventType reports whether a GCS notification event type is in the
 * provider's documented set (OBJECT_INITIALIZE is zonal-bucket-only). */
export function isKnownEventType(eventType: string): boolean {
  return (NOTIFICATION_EVENT_TYPES as readonly string[]).includes(eventType)
}

/** validateEventTypes returns an error naming the first invalid event type, or
 * null when every entry is accepted by the API. */
export function validateEventTypes(eventTypes: string[]): string | null {
  const invalid = eventTypes.find((et) => !isKnownEventType(et))
  return invalid ? `Unknown notification event type: ${invalid}` : null
}
