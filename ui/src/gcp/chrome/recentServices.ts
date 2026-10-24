import { useSyncExternalStore } from 'react'
import { RECENT_SERVICES_KEY, pushRecent, readRecent, type StorageLike } from './navModel'

function defaultStorage(): StorageLike | undefined {
  if (typeof window === 'undefined') return undefined
  try {
    return window.localStorage
  } catch {
    return undefined
  }
}

const storage = defaultStorage()
let cache = readRecent(RECENT_SERVICES_KEY, storage)
const listeners = new Set<() => void>()

function emit(): void {
  listeners.forEach((listener) => listener())
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener)
  return () => {
    listeners.delete(listener)
  }
}

function getSnapshot(): string[] {
  return cache
}

if (typeof window !== 'undefined') {
  window.addEventListener('storage', (event) => {
    if (event.key === RECENT_SERVICES_KEY) {
      cache = readRecent(RECENT_SERVICES_KEY, storage)
      emit()
    }
  })
}

/** Record a service visit so every nav surface shows the same recent list. */
export function rememberRecentService(id: string): void {
  if (!id) return
  cache = pushRecent(RECENT_SERVICES_KEY, id, storage)
  emit()
}

/** Reactive recent-service ids, shared by the global search and nav menu. */
export function useRecentServices(): string[] {
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
}
