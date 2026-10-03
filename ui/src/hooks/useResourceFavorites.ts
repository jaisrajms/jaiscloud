import { useSyncExternalStore } from 'react'

const STORAGE_KEY = 'jaiscloud-resource-favorites'

export interface ResourceFavorite {
  /** Service descriptor id, e.g. "sqs". */
  service: string
  /** Stable per-service resource id (queue URL, ARN, name, …). */
  id: string
  /** Display name. */
  label: string
  /** Deep link (router path) to the resource. */
  href: string
  /** Resource kind, e.g. "queue", "bucket". */
  type?: string
}

function readStorage(): ResourceFavorite[] {
  try {
    const raw: unknown = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? '[]')
    if (!Array.isArray(raw)) return []
    return raw.filter(
      (item): item is ResourceFavorite =>
        typeof item === 'object' &&
        item !== null &&
        typeof (item as ResourceFavorite).service === 'string' &&
        typeof (item as ResourceFavorite).id === 'string' &&
        typeof (item as ResourceFavorite).label === 'string' &&
        typeof (item as ResourceFavorite).href === 'string',
    )
  } catch {
    return []
  }
}

const sameKey = (a: Pick<ResourceFavorite, 'service' | 'id'>, b: Pick<ResourceFavorite, 'service' | 'id'>) =>
  a.service === b.service && a.id === b.id

let cache = readStorage()
const listeners = new Set<() => void>()

function commit(next: ResourceFavorite[]): void {
  cache = next
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(next))
  } catch {
    /* ignore storage errors */
  }
  listeners.forEach((listener) => listener())
}

if (typeof window !== 'undefined') {
  window.addEventListener('storage', (event) => {
    if (event.key === STORAGE_KEY) {
      cache = readStorage()
      listeners.forEach((listener) => listener())
    }
  })
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener)
  return () => listeners.delete(listener)
}

function getSnapshot(): ResourceFavorite[] {
  return cache
}

/** Favourite resources, shared across components and persisted in localStorage. */
export function useResourceFavorites() {
  const favorites = useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
  return {
    favorites,
    isFavorite: (service: string, id: string) =>
      favorites.some((item) => item.service === service && item.id === id),
    toggle: (favorite: ResourceFavorite) => {
      const exists = favorites.some((item) => sameKey(item, favorite))
      commit(exists ? favorites.filter((item) => !sameKey(item, favorite)) : [...favorites, favorite])
    },
    remove: (service: string, id: string) => {
      commit(favorites.filter((item) => !(item.service === service && item.id === id)))
    },
  }
}
