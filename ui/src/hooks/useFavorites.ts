import { useSyncExternalStore } from 'react'

const STORAGE_KEY = 'jaiscloud-favorites'

function readStorage(): string[] {
  try {
    const raw: unknown = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? '[]')
    return Array.isArray(raw) ? raw.filter((id): id is string => typeof id === 'string') : []
  } catch {
    return []
  }
}

let cache = readStorage()
const listeners = new Set<() => void>()

function commit(next: string[]): void {
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

function getSnapshot(): string[] {
  return cache
}

/**
 * Favourite service ids, shared across every mounted component (sidebar +
 * Console Home) and persisted in localStorage.
 */
export function useFavorites() {
  const favorites = useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
  return {
    favorites,
    isFavorite: (id: string) => favorites.includes(id),
    toggle: (id: string) => {
      commit(favorites.includes(id) ? favorites.filter((item) => item !== id) : [...favorites, id])
    },
  }
}
