import { describe, expect, it } from 'vitest'
import {
  persistSelectedAccount,
  readSelectedAccount,
  SELECTED_ACCOUNT_KEY,
  type StorageLike,
} from './accountStorage'

function fakeStorage(seed: Record<string, string> = {}) {
  const data = new Map(Object.entries(seed))
  return {
    data,
    getItem: (key: string) => data.get(key) ?? null,
    setItem: (key: string, value: string) => {
      data.set(key, value)
    },
  } satisfies StorageLike & { data: Map<string, string> }
}

describe('selected account storage', () => {
  it('persists and reads back the selected account', () => {
    const storage = fakeStorage()
    expect(readSelectedAccount(storage)).toBe('')
    persistSelectedAccount('acme-e2e', storage)
    expect(storage.data.get(SELECTED_ACCOUNT_KEY)).toBe('acme-e2e')
    expect(readSelectedAccount(storage)).toBe('acme-e2e')
  })

  it('never writes an empty or whitespace-only id', () => {
    const storage = fakeStorage({ [SELECTED_ACCOUNT_KEY]: 'kept' })
    persistSelectedAccount('', storage)
    persistSelectedAccount('   ', storage)
    expect(readSelectedAccount(storage)).toBe('kept')
  })

  it('trims surrounding whitespace', () => {
    const storage = fakeStorage({ [SELECTED_ACCOUNT_KEY]: '  acme-e2e  ' })
    expect(readSelectedAccount(storage)).toBe('acme-e2e')
  })

  it('returns an empty selection from a missing or throwing store', () => {
    const throwing: StorageLike = {
      getItem: () => {
        throw new Error('denied')
      },
      setItem: () => {
        throw new Error('denied')
      },
    }
    expect(readSelectedAccount(fakeStorage())).toBe('')
    expect(readSelectedAccount(throwing)).toBe('')
    expect(() => persistSelectedAccount('acme-e2e', throwing)).not.toThrow()
  })
})
