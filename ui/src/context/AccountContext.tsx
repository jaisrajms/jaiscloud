import { createContext, useContext, useState, useEffect } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { setCurrentAccount } from '../api/client'
import { useMeta } from '../hooks/useMeta'
import { persistSelectedAccount, readSelectedAccount } from './accountStorage'

interface AccountContextType {
  accountId: string
  setAccountId: (id: string) => void
}

const AccountContext = createContext<AccountContextType>({
  accountId: '',
  setAccountId: () => {},
})

function fetchAccounts(): Promise<{ accounts: string[] }> {
  return fetch('/api/ui/v1/meta/accounts').then((r) => r.json())
}

export function useAccounts() {
  return useQuery({
    queryKey: ['meta', 'accounts'],
    queryFn: fetchAccounts,
    staleTime: 60_000,
  })
}

/**
 * The persisted choice seeds the request layer at module load, before the tree
 * renders: `request()` reads module state to build `?account=`, and the
 * provider's effect runs only after the first child render has already issued
 * its queries. Without this, a refresh fetches the default project's data
 * before the selected project is applied.
 */
const initialAccount = readSelectedAccount()
setCurrentAccount(initialAccount)

export function AccountProvider({ children }: { children: React.ReactNode }) {
  const { data: meta } = useMeta()
  const [accountId, setAccountIdState] = useState(initialAccount)
  const queryClient = useQueryClient()

  useEffect(() => {
    if (meta?.accountId && !accountId) {
      setAccountIdState(meta.accountId)
      setCurrentAccount(meta.accountId)
    }
  }, [meta?.accountId, accountId])

  function setAccountId(id: string) {
    setAccountIdState(id)
    setCurrentAccount(id)
    persistSelectedAccount(id)
    // Invalidate all cached data so every component refetches under the new account.
    queryClient.invalidateQueries()
  }

  return (
    <AccountContext.Provider value={{ accountId, setAccountId }}>
      {children}
    </AccountContext.Provider>
  )
}

export function useAccount() {
  return useContext(AccountContext)
}
