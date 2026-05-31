import { jsx as _jsx } from "react/jsx-runtime";
import { createContext, useContext, useState, useEffect } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { setCurrentAccount } from '../api/client';
import { useMeta } from '../hooks/useMeta';
const AccountContext = createContext({
    accountId: '',
    setAccountId: () => { },
});
function fetchAccounts() {
    return fetch('/api/ui/v1/meta/accounts').then((r) => r.json());
}
export function useAccounts() {
    return useQuery({
        queryKey: ['meta', 'accounts'],
        queryFn: fetchAccounts,
        staleTime: 60_000,
    });
}
export function AccountProvider({ children }) {
    const { data: meta } = useMeta();
    const [accountId, setAccountIdState] = useState('');
    const queryClient = useQueryClient();
    useEffect(() => {
        if (meta?.accountId && !accountId) {
            setAccountIdState(meta.accountId);
            setCurrentAccount(meta.accountId);
        }
    }, [meta?.accountId, accountId]);
    function setAccountId(id) {
        setAccountIdState(id);
        setCurrentAccount(id);
        // Invalidate all cached data so every component refetches under the new account.
        queryClient.invalidateQueries();
    }
    return (_jsx(AccountContext.Provider, { value: { accountId, setAccountId }, children: children }));
}
export function useAccount() {
    return useContext(AccountContext);
}
