import { useQueryClient } from '@tanstack/react-query';
import { useCallback } from 'react';
/** Default poll interval when SSE is disconnected (ms). */
export const FALLBACK_POLL_MS = 5_000;
/** Enable SSE-driven updates for a query key by removing the refetch interval. */
export function useAutoRefresh() {
    const qc = useQueryClient();
    const enableSSE = useCallback((queryKey) => {
        qc.setQueryDefaults(queryKey, { refetchInterval: false });
    }, [qc]);
    const enablePoll = useCallback((queryKey) => {
        qc.setQueryDefaults(queryKey, { refetchInterval: FALLBACK_POLL_MS });
    }, [qc]);
    return { enableSSE, enablePoll };
}
