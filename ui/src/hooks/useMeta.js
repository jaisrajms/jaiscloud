import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';
export function useMeta() {
    return useQuery({
        queryKey: ['meta'],
        queryFn: () => api.get('/api/ui/v1/meta'),
        staleTime: 60_000,
        retry: 3,
    });
}
