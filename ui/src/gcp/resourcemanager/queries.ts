import type { QueryClient } from '@tanstack/react-query'

/**
 * Invalidate the project list and the project picker's accounts list after a
 * project mutation. The picker's list is the Resource Manager registry, so a
 * create/delete/undelete must refresh both.
 */
export function invalidateProjects(queryClient: QueryClient): void {
  void queryClient.invalidateQueries({ queryKey: ['gcp', 'resourcemanager'] })
  void queryClient.invalidateQueries({ queryKey: ['meta', 'accounts'] })
}
