import { useCallback, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { createProject } from '../../api/gcp/resourcemanager'

/**
 * Register an observed project through the existing Resource Manager create
 * endpoint. Invalid-grammar ids fail inline and stay observed; a success
 * refetches both the discovery view and the console's account list.
 */
export function useRegisterProject() {
  const queryClient = useQueryClient()
  const [registering, setRegistering] = useState<string | null>(null)
  const [errors, setErrors] = useState<Record<string, string>>({})

  const register = useCallback(
    async (id: string) => {
      setRegistering(id)
      setErrors((prev) => ({ ...prev, [id]: '' }))
      try {
        await createProject({ projectId: id })
        await queryClient.invalidateQueries({ queryKey: ['gcp', 'project-discovery'] })
        await queryClient.invalidateQueries({ queryKey: ['meta', 'accounts'] })
      } catch (error) {
        setErrors((prev) => ({
          ...prev,
          [id]: error instanceof Error ? error.message : String(error),
        }))
      } finally {
        setRegistering(null)
      }
    },
    [queryClient],
  )

  return { register, registering, errors }
}
