import { Alert, Button } from '@cloudscape-design/components'

interface Props {
  header: string
  message?: string
  onRetry?: () => void
}

/**
 * Consistent page-level error state: a Cloudscape error Alert with an optional
 * retry action. Replaces the per-page `Alert type="error"` blocks.
 */
export function ErrorState({ header, message, onRetry }: Props) {
  return (
    <Alert
      type="error"
      header={header}
      action={onRetry ? <Button onClick={onRetry}>Retry</Button> : undefined}
    >
      {message}
    </Alert>
  )
}
