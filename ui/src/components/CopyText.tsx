import { Box, CopyToClipboard } from '@cloudscape-design/components'

interface Props {
  value?: string
  /** Human-readable noun for the copy button label, e.g. "queue URL". */
  label?: string
  ariaLabel?: string
}

/**
 * Monospace value with a copy-to-clipboard icon button. Renders an em dash when
 * the value is empty, so callers can use it directly in table/detail cells.
 */
export function CopyText({ value, label, ariaLabel }: Props) {
  if (!value) return <>—</>
  return (
    <span style={{ display: 'inline-flex', alignItems: 'center', gap: '0.25rem' }}>
      <Box variant="code" display="inline">
        {value}
      </Box>
      <CopyToClipboard
        variant="icon"
        textToCopy={value}
        copyButtonAriaLabel={ariaLabel ?? `Copy ${label ?? value}`}
        copySuccessText="Copied"
        copyErrorText="Copy failed"
      />
    </span>
  )
}
