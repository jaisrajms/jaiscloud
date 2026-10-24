import { GcpCodeEditor } from '../common/GcpCodeEditor'

export interface JsonEditorProps {
  label: string
  value: string
  onChange: (value: string) => void
  error?: string | null
  minRows?: number
  helperText?: string
  disabled?: boolean
}

/**
 * Firestore's typed-encoding JSON editor. Backed by the shared GCP code editor
 * (UI63) in JSON mode; the props are unchanged so DocumentDetailPage and
 * CreateDocumentDialog keep working as-is.
 */
export function JsonEditor({
  label,
  value,
  onChange,
  error,
  minRows = 12,
  helperText,
  disabled,
}: JsonEditorProps) {
  return (
    <GcpCodeEditor
      label={label}
      value={value}
      onChange={onChange}
      error={error}
      helperText={helperText}
      disabled={disabled}
      language="json"
      minRows={minRows}
    />
  )
}
