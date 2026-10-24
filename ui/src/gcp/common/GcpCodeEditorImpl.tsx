import { useEffect, useId, useRef, useState } from 'react'
import ace from 'ace-builds'
import 'ace-builds/src-noconflict/mode-json'
import 'ace-builds/src-noconflict/mode-yaml'
import 'ace-builds/src-noconflict/mode-text'
import 'ace-builds/src-noconflict/theme-dawn'
import 'ace-builds/src-noconflict/theme-tomorrow_night'
import { Box, FormControl, FormHelperText, FormLabel, useTheme } from '@mui/material'
import {
  aceEditorOptions,
  DEFAULT_MAX_LINES,
  DEFAULT_MIN_LINES,
  LINT_DEBOUNCE_MS,
  lintJson,
  type GcpCodeLanguage,
} from './codeEditor'

type AceEditor = ReturnType<typeof ace.edit>

export interface GcpCodeEditorImplProps {
  label?: string
  value: string
  onChange?: (value: string) => void
  error?: string | null
  helperText?: string
  language?: GcpCodeLanguage
  readOnly?: boolean
  disabled?: boolean
  /** Inline JSON lint (default: on for `language="json"`). */
  lint?: boolean
  /** Minimum visible lines; the editor grows with its content up to maxLines. */
  minRows?: number
  /** Focus the editor when it mounts, mirroring a TextField's autoFocus. */
  autoFocus?: boolean
  ariaLabel?: string
}

const MONO_FONT =
  "'Roboto Mono', ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"

/**
 * MUI-native code editor backed by ace. This is the lazily-loaded half (see
 * `GcpCodeEditor.tsx`); it imports ace and the noconflict modes/themes
 * directly, with no Cloudscape wrapper, so the GCP console stays MUI-only.
 */
export function GcpCodeEditorImpl({
  label,
  value,
  onChange,
  error,
  helperText,
  language = 'json',
  readOnly = false,
  disabled = false,
  lint = language === 'json',
  minRows = DEFAULT_MIN_LINES,
  autoFocus = false,
  ariaLabel,
}: GcpCodeEditorImplProps) {
  const theme = useTheme()
  const themeMode = theme.palette.mode === 'dark' ? 'dark' : 'light'
  const helperId = useId()
  const [lintError, setLintError] = useState<string | null>(null)

  const containerRef = useRef<HTMLDivElement | null>(null)
  const editorRef = useRef<AceEditor | null>(null)
  const onChangeRef = useRef(onChange)
  onChangeRef.current = onChange
  const autoFocusRef = useRef(autoFocus)
  autoFocusRef.current = autoFocus
  const suppressRef = useRef(false)

  // Mount ace once per container. React StrictMode double-invokes effects in
  // dev; `editor.destroy()` clears the container's ace env, so re-mounting is
  // safe.
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const editor = ace.edit(el)
    editorRef.current = editor
    editor.session.on('change', () => {
      if (suppressRef.current) return
      onChangeRef.current?.(editor.getValue())
    })
    if (autoFocusRef.current) editor.focus()
    return () => {
      editor.destroy()
      editorRef.current = null
    }
  }, [])

  // Language, theme and read-only/disabled options.
  useEffect(() => {
    const editor = editorRef.current
    if (!editor) return
    editor.setOptions(
      aceEditorOptions({
        language,
        theme: themeMode,
        readOnly: readOnly || disabled,
        minLines: minRows,
        maxLines: Math.max(DEFAULT_MAX_LINES, minRows),
      }),
    )
  }, [language, themeMode, readOnly, disabled, minRows])

  // Sync an externally-changed value without moving the cursor.
  useEffect(() => {
    const editor = editorRef.current
    if (!editor || editor.getValue() === value) return
    const cursor = editor.getCursorPosition()
    suppressRef.current = true
    editor.setValue(value, -1)
    editor.moveCursorToPosition(cursor)
    suppressRef.current = false
  }, [value])

  // Inline JSON lint (UI69). Debounced so typing stays smooth; the ace gutter
  // shows the marker and the first message is mirrored into the helper slot so
  // it is readable without hovering.
  useEffect(() => {
    const editor = editorRef.current
    if (!editor) return
    if (disabled || !lint || language !== 'json') {
      editor.session.clearAnnotations()
      setLintError(null)
      return
    }
    const timer = window.setTimeout(() => {
      const annotations = lintJson(value)
      editor.session.setAnnotations(annotations)
      setLintError(annotations[0]?.text ?? null)
    }, LINT_DEBOUNCE_MS)
    return () => window.clearTimeout(timer)
  }, [value, language, lint, disabled])

  const shownError = error || lintError || null
  const showHelper = Boolean(shownError || helperText)

  return (
    <FormControl fullWidth error={Boolean(shownError)} disabled={disabled}>
      {label ? (
        <FormLabel
          id={`${helperId}-label`}
          sx={{ fontSize: 12, fontWeight: 500, mb: 0.5, color: 'text.secondary' }}
        >
          {label}
        </FormLabel>
      ) : null}
      <Box
        sx={{
          border: '1px solid',
          borderColor: shownError ? 'error.main' : 'divider',
          borderRadius: 1,
          overflow: 'hidden',
          bgcolor: 'background.paper',
          ...(disabled ? { opacity: 0.6, pointerEvents: 'none' } : {}),
        }}
      >
        <Box
          ref={containerRef}
          aria-label={ariaLabel ?? label ?? 'Code editor'}
          aria-describedby={showHelper ? helperId : undefined}
          sx={{
            width: '100%',
            minHeight: minRows * 20,
            '& .ace_editor': {
              width: '100%',
              fontFamily: MONO_FONT,
              fontSize: 13,
              lineHeight: 1.5,
              backgroundColor: 'transparent',
              color: 'text.primary',
            },
            '& .ace_gutter': {
              backgroundColor: 'background.default',
              color: 'text.secondary',
            },
            '& .ace_gutter-active-line, & .ace_marker-layer .ace_active-line': {
              backgroundColor: 'action.hover',
            },
            '& .ace_marker-layer .ace_selection': { backgroundColor: 'action.selected' },
            '& .ace_fold-widget': { color: 'text.secondary' },
          }}
        />
      </Box>
      {showHelper ? (
        <FormHelperText id={helperId}>{shownError || helperText}</FormHelperText>
      ) : null}
    </FormControl>
  )
}
