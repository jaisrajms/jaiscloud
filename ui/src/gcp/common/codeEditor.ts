/** Shared code-editor configuration for the GCP console.
 *
 * The console's JSON/structured-text inputs used to be plain monospace MUI
 * TextFields. This module holds the pure mapping from a console language and
 * MUI palette mode to the ace mode/theme and editor options, so the lazy ace
 * component stays a thin mount and the mapping is unit-testable.
 */

/** Languages the shared editor understands. */
export type GcpCodeLanguage = 'json' | 'yaml' | 'text'

/** Effective light/dark appearance, matching `GcpThemeMode`. */
export type GcpCodeThemeMode = 'light' | 'dark'

/** ace mode path for a console language. */
export function aceModeFor(language: GcpCodeLanguage): string {
  switch (language) {
    case 'yaml':
      return 'ace/mode/yaml'
    case 'text':
      return 'ace/mode/text'
    default:
      return 'ace/mode/json'
  }
}

/** ace theme name matched to the MUI palette mode. Mirrors the AWS editor's
 * dawn/tomorrow_night pair so the two consoles stay visually consistent. */
export function aceThemeFor(mode: GcpCodeThemeMode): string {
  return mode === 'dark' ? 'ace/theme/tomorrow_night' : 'ace/theme/dawn'
}

export interface AceEditorOptions {
  mode: string
  theme: string
  readOnly: boolean
  wrap: boolean
  showFoldWidgets: boolean
  displayIndentGuides: boolean
  showPrintMargin: boolean
  showGutter: boolean
  highlightActiveLine: boolean
  highlightGutterLine: boolean
  tabSize: number
  fontSize: number
  minLines: number
  maxLines: number
  /** ace's JSON linter runs in a web worker that needs a bundler-registered
   * module URL (`ace.config.setModuleUrl`) and a separate ~150 KB asset.
   * Disabled; inline lint comes from `lintJson` instead. */
  useWorker: boolean
}

export interface AceEditorOptionInput {
  language: GcpCodeLanguage
  theme: GcpCodeThemeMode
  readOnly: boolean
  minLines?: number
  maxLines?: number
}

export const DEFAULT_MIN_LINES = 8
export const DEFAULT_MAX_LINES = 40

/** ace editor options shared by every mount: folding, bracket matching (on by
 * default), soft wrap and gutter styling. */
export function aceEditorOptions({
  language,
  theme,
  readOnly,
  minLines = DEFAULT_MIN_LINES,
  maxLines = DEFAULT_MAX_LINES,
}: AceEditorOptionInput): AceEditorOptions {
  const min = Math.max(1, minLines)
  return {
    mode: aceModeFor(language),
    theme: aceThemeFor(theme),
    readOnly,
    wrap: true,
    showFoldWidgets: true,
    displayIndentGuides: true,
    showPrintMargin: false,
    showGutter: true,
    highlightActiveLine: !readOnly,
    highlightGutterLine: !readOnly,
    tabSize: 2,
    fontSize: 13,
    minLines: min,
    maxLines: Math.max(min, maxLines),
    useWorker: false,
  }
}

/** A single ace annotation, in ace's 0-based row/column coordinates. */
export interface JsonLintAnnotation {
  row: number
  column: number
  /** Human-readable message, including the location when it is known. */
  text: string
  type: 'error'
}

/** Debounce for the inline lint so typing is not interrupted per keystroke. */
export const LINT_DEBOUNCE_MS = 250

/** V8 (Chrome/Node ≥ 120) and Firefox both spell the location out. */
const LINE_COLUMN_RE = /line (\d+) column (\d+)/i
/** Older V8 only reports a character offset. */
const POSITION_RE = /position (\d+)/i
/** V8 reports a truncated document without a location. */
const END_OF_INPUT_RE = /unexpected end of (?:json )?input/i

interface ErrorLocation {
  row: number
  column: number
  /** False when the engine message carried no usable location. */
  known: boolean
}

/** Map a character offset to ace's 0-based row/column. */
function offsetToLocation(value: string, offset: number): ErrorLocation {
  const clamped = Math.min(Math.max(0, offset), value.length)
  const before = value.slice(0, clamped)
  const lastNewline = before.lastIndexOf('\n')
  return {
    row: before.split('\n').length - 1,
    column: clamped - (lastNewline + 1),
    known: true,
  }
}

/** Best-effort location from a `JSON.parse` error message. Engines disagree on
 * the wording (`line L column C`, `position N`, or nothing at all), so parse in
 * order of specificity and fall back to the first character. */
function errorLocation(value: string, message: string): ErrorLocation {
  const lineColumn = LINE_COLUMN_RE.exec(message)
  if (lineColumn) {
    return {
      row: Math.max(0, Number(lineColumn[1]) - 1),
      column: Math.max(0, Number(lineColumn[2]) - 1),
      known: true,
    }
  }
  const position = POSITION_RE.exec(message)
  if (position) return offsetToLocation(value, Number(position[1]))
  if (END_OF_INPUT_RE.test(message)) return offsetToLocation(value, value.length)
  return { row: 0, column: 0, known: false }
}

/** Strip the engine-specific noise and location phrasing from a parse error. */
function describeJsonError(message: string): string {
  return message
    .replace(/^JSON\.parse:\s*/i, '')
    .replace(/^JSON Parse error:\s*/i, '')
    .replace(/\s+in JSON at position \d+/i, '')
    .replace(/\s+at position \d+/i, '')
    .replace(/\s*\(line \d+ column \d+\)/i, '')
    .replace(/\s+at line \d+ column \d+ of the JSON data/i, '')
    .replace(/\s+of the JSON data/i, '')
    .trim()
}

/**
 * Inline JSON lint for the console's code editor: returns ace annotations for
 * malformed input, or an empty list when the document parses.
 *
 * This replaces ace's `worker-json` linter, which needs a bundler-registered
 * module URL and a separate worker asset; `JSON.parse` on a short debounce gives
 * the same as-you-type feedback without either. `JSON.parse` stops at the first
 * error, so at most one annotation is returned.
 */
export function lintJson(value: string): JsonLintAnnotation[] {
  if (!value.trim()) return []
  try {
    JSON.parse(value)
    return []
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    return [jsonParseAnnotation(value, message)]
  }
}

/**
 * Build the annotation for a known parse failure. Split out from `lintJson` so
 * the per-engine message wordings can be tested without depending on which
 * engine the test runner happens to use.
 */
export function jsonParseAnnotation(value: string, message: string): JsonLintAnnotation {
  const { row, column, known } = errorLocation(value, message)
  const detail = describeJsonError(message) || 'Invalid JSON'
  return {
    row,
    column,
    type: 'error',
    text: known ? `${detail} (line ${row + 1}, column ${column + 1})` : detail,
  }
}
