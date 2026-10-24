import { describe, expect, it } from 'vitest'
import {
  aceEditorOptions,
  aceModeFor,
  aceThemeFor,
  DEFAULT_MAX_LINES,
  DEFAULT_MIN_LINES,
  jsonParseAnnotation,
  lintJson,
} from './codeEditor'

describe('aceModeFor', () => {
  it('maps each console language to its ace mode', () => {
    expect(aceModeFor('json')).toBe('ace/mode/json')
    expect(aceModeFor('yaml')).toBe('ace/mode/yaml')
    expect(aceModeFor('text')).toBe('ace/mode/text')
  })
})

describe('aceThemeFor', () => {
  it('picks a light theme for light and a dark theme for dark', () => {
    expect(aceThemeFor('light')).toBe('ace/theme/dawn')
    expect(aceThemeFor('dark')).toBe('ace/theme/tomorrow_night')
  })
})

describe('aceEditorOptions', () => {
  it('enables folding and soft wrap and leaves the worker off (lintJson handles lint)', () => {
    const opts = aceEditorOptions({ language: 'json', theme: 'light', readOnly: false })
    expect(opts.mode).toBe('ace/mode/json')
    expect(opts.theme).toBe('ace/theme/dawn')
    expect(opts.showFoldWidgets).toBe(true)
    expect(opts.wrap).toBe(true)
    expect(opts.useWorker).toBe(false)
    expect(opts.minLines).toBe(DEFAULT_MIN_LINES)
    expect(opts.maxLines).toBe(DEFAULT_MAX_LINES)
  })

  it('turns the cursor line off when read-only', () => {
    const opts = aceEditorOptions({ language: 'json', theme: 'dark', readOnly: true })
    expect(opts.readOnly).toBe(true)
    expect(opts.highlightActiveLine).toBe(false)
    expect(opts.highlightGutterLine).toBe(false)
    expect(opts.theme).toBe('ace/theme/tomorrow_night')
  })

  it('clamps maxLines to at least minLines and floors minLines at one', () => {
    const opts = aceEditorOptions({
      language: 'yaml',
      theme: 'light',
      readOnly: false,
      minLines: 16,
      maxLines: 4,
    })
    expect(opts.minLines).toBe(16)
    expect(opts.maxLines).toBe(16)

    const tiny = aceEditorOptions({
      language: 'text',
      theme: 'light',
      readOnly: false,
      minLines: 0,
    })
    expect(tiny.minLines).toBe(1)
  })
})

describe('lintJson', () => {
  it('returns no annotations for valid, empty or whitespace-only input', () => {
    expect(lintJson('')).toEqual([])
    expect(lintJson('   \n  ')).toEqual([])
    expect(lintJson('{"a": 1}')).toEqual([])
    expect(lintJson('[1, 2]')).toEqual([])
    expect(lintJson('null')).toEqual([])
  })

  it('reports one error annotation inside the document for malformed input', () => {
    const value = '{\n  "a": 1,\n}'
    expect(lintJson(value)).toHaveLength(1)
    const annotation = lintJson(value)[0]!
    expect(annotation.type).toBe('error')
    expect(annotation.row).toBeGreaterThanOrEqual(0)
    expect(annotation.row).toBeLessThan(value.split('\n').length)
    expect(annotation.text).toMatch(/\(line \d+, column \d+\)$/)
  })

  it('points at the end of the document for a truncated value', () => {
    expect(lintJson('{"a":')).toEqual([
      {
        row: 0,
        column: 5,
        type: 'error',
        text: 'Unexpected end of JSON input (line 1, column 6)',
      },
    ])
  })
})

describe('jsonParseAnnotation', () => {
  it('uses the line/column V8 ≥ 120 reports', () => {
    // Chrome/Node ≥ 120 and Firefox both spell the location out.
    expect(
      jsonParseAnnotation(
        '{"a":1,}',
        'Expected double-quoted property name in JSON at position 7 (line 1 column 8)',
      ),
    ).toEqual({
      row: 0,
      column: 7,
      type: 'error',
      text: 'Expected double-quoted property name (line 1, column 8)',
    })
  })

  it('parses Firefox wording', () => {
    expect(
      jsonParseAnnotation(
        '{"a":1,}',
        'JSON.parse: expected double-quoted property name at line 1 column 8 of the JSON data',
      ),
    ).toEqual({
      row: 0,
      column: 7,
      type: 'error',
      text: 'expected double-quoted property name (line 1, column 8)',
    })
  })

  it('derives row/column from a bare character offset (older V8)', () => {
    expect(
      jsonParseAnnotation('{\n  "a": 1,\n}', 'Unexpected token } in JSON at position 12'),
    ).toEqual({
      row: 2,
      column: 0,
      type: 'error',
      text: 'Unexpected token } (line 3, column 1)',
    })
  })

  it('clamps an out-of-range offset instead of throwing', () => {
    expect(
      jsonParseAnnotation('{"a":1}', 'Unexpected token in JSON at position 99'),
    ).toMatchObject({ row: 0, column: 7 })
  })

  it('keeps the raw message with no location when the engine gives none (WebKit)', () => {
    expect(jsonParseAnnotation('{"a":1,}', "JSON Parse error: Expected '}'")).toEqual({
      row: 0,
      column: 0,
      type: 'error',
      text: "Expected '}'",
    })
  })

  it('falls back to a generic message when stripping leaves nothing', () => {
    // Only engine noise survives: the fallback text is used, with no location.
    expect(jsonParseAnnotation('x', ' of the JSON data')).toEqual({
      row: 0,
      column: 0,
      type: 'error',
      text: 'Invalid JSON',
    })
  })
})
