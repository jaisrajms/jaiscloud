import { describe, expect, it } from 'vitest'
import { resolveThemeMode } from './appearance'

describe('resolveThemeMode', () => {
  it('follows the system preference when set to system', () => {
    expect(resolveThemeMode('system', true)).toBe('dark')
    expect(resolveThemeMode('system', false)).toBe('light')
  })

  it('pins the theme when the user overrides the system', () => {
    expect(resolveThemeMode('dark', false)).toBe('dark')
    expect(resolveThemeMode('light', true)).toBe('light')
  })
})
