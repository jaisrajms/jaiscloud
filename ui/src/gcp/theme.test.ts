import { describe, expect, it } from 'vitest'
import { createGcpTheme } from './theme'

function rootStyle(theme: ReturnType<typeof createGcpTheme>, slot: string): Record<string, unknown> {
  const components = theme.components as unknown as Record<
    string,
    { styleOverrides?: { root?: unknown } }
  >
  const root = components[slot]?.styleOverrides?.root
  if (typeof root === 'function') {
    return (root as (props: { theme: typeof theme }) => Record<string, unknown>)({ theme })
  }
  return (root as Record<string, unknown>) ?? {}
}

describe('createGcpTheme', () => {
  it('defaults to the light Google palette', () => {
    const theme = createGcpTheme()
    expect(theme.palette.mode).toBe('light')
    expect(theme.palette.primary.main).toBe('#1a73e8')
    expect(theme.palette.background.default).toBe('#f8f9fa')
    expect(theme.palette.background.paper).toBe('#ffffff')
  })

  it('exposes a dark palette with soft-blue primary and dark surfaces', () => {
    const theme = createGcpTheme('dark')
    expect(theme.palette.mode).toBe('dark')
    expect(theme.palette.primary.main).toBe('#8ab4f8')
    expect(theme.palette.background.default).toBe('#202124')
    expect(theme.palette.background.paper).toBe('#292a2d')
    expect(theme.palette.text.primary).toBe('#e8eaed')
    expect(theme.palette.divider).toBe('#3c4043')
  })

  it('uses dense 13px tables and compact tab bars', () => {
    const theme = createGcpTheme('light')
    expect(rootStyle(theme, 'MuiTableCell').fontSize).toBe(13)
    expect(rootStyle(theme, 'MuiTableCell').padding).toBe('6px 16px')
    expect(rootStyle(theme, 'MuiTab').minHeight).toBe(40)
    expect(rootStyle(theme, 'MuiTab').fontSize).toBe(13)
  })

  it('tightens table padding in compact density', () => {
    expect(rootStyle(createGcpTheme('light', 'compact'), 'MuiTableCell').padding).toBe('4px 12px')
    expect(rootStyle(createGcpTheme('light', 'comfortable'), 'MuiTableCell').padding).toBe(
      '6px 16px',
    )
  })

  it('paints the app bar from the palette rather than a fixed light color', () => {
    const light = createGcpTheme('light')
    const dark = createGcpTheme('dark')
    expect(rootStyle(light, 'MuiAppBar').backgroundColor).toBe('#ffffff')
    expect(rootStyle(dark, 'MuiAppBar').backgroundColor).toBe('#292a2d')
  })
})
