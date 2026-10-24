import { createTheme, type Theme } from '@mui/material/styles'

/** Light/dark appearance for the GCP console. */
export type GcpThemeMode = 'light' | 'dark'

/** Table/row packing; `compact` mirrors the densest real-console tables. */
export type GcpDensity = 'comfortable' | 'compact'

const ROBOTO = '"Roboto", "Helvetica", "Arial", sans-serif'

/**
 * Material theme for the GCP console, approximating the real Google Cloud
 * Console: Google Blue primary, neutral grays, Roboto and a denser layout than
 * Material's defaults. Only the GCP shell uses this; the AWS/Azure consoles
 * stay on Cloudscape.
 *
 * The dark palette follows the real console's dark tokens (surface #202124 /
 * #292a2d, soft-blue primary) rather than inverting the light one.
 */
export function createGcpTheme(
  mode: GcpThemeMode = 'light',
  density: GcpDensity = 'comfortable',
): Theme {
  const dark = mode === 'dark'
  const cellPadding = density === 'compact' ? '4px 12px' : '6px 16px'
  const rowHeight = density === 'compact' ? 36 : 44

  return createTheme({
    palette: {
      mode,
      ...(dark
        ? {
            primary: { main: '#8ab4f8' },
            secondary: { main: '#9aa0a6' },
            background: { default: '#202124', paper: '#292a2d' },
            text: { primary: '#e8eaed', secondary: '#9aa0a6' },
            divider: '#3c4043',
            action: {
              hover: 'rgba(232, 234, 237, 0.08)',
              selected: 'rgba(138, 180, 248, 0.16)',
              focus: 'rgba(138, 180, 248, 0.24)',
            },
          }
        : {
            primary: { main: '#1a73e8' },
            secondary: { main: '#5f6368' },
            background: { default: '#f8f9fa', paper: '#ffffff' },
            text: { primary: '#202124', secondary: '#5f6368' },
            divider: '#dadce0',
            action: {
              hover: 'rgba(60, 64, 67, 0.06)',
              selected: 'rgba(26, 115, 232, 0.08)',
              focus: 'rgba(26, 115, 232, 0.16)',
            },
          }),
    },
    typography: {
      fontFamily: ROBOTO,
      button: { textTransform: 'none', fontWeight: 500 },
      body2: { fontSize: 13 },
      caption: { fontSize: 12 },
      overline: { fontSize: 11, fontWeight: 500, letterSpacing: '0.8px' },
    },
    shape: { borderRadius: 8 },
    components: {
      MuiCssBaseline: {
        styleOverrides: {
          body: { backgroundColor: dark ? '#202124' : '#f8f9fa' },
        },
      },
      MuiButtonBase: {
        styleOverrides: {
          root: ({ theme }) => ({
            '&.Mui-focusVisible': {
              outline: `2px solid ${theme.palette.primary.main}`,
              outlineOffset: 2,
            },
          }),
        },
      },
      MuiAppBar: {
        defaultProps: { elevation: 0, color: 'inherit' },
        styleOverrides: {
          root: ({ theme }) => ({
            backgroundColor: theme.palette.background.paper,
            backgroundImage: 'none',
            borderBottom: `1px solid ${theme.palette.divider}`,
          }),
        },
      },
      MuiToolbar: {
        styleOverrides: {
          root: { minHeight: 48 },
          dense: { minHeight: 40 },
        },
      },
      MuiButton: { styleOverrides: { root: { textTransform: 'none' } } },
      MuiListItemButton: {
        styleOverrides: {
          root: { borderRadius: 8, margin: '0 8px', minHeight: density === 'compact' ? 32 : 36 },
        },
      },
      MuiTable: { defaultProps: { size: 'small' } },
      MuiTableCell: {
        styleOverrides: {
          root: { padding: cellPadding, fontSize: 13 },
          head: { fontWeight: 500, color: 'inherit' },
        },
      },
      MuiTableRow: { styleOverrides: { root: { height: rowHeight } } },
      MuiTabs: { styleOverrides: { root: { minHeight: 40 } } },
      MuiTab: {
        styleOverrides: {
          root: { minHeight: 40, fontSize: 13, textTransform: 'none', fontWeight: 500 },
        },
      },
      MuiChip: { styleOverrides: { root: { fontSize: 12 } } },
      MuiPaper: { styleOverrides: { root: { backgroundImage: 'none' } } },
      MuiCard: {
        defaultProps: { elevation: 0 },
        styleOverrides: { root: ({ theme }) => ({ border: `1px solid ${theme.palette.divider}` }) },
      },
      MuiDivider: { styleOverrides: { root: { borderColor: dark ? '#3c4043' : '#dadce0' } } },
    },
  })
}

/** Light console theme (default), retained for direct imports. */
export const gcpTheme = createGcpTheme('light')
