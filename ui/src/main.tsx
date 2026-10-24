import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import '@cloudscape-design/global-styles/index.css'
import { applyMode, Mode, setThemeClass, Theme } from '@cloudscape-design/global-styles'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter } from 'react-router-dom'
import App from './App'
import { api } from './api/client'
import type { Meta } from './hooks/useMeta'
import { applyAwsConsoleTheme } from './theme/aws/theme'
import './theme/aws/console.css'

// Cloudscape is the design system for the AWS/Azure consoles; the GCP console
// renders its own Material theme (see src/gcp). Apply the AWS palette at boot.
setThemeClass(Theme.OneTheme)
applyMode(localStorage.getItem('jaiscloud-mode') === 'dark' ? Mode.Dark : Mode.Light)
applyAwsConsoleTheme()

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 10_000,
      retry: 1,
    },
  },
})

const root = document.getElementById('root')
if (!root) throw new Error('Root element not found')
const container = createRoot(root)

/**
 * Resolve the cloud identity before the first paint, then seed the query cache
 * so App renders against the right shell without refetching meta. A failed
 * fetch falls through to the connection error App already renders.
 */
async function bootstrap() {
  try {
    const meta = await api.get<Meta>('/api/ui/v1/meta')
    queryClient.setQueryData(['meta'], meta)
  } catch {
    /* App renders the connection error */
  }

  container.render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter basename="/ui">
          <App />
        </BrowserRouter>
      </QueryClientProvider>
    </StrictMode>,
  )
}

void bootstrap()
