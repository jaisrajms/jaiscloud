import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  base: '/ui/',
  build: {
    // MUST output to internal/aws/ui/dist/ — go:embed in assets_ui.go looks there.
    // go:embed cannot use '..' paths, so Vite must write INTO the Go package directory.
    outDir: '../internal/aws/ui/dist',
    emptyOutDir: true,
    reportCompressedSize: true,
  },
  server: {
    port: 5173,
    proxy: {
      '/api/ui': { target: 'http://localhost:4567', changeOrigin: true },
      '/ui': { target: 'http://localhost:4567', changeOrigin: true },
    },
  },
})
