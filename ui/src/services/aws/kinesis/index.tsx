import { Routes, Route, Navigate } from 'react-router-dom'
import { KinesisStreams } from './KinesisStreams'

export function KinesisRoutes() {
  return (
    <Routes>
      <Route index element={<Navigate to="streams" replace />} />
      <Route path="streams" element={<KinesisStreams />} />
      <Route path="*" element={<Navigate to="streams" replace />} />
    </Routes>
  )
}
