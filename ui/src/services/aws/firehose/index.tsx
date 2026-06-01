import { Routes, Route, Navigate } from 'react-router-dom'
import { FirehoseStreams } from './FirehoseStreams'

export function FirehoseRoutes() {
  return (
    <Routes>
      <Route index element={<Navigate to="streams" replace />} />
      <Route path="streams" element={<FirehoseStreams />} />
      <Route path="*" element={<Navigate to="streams" replace />} />
    </Routes>
  )
}
