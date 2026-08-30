import { Routes, Route, Navigate } from 'react-router-dom'
import { SESIdentities } from './SESIdentities'

export function SESRoutes() {
  return (
    <Routes>
      <Route index element={<Navigate to="identities" replace />} />
      <Route path="identities" element={<SESIdentities />} />
      <Route path="*" element={<Navigate to="identities" replace />} />
    </Routes>
  )
}
