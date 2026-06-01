import { Routes, Route } from 'react-router-dom'
import { EKSClusters } from './EKSClusters'

export function EKSRoutes() {
  return (
    <Routes>
      <Route index element={<EKSClusters />} />
      <Route path="clusters" element={<EKSClusters />} />
    </Routes>
  )
}
