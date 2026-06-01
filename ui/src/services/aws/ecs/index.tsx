import { Routes, Route } from 'react-router-dom'
import { ECSClusters } from './ECSClusters'

export function ECSRoutes() {
  return (
    <Routes>
      <Route index element={<ECSClusters />} />
      <Route path="clusters" element={<ECSClusters />} />
    </Routes>
  )
}
