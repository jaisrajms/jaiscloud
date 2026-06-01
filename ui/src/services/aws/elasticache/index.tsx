import { Routes, Route } from 'react-router-dom'
import { ElastiCacheClusters } from './ElastiCacheClusters'

export function ElastiCacheRoutes() {
  return (
    <Routes>
      <Route index element={<ElastiCacheClusters />} />
      <Route path="clusters" element={<ElastiCacheClusters />} />
    </Routes>
  )
}
