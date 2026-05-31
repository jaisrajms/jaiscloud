import { Routes, Route, Navigate } from 'react-router-dom'
import { GlueDatabases } from './GlueDatabases'
import { GlueJobs } from './GlueJobs'
import { GlueCrawlers } from './GlueCrawlers'

export function GlueRoutes() {
  return (
    <Routes>
      <Route index element={<Navigate to="databases" replace />} />
      <Route path="databases" element={<GlueDatabases />} />
      <Route path="jobs" element={<GlueJobs />} />
      <Route path="crawlers" element={<GlueCrawlers />} />
    </Routes>
  )
}
