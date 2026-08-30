import { Routes, Route, Navigate } from 'react-router-dom'
import { CloudWatchMetrics } from './CloudWatchMetrics'
import { CloudWatchAlarms } from './CloudWatchAlarms'
import { CloudWatchDashboards } from './CloudWatchDashboards'

export function CloudWatchRoutes() {
  return (
    <Routes>
      <Route index element={<Navigate to="metrics" replace />} />
      <Route path="metrics" element={<CloudWatchMetrics />} />
      <Route path="alarms" element={<CloudWatchAlarms />} />
      <Route path="dashboards" element={<CloudWatchDashboards />} />
    </Routes>
  )
}
