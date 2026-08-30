import { Routes, Route, Navigate } from 'react-router-dom'
import { LogGroupList } from './LogGroupList'
import { LogGroupDetail } from './LogGroupDetail'
import { LogStreamView } from './LogStreamView'
import { LogInsights } from './LogInsights'

export function LogsRoutes() {
  return (
    <Routes>
      <Route index element={<Navigate to="groups" replace />} />
      <Route path="groups" element={<LogGroupList />} />
      <Route path="groups/:name" element={<LogGroupDetail />} />
      <Route path="groups/:name/streams/:stream" element={<LogStreamView />} />
      <Route path="insights" element={<LogInsights />} />
    </Routes>
  )
}
