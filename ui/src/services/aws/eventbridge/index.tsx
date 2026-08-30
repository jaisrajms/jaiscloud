import { Routes, Route, Navigate } from 'react-router-dom'
import { EventBridgeRules } from './EventBridgeRules'
import { EventBridgeBuses } from './EventBridgeBuses'

export function EventBridgeRoutes() {
  return (
    <Routes>
      <Route index element={<Navigate to="rules" replace />} />
      <Route path="rules" element={<EventBridgeRules />} />
      <Route path="buses" element={<EventBridgeBuses />} />
    </Routes>
  )
}
