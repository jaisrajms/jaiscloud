import { Routes, Route, Navigate } from 'react-router-dom'
import { SFNStateMachines } from './SFNStateMachines'
import { SFNExecutions } from './SFNExecutions'

export function SFNRoutes() {
  return (
    <Routes>
      <Route index element={<Navigate to="state-machines" replace />} />
      <Route path="state-machines" element={<SFNStateMachines />} />
      <Route path="executions" element={<SFNExecutions />} />
    </Routes>
  )
}
