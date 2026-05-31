import { Routes, Route, Navigate } from 'react-router-dom'
import { EMRContainersList } from './EMRContainersList'
import { EMRContainersDetail } from './EMRContainersDetail'

export function EMRContainersRoutes() {
  return (
    <Routes>
      <Route index element={<Navigate to="clusters" replace />} />
      <Route path="clusters" element={<EMRContainersList />} />
      <Route path=":id" element={<EMRContainersDetail />} />
    </Routes>
  )
}
