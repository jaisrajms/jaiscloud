import { Routes, Route, Navigate } from 'react-router-dom'
import { EMRList } from './EMRList'
import { EMRDetail } from './EMRDetail'

export function EMRRoutes() {
  return (
    <Routes>
      <Route index element={<Navigate to="clusters" replace />} />
      <Route path="clusters" element={<EMRList />} />
      <Route path=":id" element={<EMRDetail />} />
    </Routes>
  )
}
