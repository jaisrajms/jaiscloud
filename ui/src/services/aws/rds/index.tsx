import { Routes, Route } from 'react-router-dom'
import { RDSInstances } from './RDSInstances'

export function RDSRoutes() {
  return (
    <Routes>
      <Route index element={<RDSInstances />} />
      <Route path="instances" element={<RDSInstances />} />
    </Routes>
  )
}
