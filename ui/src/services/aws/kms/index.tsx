import { Routes, Route } from 'react-router-dom'
import { KMSList } from './KMSList'

export function KMSRoutes() {
  return (
    <Routes>
      <Route index element={<KMSList />} />
    </Routes>
  )
}
