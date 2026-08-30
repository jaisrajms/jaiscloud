import { Routes, Route } from 'react-router-dom'
import { IAMList } from './IAMList'

export function IAMRoutes() {
  return (
    <Routes>
      <Route index element={<IAMList />} />
    </Routes>
  )
}
