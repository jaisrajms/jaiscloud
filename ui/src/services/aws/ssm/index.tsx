import { Routes, Route } from 'react-router-dom'
import { SSMList } from './SSMList'

export function SSMRoutes() {
  return (
    <Routes>
      <Route index element={<SSMList />} />
    </Routes>
  )
}
