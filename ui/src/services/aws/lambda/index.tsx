import { Routes, Route } from 'react-router-dom'
import { LambdaList } from './LambdaList'
import { LambdaDetail } from './LambdaDetail'

export function LambdaRoutes() {
  return (
    <Routes>
      <Route index element={<LambdaList />} />
      <Route path=":name" element={<LambdaDetail />} />
    </Routes>
  )
}
