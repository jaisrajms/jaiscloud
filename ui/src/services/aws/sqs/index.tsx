import { Routes, Route } from 'react-router-dom'
import { SQSList } from './SQSList'
import { SQSDetail } from './SQSDetail'

export function SQSRoutes() {
  return (
    <Routes>
      <Route index element={<SQSList />} />
      <Route path=":queueUrl" element={<SQSDetail />} />
    </Routes>
  )
}
