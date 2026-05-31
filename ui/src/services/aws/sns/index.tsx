import { Routes, Route } from 'react-router-dom'
import { SNSList } from './SNSList'
import { SNSDetail } from './SNSDetail'

export function SNSRoutes() {
  return (
    <Routes>
      <Route index element={<SNSList />} />
      <Route path=":topicArn" element={<SNSDetail />} />
    </Routes>
  )
}
