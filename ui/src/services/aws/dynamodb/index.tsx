import { Routes, Route } from 'react-router-dom'
import { DynamoDBList } from './DynamoDBList'
import { DynamoDBDetail } from './DynamoDBDetail'

export function DynamoDBRoutes() {
  return (
    <Routes>
      <Route index element={<DynamoDBList />} />
      <Route path=":table" element={<DynamoDBDetail />} />
    </Routes>
  )
}
