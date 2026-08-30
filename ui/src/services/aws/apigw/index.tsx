import { Routes, Route, Navigate } from 'react-router-dom'
import { APIGatewayAPIs } from './APIGatewayAPIs'

export function APIGatewayRoutes() {
  return (
    <Routes>
      <Route index element={<Navigate to="apis" replace />} />
      <Route path="apis" element={<APIGatewayAPIs />} />
    </Routes>
  )
}
