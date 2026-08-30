import { Routes, Route, Navigate } from 'react-router-dom'
import { ELBv2LoadBalancers } from './ELBv2LoadBalancers'

export function ELBv2Routes() {
  return (
    <Routes>
      <Route index element={<Navigate to="load-balancers" replace />} />
      <Route path="load-balancers" element={<ELBv2LoadBalancers />} />
      <Route path="*" element={<Navigate to="load-balancers" replace />} />
    </Routes>
  )
}
