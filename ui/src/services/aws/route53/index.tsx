import { Routes, Route } from 'react-router-dom'
import { Route53Zones } from './Route53Zones'

export function Route53Routes() {
  return (
    <Routes>
      <Route index element={<Route53Zones />} />
      <Route path="zones" element={<Route53Zones />} />
    </Routes>
  )
}
