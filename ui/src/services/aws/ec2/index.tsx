import { Routes, Route } from 'react-router-dom'
import { EC2Instances } from './EC2Instances'

export function EC2Routes() {
  return (
    <Routes>
      <Route index element={<EC2Instances />} />
      <Route path="instances" element={<EC2Instances />} />
    </Routes>
  )
}
