import { Routes, Route } from 'react-router-dom'
import { CFNStacks } from './CFNStacks'

export function CloudFormationRoutes() {
  return (
    <Routes>
      <Route index element={<CFNStacks />} />
      <Route path="stacks" element={<CFNStacks />} />
    </Routes>
  )
}
