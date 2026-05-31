import { Routes, Route } from 'react-router-dom'
import { SecretsList } from './SecretsList'
import { SecretsDetail } from './SecretsDetail'

export function SecretsManagerRoutes() {
  return (
    <Routes>
      <Route index element={<SecretsList />} />
      <Route path=":name" element={<SecretsDetail />} />
    </Routes>
  )
}
