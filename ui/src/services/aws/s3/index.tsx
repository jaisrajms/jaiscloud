import { Routes, Route } from 'react-router-dom'
import { S3List } from './S3List'
import { S3Detail } from './S3Detail'

export function S3Routes() {
  return (
    <Routes>
      <Route index element={<S3List />} />
      <Route path=":bucket" element={<S3Detail />} />
    </Routes>
  )
}
