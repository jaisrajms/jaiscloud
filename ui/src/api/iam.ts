import { api } from './client'

export interface IAMRole {
  roleName: string
  roleId?: string
  arn: string
  description?: string
  assumeRolePolicyDocument?: string
  createDate?: string
  maxSessionDuration?: number
}

export interface ListRolesResponse {
  items: IAMRole[]
  marker?: string
  total: number
}

export interface IAMUser {
  userName: string
  userId?: string
  arn: string
  createDate?: string
}

export interface ListUsersResponse {
  items: IAMUser[]
  marker?: string
  total: number
}

export interface AccessKey {
  accessKeyId: string
  secretAccessKey?: string
  status: string
  createDate?: string
}

export interface IAMPolicy {
  policyName: string
  policyId?: string
  arn: string
  description?: string
  createDate?: string
  attachmentCount: number
}

export interface ListPoliciesResponse {
  items: IAMPolicy[]
  marker?: string
  total: number
}

export interface AttachedPolicy {
  policyName: string
  policyArn: string
}

const BASE = '/api/ui/v1/iam'

// Roles
export function listRoles(params?: { marker?: string }): Promise<ListRolesResponse> {
  return api.get<ListRolesResponse>(`${BASE}/roles`, params as Record<string, string | number>)
}

export function createRole(req: {
  roleName: string
  assumeRolePolicyDocument?: string
  description?: string
}): Promise<unknown> {
  return api.post<unknown>(`${BASE}/roles`, req)
}

export function getRole(roleName: string): Promise<unknown> {
  return api.get<unknown>(`${BASE}/roles/${encodeURIComponent(roleName)}`)
}

export function deleteRole(roleName: string): Promise<void> {
  return api.delete<void>(`${BASE}/roles/${encodeURIComponent(roleName)}`)
}

export function listAttachedRolePolicies(roleName: string): Promise<{ items: AttachedPolicy[] }> {
  return api.get<{ items: AttachedPolicy[] }>(`${BASE}/roles/${encodeURIComponent(roleName)}/policies`)
}

export function attachRolePolicy(roleName: string, policyArn: string): Promise<void> {
  return api.post<void>(`${BASE}/roles/${encodeURIComponent(roleName)}/policies`, { policyArn })
}

export function detachRolePolicy(roleName: string, policyArn: string): Promise<void> {
  return api.delete<void>(`${BASE}/roles/${encodeURIComponent(roleName)}/policies`, { policyArn })
}

// Users
export function listUsers(params?: { marker?: string }): Promise<ListUsersResponse> {
  return api.get<ListUsersResponse>(`${BASE}/users`, params as Record<string, string | number>)
}

export function createUser(req: { userName: string }): Promise<unknown> {
  return api.post<unknown>(`${BASE}/users`, req)
}

export function deleteUser(userName: string): Promise<void> {
  return api.delete<void>(`${BASE}/users/${encodeURIComponent(userName)}`)
}

export function listAccessKeys(userName: string): Promise<{ items: AccessKey[] }> {
  return api.get<{ items: AccessKey[] }>(`${BASE}/users/${encodeURIComponent(userName)}/access-keys`)
}

export function createAccessKey(userName: string): Promise<{ AccessKey: AccessKey }> {
  return api.post<{ AccessKey: AccessKey }>(`${BASE}/users/${encodeURIComponent(userName)}/access-keys`)
}

export function deleteAccessKey(userName: string, accessKeyId: string): Promise<void> {
  return api.delete<void>(`${BASE}/users/${encodeURIComponent(userName)}/access-keys`, { accessKeyId })
}

// Policies
export function listPolicies(params?: { scope?: string; marker?: string }): Promise<ListPoliciesResponse> {
  return api.get<ListPoliciesResponse>(`${BASE}/policies`, params as Record<string, string | number>)
}

export function createPolicy(req: {
  policyName: string
  policyDocument: string
  description?: string
}): Promise<unknown> {
  return api.post<unknown>(`${BASE}/policies`, req)
}

export function deletePolicy(policyArn: string): Promise<void> {
  return api.delete<void>(`${BASE}/policies`, { policyArn })
}
