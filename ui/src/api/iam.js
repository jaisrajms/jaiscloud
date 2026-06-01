import { api } from './client';
const BASE = '/api/ui/v1/iam';
// Roles
export function listRoles(params) {
    return api.get(`${BASE}/roles`, params);
}
export function createRole(req) {
    return api.post(`${BASE}/roles`, req);
}
export function getRole(roleName) {
    return api.get(`${BASE}/roles/${encodeURIComponent(roleName)}`);
}
export function deleteRole(roleName) {
    return api.delete(`${BASE}/roles/${encodeURIComponent(roleName)}`);
}
export function listAttachedRolePolicies(roleName) {
    return api.get(`${BASE}/roles/${encodeURIComponent(roleName)}/policies`);
}
export function attachRolePolicy(roleName, policyArn) {
    return api.post(`${BASE}/roles/${encodeURIComponent(roleName)}/policies`, { policyArn });
}
export function detachRolePolicy(roleName, policyArn) {
    return api.delete(`${BASE}/roles/${encodeURIComponent(roleName)}/policies`, { policyArn });
}
// Users
export function listUsers(params) {
    return api.get(`${BASE}/users`, params);
}
export function createUser(req) {
    return api.post(`${BASE}/users`, req);
}
export function deleteUser(userName) {
    return api.delete(`${BASE}/users/${encodeURIComponent(userName)}`);
}
export function listAccessKeys(userName) {
    return api.get(`${BASE}/users/${encodeURIComponent(userName)}/access-keys`);
}
export function createAccessKey(userName) {
    return api.post(`${BASE}/users/${encodeURIComponent(userName)}/access-keys`);
}
export function deleteAccessKey(userName, accessKeyId) {
    return api.delete(`${BASE}/users/${encodeURIComponent(userName)}/access-keys`, { accessKeyId });
}
// Policies
export function listPolicies(params) {
    return api.get(`${BASE}/policies`, params);
}
export function createPolicy(req) {
    return api.post(`${BASE}/policies`, req);
}
export function deletePolicy(policyArn) {
    return api.delete(`${BASE}/policies`, { policyArn });
}
