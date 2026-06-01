import { api } from './client';
const BASE = '/api/ui/v1/ecs';
export const listClusters = () => api.get(`${BASE}/clusters`);
export const createCluster = (name) => api.post(`${BASE}/clusters`, { name });
export const deleteCluster = (name) => api.delete(`${BASE}/clusters/${encodeURIComponent(name)}`);
export const listTasks = (clusterName) => api.get(`${BASE}/clusters/${encodeURIComponent(clusterName)}/tasks`);
export const runTask = (clusterName, taskDefinition, count) => api.post(`${BASE}/clusters/${encodeURIComponent(clusterName)}/tasks`, { taskDefinition, count });
export const stopTask = (taskArn) => api.post(`${BASE}/tasks/${encodeURIComponent(taskArn)}/stop`);
export const listServices = (clusterName) => api.get(`${BASE}/clusters/${encodeURIComponent(clusterName)}/services`);
