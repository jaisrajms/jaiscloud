import { api } from './client';
const BASE = '/api/ui/v1/sns';
export function listTopics(params) {
    return api.get(`${BASE}/topics`, params);
}
export function createTopic(req) {
    return api.post(`${BASE}/topics`, req);
}
export function deleteTopic(arn) {
    return api.delete(`${BASE}/topics`, { arn });
}
export function getTopic(arn) {
    return api.get(`${BASE}/topics/detail`, { arn });
}
export function listSubscriptionsByTopic(arn) {
    return api.get(`${BASE}/topics/subscriptions`, { arn });
}
export function subscribe(arn, req) {
    return api.post(`${BASE}/topics/subscribe`, req, { arn });
}
export function unsubscribe(subArn) {
    return api.delete(`${BASE}/topics/subscriptions/${encodeURIComponent(subArn)}`);
}
export function publish(arn, req) {
    return api.post(`${BASE}/topics/publish`, req, { arn });
}
