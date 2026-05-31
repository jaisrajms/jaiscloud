import { api } from './client';
const BASE = '/api/ui/v1/sqs';
export function listQueues(params) {
    return api.get(`${BASE}/queues`, params);
}
export function getQueue(queueUrl) {
    return api.get(`${BASE}/queues/detail`, { url: queueUrl });
}
export function createQueue(req) {
    return api.post(`${BASE}/queues`, req);
}
export function deleteQueue(queueUrl) {
    return api.delete(`${BASE}/queues`, { url: queueUrl });
}
export function purgeQueue(queueUrl) {
    return api.post(`${BASE}/queues/purge`, undefined, { url: queueUrl });
}
export function sendMessage(queueUrl, req) {
    return api.post(`${BASE}/queues/messages`, req, { url: queueUrl });
}
export function receiveMessages(queueUrl, params) {
    const qp = { url: queueUrl };
    if (params?.maxMessages)
        qp.maxMessages = params.maxMessages;
    return api.get(`${BASE}/queues/messages`, qp);
}
export function deleteMessage(queueUrl, receiptHandle) {
    return api.delete(`${BASE}/queues/messages/receipt`, { url: queueUrl, receiptHandle });
}
export function peekMessages(queueUrl, params) {
    const qp = { url: queueUrl };
    if (params?.offset != null)
        qp.offset = params.offset;
    if (params?.limit != null)
        qp.limit = params.limit;
    return api.get(`${BASE}/queues/messages/peek`, qp);
}
export function listDLQSources(queueUrl) {
    return api.get(`${BASE}/queues/dlq-sources`, { url: queueUrl });
}
export function getTags(queueUrl) {
    return api.get(`${BASE}/queues/tags`, { url: queueUrl });
}
