import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Fragment, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getQueue, purgeQueue, listDLQSources, getTags, peekMessages } from '../../../api/sqs';
import { SQSMessageSend } from './SQSMessageSend';
const TAB_LABELS = {
    overview: 'Overview',
    messages: 'Messages',
    dlq: 'Dead-letter queue',
    tags: 'Tags',
};
export function SQSDetail() {
    const { queueUrl: rawParam } = useParams();
    const queueUrl = rawParam ? decodeURIComponent(rawParam) : '';
    const [tab, setTab] = useState('overview');
    const [msgPage, setMsgPage] = useState(0);
    const [expandedMsgId, setExpandedMsgId] = useState(null);
    const [showSend, setShowSend] = useState(false);
    const [purgeConfirm, setPurgeConfirm] = useState(false);
    const PAGE_SIZE = 50;
    const qc = useQueryClient();
    const { data: queue, isLoading, error } = useQuery({
        queryKey: ['sqs', 'queue', queueUrl],
        queryFn: () => getQueue(queueUrl),
        enabled: !!queueUrl,
    });
    const { data: dlqSources } = useQuery({
        queryKey: ['sqs', 'queue', queueUrl, 'dlq-sources'],
        queryFn: () => listDLQSources(queueUrl),
        enabled: tab === 'dlq' && !!queueUrl,
    });
    const { data: tags } = useQuery({
        queryKey: ['sqs', 'queue', queueUrl, 'tags'],
        queryFn: () => getTags(queueUrl),
        enabled: tab === 'tags' && !!queueUrl,
    });
    const { data: peekData, isFetching: peekFetching, refetch: refetchPeek } = useQuery({
        queryKey: ['sqs', 'queue', queueUrl, 'peek', msgPage],
        queryFn: () => peekMessages(queueUrl, { offset: msgPage * PAGE_SIZE, limit: PAGE_SIZE }),
        enabled: tab === 'messages' && !!queueUrl,
    });
    const purgeMut = useMutation({
        mutationFn: () => purgeQueue(queueUrl),
        onSuccess: () => {
            setPurgeConfirm(false);
            void qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl] });
        },
    });
    if (isLoading) {
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading\u2026" });
    }
    if (error || !queue) {
        return (_jsxs("div", { children: [_jsx(Link, { to: "/aws/sqs", style: { color: '#0972d3', fontSize: '0.9em', textDecoration: 'none' }, children: "\u2190 Queues" }), _jsx("p", { style: { color: '#d13212' }, children: error ? error.message : 'Queue not found.' })] }));
    }
    const overviewRows = [
        ['URL', queue.url],
        ['ARN', queue.arn],
        ['Type', queue.type],
        ['Messages available', queue.messagesAvailable.toLocaleString()],
        ['Messages in flight', queue.messagesInFlight.toLocaleString()],
        ['Visibility timeout', `${queue.visibilityTimeout}s`],
        ['Message retention', `${queue.retentionPeriod}s`],
        ['Max message size', `${Math.round(queue.maxMessageSize / 1024)} KB`],
        ['Dead-letter queue', queue.dlqArn || '—'],
        ['Max receive count', queue.dlqMaxReceive ? String(queue.dlqMaxReceive) : '—'],
        ['Created', queue.createdAt ? new Date(queue.createdAt).toLocaleString() : '—'],
    ];
    return (_jsxs("div", { children: [_jsxs("div", { style: { marginBottom: '1.5rem' }, children: [_jsx(Link, { to: "/aws/sqs", style: { color: '#0972d3', fontSize: '0.85em', textDecoration: 'none' }, children: "\u2190 SQS Queues" }), _jsxs("div", { style: { display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', marginTop: '0.5rem', gap: '1rem', flexWrap: 'wrap' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: '0 0 0.4rem', fontSize: '1.4rem', fontWeight: 600 }, children: queue.name }), _jsxs("div", { style: { display: 'flex', alignItems: 'center', gap: '0.6rem' }, children: [_jsx("span", { style: {
                                                    background: queue.type === 'FIFO' ? '#e0f0ff' : '#f4f5f7',
                                                    color: queue.type === 'FIFO' ? '#0972d3' : '#5f6b7a',
                                                    padding: '0.15em 0.55em', borderRadius: 3, fontSize: '0.8em', fontWeight: 500,
                                                }, children: queue.type }), _jsx("code", { style: { fontSize: '0.75em', color: '#8d9daa' }, children: queue.arn })] })] }), _jsx("button", { onClick: () => setPurgeConfirm(true), style: { background: 'none', border: '1px solid #e77600', color: '#e77600', borderRadius: 4, padding: '0.4rem 0.9rem', cursor: 'pointer', fontSize: '0.85em', flexShrink: 0 }, children: "Purge queue" })] })] }), _jsx("div", { style: { display: 'flex', borderBottom: '2px solid #e7e9ec', marginBottom: '1.5rem' }, children: Object.keys(TAB_LABELS).map((t) => (_jsx("button", { onClick: () => setTab(t), style: {
                        background: 'none', border: 'none', padding: '0.6rem 1.25rem', cursor: 'pointer',
                        fontSize: '0.9em', fontWeight: tab === t ? 600 : 400,
                        color: tab === t ? '#e77600' : '#5f6b7a',
                        borderBottom: `2px solid ${tab === t ? '#e77600' : 'transparent'}`,
                        marginBottom: -2, whiteSpace: 'nowrap',
                    }, children: TAB_LABELS[t] }, t))) }), tab === 'overview' && (_jsx("dl", { style: { display: 'grid', gridTemplateColumns: 'max-content 1fr', gap: '0 2rem', margin: 0, fontSize: '0.9em' }, children: overviewRows.map(([label, value]) => (_jsxs(Fragment, { children: [_jsx("dt", { style: { color: '#5f6b7a', fontWeight: 500, padding: '0.5rem 0', borderBottom: '1px solid #f4f5f7', whiteSpace: 'nowrap' }, children: label }), _jsx("dd", { style: { margin: 0, padding: '0.5rem 0', borderBottom: '1px solid #f4f5f7', wordBreak: 'break-all', color: '#16191f' }, children: value })] }, label))) })), tab === 'messages' && (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem', flexWrap: 'wrap' }, children: [_jsx("span", { style: { fontSize: '0.88em', color: '#5f6b7a' }, children: peekData ? `${peekData.total.toLocaleString()} message${peekData.total !== 1 ? 's' : ''}` : '—' }), _jsx("button", { onClick: () => { setMsgPage(0); void refetchPeek(); }, disabled: peekFetching, style: { ...btnSmall, marginLeft: 'auto', opacity: peekFetching ? 0.6 : 1 }, children: peekFetching ? 'Loading…' : '↻ Refresh' }), _jsx("button", { onClick: () => setShowSend((s) => !s), style: { ...btnSmall, borderColor: '#e77600', color: '#e77600' }, children: showSend ? 'Hide send form' : '+ Send message' })] }), showSend && (_jsx("div", { style: { marginBottom: '1.25rem' }, children: _jsx(SQSMessageSend, { queueUrl: queueUrl, isFifo: queue.type === 'FIFO', onSent: () => { void refetchPeek(); } }) })), !peekData || peekData.messages.length === 0 ? (_jsx("p", { style: { color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }, children: peekFetching ? 'Loading messages…' : 'No messages in this queue.' })) : (_jsxs("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: [_jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.88em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Status" }), _jsx("th", { style: th, children: "Message ID" }), _jsx("th", { style: th, children: "Body" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Rcv" }), _jsx("th", { style: th, children: "Sent At" }), queue.type === 'FIFO' && _jsx("th", { style: th, children: "Group" })] }) }), _jsx("tbody", { children: peekData.messages.map((m) => (_jsxs(Fragment, { children: [_jsxs("tr", { style: { borderBottom: expandedMsgId === m.messageId ? 'none' : '1px solid #e7e9ec', cursor: 'pointer' }, onClick: () => setExpandedMsgId(expandedMsgId === m.messageId ? null : m.messageId), onMouseEnter: (e) => (e.currentTarget.style.background = '#fafbfc'), onMouseLeave: (e) => (e.currentTarget.style.background = ''), children: [_jsx("td", { style: td, children: _jsx("span", { style: {
                                                                    display: 'inline-block', padding: '0.1em 0.5em', borderRadius: 3,
                                                                    fontSize: '0.8em', fontWeight: 500,
                                                                    background: m.status === 'visible' ? '#d1fae5' : m.status === 'in-flight' ? '#fef3c7' : '#e0f2fe',
                                                                    color: m.status === 'visible' ? '#065f46' : m.status === 'in-flight' ? '#92400e' : '#0369a1',
                                                                }, children: m.status }) }), _jsx("td", { style: { ...td, fontFamily: 'monospace', fontSize: '0.8em', color: '#5f6b7a', maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }, children: m.messageId }), _jsx("td", { style: { ...td, maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontFamily: 'monospace', fontSize: '0.82em' }, children: m.body }), _jsx("td", { style: { ...td, textAlign: 'right', color: '#5f6b7a' }, children: m.receiveCount }), _jsx("td", { style: { ...td, color: '#5f6b7a', whiteSpace: 'nowrap' }, children: m.sentAt ? new Date(m.sentAt).toLocaleString() : '—' }), queue.type === 'FIFO' && _jsx("td", { style: { ...td, fontFamily: 'monospace', fontSize: '0.82em' }, children: m.groupId ?? '—' })] }), expandedMsgId === m.messageId && (_jsx("tr", { style: { borderBottom: '1px solid #e7e9ec' }, children: _jsx("td", { colSpan: queue.type === 'FIFO' ? 6 : 5, style: { padding: '0 1rem 0.75rem' }, children: _jsx("pre", { style: {
                                                                margin: 0, padding: '0.75rem', background: '#1e1e1e', color: '#d4d4d4',
                                                                borderRadius: 6, overflow: 'auto', fontSize: '0.82em',
                                                                whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: 300,
                                                            }, children: (() => { try {
                                                                return JSON.stringify(JSON.parse(m.body), null, 2);
                                                            }
                                                            catch {
                                                                return m.body;
                                                            } })() }) }) }))] }, m.messageId))) })] }), peekData.total > PAGE_SIZE && (_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0.6rem 1rem', background: '#f4f5f7', borderTop: '1px solid #e7e9ec', fontSize: '0.85em' }, children: [_jsxs("span", { style: { color: '#5f6b7a' }, children: [msgPage * PAGE_SIZE + 1, "\u2013", Math.min((msgPage + 1) * PAGE_SIZE, peekData.total), " of ", peekData.total.toLocaleString()] }), _jsxs("div", { style: { display: 'flex', gap: '0.5rem' }, children: [_jsx("button", { onClick: () => { setMsgPage((p) => p - 1); setExpandedMsgId(null); }, disabled: msgPage === 0, style: btnSmall, children: "\u2190 Prev" }), _jsx("button", { onClick: () => { setMsgPage((p) => p + 1); setExpandedMsgId(null); }, disabled: (msgPage + 1) * PAGE_SIZE >= peekData.total, style: btnSmall, children: "Next \u2192" })] })] }))] }))] })), tab === 'dlq' && (_jsxs("div", { children: [queue.dlqArn ? (_jsxs("div", { style: { marginBottom: '1.5rem', padding: '1rem', background: '#f4f5f7', borderRadius: 6, fontSize: '0.9em' }, children: [_jsx("div", { style: { fontWeight: 500, marginBottom: '0.5rem', color: '#5f6b7a' }, children: "Dead-letter queue ARN" }), _jsx("code", { style: { wordBreak: 'break-all' }, children: queue.dlqArn }), queue.dlqMaxReceive && (_jsxs("div", { style: { marginTop: '0.5rem', color: '#5f6b7a' }, children: ["Max receive count: ", _jsx("strong", { children: queue.dlqMaxReceive })] }))] })) : (_jsx("p", { style: { color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }, children: "No dead-letter queue configured." })), dlqSources && dlqSources.items.length > 0 && (_jsxs("div", { children: [_jsx("h4", { style: { margin: '0 0 0.75rem', fontSize: '0.95rem', fontWeight: 600, color: '#16191f' }, children: "Source queues using this queue as DLQ" }), _jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 6, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Name" }), _jsx("th", { style: th, children: "Type" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Max Receive Count" })] }) }), _jsx("tbody", { children: dlqSources.items.map((q) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec' }, children: [_jsx("td", { style: td, children: _jsx(Link, { to: `/aws/sqs/${encodeURIComponent(q.url)}`, style: { color: '#0972d3', textDecoration: 'none' }, children: q.name }) }), _jsx("td", { style: td, children: q.type }), _jsx("td", { style: { ...td, textAlign: 'right' }, children: q.dlqMaxReceive ?? '—' })] }, q.url))) })] }) })] })), dlqSources && dlqSources.items.length === 0 && (_jsx("p", { style: { color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }, children: "No queues are using this queue as their dead-letter queue." }))] })), tab === 'tags' && (_jsx("div", { children: !tags || Object.keys(tags).length === 0 ? (_jsx("p", { style: { color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }, children: "No tags on this queue." })) : (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 6, overflow: 'hidden' }, children: _jsxs("table", { style: { borderCollapse: 'collapse', fontSize: '0.9em', width: '100%' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Key" }), _jsx("th", { style: th, children: "Value" })] }) }), _jsx("tbody", { children: Object.entries(tags).map(([k, v]) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec' }, children: [_jsx("td", { style: td, children: _jsx("code", { style: { background: '#f4f5f7', padding: '0.2em 0.5em', borderRadius: 3 }, children: k }) }), _jsx("td", { style: td, children: v })] }, k))) })] }) })) })), purgeConfirm && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 0.75rem', fontSize: '1.05rem' }, children: "Purge queue?" }), _jsxs("p", { style: { margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["All messages in ", _jsx("strong", { children: queue.name }), " will be permanently deleted. This action cannot be undone."] }), purgeMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: purgeMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => setPurgeConfirm(false), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => purgeMut.mutate(), disabled: purgeMut.isPending, style: { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em', opacity: purgeMut.isPending ? 0.6 : 1 }, children: purgeMut.isPending ? 'Purging…' : 'Purge' })] })] }) }))] }));
}
const th = { padding: '0.55rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' };
const td = { padding: '0.7rem 1rem' };
const btnSmall = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.3rem 0.75rem', cursor: 'pointer', fontSize: '0.83em' };
const btnSecondary = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const dialogStyle = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 460, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' };
