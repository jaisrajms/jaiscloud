import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { receiveMessages, deleteMessage } from '../../../api/sqs';
function tryPrettyJson(s) {
    try {
        return JSON.stringify(JSON.parse(s), null, 2);
    }
    catch {
        return s;
    }
}
export function SQSMessageReceive({ queueUrl }) {
    const [messages, setMessages] = useState([]);
    const [maxMessages, setMaxMessages] = useState(10);
    const [expandedId, setExpandedId] = useState(null);
    const qc = useQueryClient();
    const receiveMut = useMutation({
        mutationFn: () => receiveMessages(queueUrl, { maxMessages }),
        onSuccess: (data) => {
            setMessages(data.messages ?? []);
            qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl] });
        },
    });
    const deleteMut = useMutation({
        mutationFn: (receipt) => deleteMessage(queueUrl, receipt),
        onSuccess: (_, receipt) => {
            setMessages((prev) => prev.filter((m) => m.receiptHandle !== receipt));
            qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl] });
        },
    });
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1rem', flexWrap: 'wrap' }, children: [_jsx("h4", { style: { margin: 0, fontSize: '1rem', fontWeight: 600 }, children: "Receive messages" }), _jsxs("label", { style: { display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.9em', marginLeft: 'auto' }, children: ["Max:", _jsx("input", { type: "number", min: 1, max: 10, value: maxMessages, onChange: (e) => setMaxMessages(Number(e.target.value)), style: { width: 56, border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.3rem 0.5rem', fontSize: '0.9em' } })] }), _jsx("button", { onClick: () => receiveMut.mutate(), disabled: receiveMut.isPending, style: {
                            background: '#0972d3', color: '#fff', border: 'none',
                            borderRadius: 4, padding: '0.4rem 1rem',
                            cursor: 'pointer', fontSize: '0.9em',
                            opacity: receiveMut.isPending ? 0.6 : 1,
                        }, children: receiveMut.isPending ? 'Polling…' : 'Poll for messages' })] }), receiveMut.error && (_jsx("p", { style: { color: '#d13212', fontSize: '0.85em', margin: '0 0 0.75rem' }, children: receiveMut.error.message })), receiveMut.isSuccess && messages.length === 0 && (_jsx("p", { style: { color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }, children: "No messages available in the queue." })), messages.length > 0 && (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 6, overflow: 'hidden' }, children: messages.map((m, i) => (_jsxs("div", { style: { borderBottom: i < messages.length - 1 ? '1px solid #e7e9ec' : undefined, padding: '0.75rem 1rem' }, children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.5rem', flexWrap: 'wrap' }, children: [_jsx("code", { style: { fontSize: '0.78em', color: '#5f6b7a', flexShrink: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 300 }, children: m.messageId }), m.sentAt && (_jsx("span", { style: { fontSize: '0.75em', color: '#8d9daa', flexShrink: 0 }, children: new Date(m.sentAt).toLocaleString() })), _jsxs("div", { style: { display: 'flex', gap: '0.4rem', marginLeft: 'auto', flexShrink: 0 }, children: [_jsx("button", { onClick: () => setExpandedId(expandedId === m.messageId ? null : m.messageId), style: btnSmall, children: expandedId === m.messageId ? 'Collapse' : 'View body' }), _jsx("button", { onClick: () => deleteMut.mutate(m.receiptHandle), disabled: deleteMut.isPending, style: { ...btnSmall, borderColor: '#d13212', color: '#d13212' }, children: "Delete" })] })] }), expandedId === m.messageId ? (_jsx("pre", { style: {
                                margin: 0, padding: '0.75rem', background: '#f4f5f7',
                                borderRadius: 4, overflow: 'auto', fontSize: '0.82em',
                                whiteSpace: 'pre-wrap', wordBreak: 'break-all', maxHeight: 300,
                            }, children: tryPrettyJson(m.body) })) : (_jsx("div", { style: {
                                fontSize: '0.85em', color: '#16191f',
                                overflow: 'hidden', textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap', fontFamily: 'monospace',
                            }, children: m.body }))] }, m.messageId))) }))] }));
}
const btnSmall = {
    background: 'none', border: '1px solid #c9cdd4',
    borderRadius: 4, padding: '0.2rem 0.6rem',
    cursor: 'pointer', fontSize: '0.8em',
};
