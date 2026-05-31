import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { sendMessage } from '../../../api/sqs';
export function SQSMessageSend({ queueUrl, isFifo, onSent }) {
    const [body, setBody] = useState('');
    const [delay, setDelay] = useState(0);
    const [groupId, setGroupId] = useState('');
    const [dedupId, setDedupId] = useState('');
    const [lastMsgId, setLastMsgId] = useState(null);
    const qc = useQueryClient();
    const mut = useMutation({
        mutationFn: (req) => sendMessage(queueUrl, req),
        onSuccess: (data) => {
            setLastMsgId(data.messageId);
            setBody('');
            qc.invalidateQueries({ queryKey: ['sqs', 'queue', queueUrl] });
            onSent?.();
        },
        onError: () => setLastMsgId(null),
    });
    function submit(e) {
        e.preventDefault();
        const req = {
            body,
            ...(delay > 0 ? { delaySeconds: delay } : {}),
            ...(isFifo && groupId ? { messageGroupId: groupId } : {}),
            ...(isFifo && dedupId ? { messageDeduplicationId: dedupId } : {}),
        };
        mut.mutate(req);
    }
    return (_jsxs("div", { style: { background: '#f4f5f7', borderRadius: 8, padding: '1.25rem', marginBottom: '1.5rem' }, children: [_jsx("h4", { style: { margin: '0 0 1rem', fontSize: '1rem', fontWeight: 600 }, children: "Send message" }), _jsxs("form", { onSubmit: submit, children: [_jsxs("label", { style: labelStyle, children: ["Message body", _jsx("textarea", { required: true, value: body, onChange: (e) => setBody(e.target.value), rows: 5, style: { ...inputStyle, fontFamily: 'monospace', resize: 'vertical', fontSize: '0.85em' }, placeholder: '{"key": "value"}' })] }), _jsxs("div", { style: { display: 'grid', gridTemplateColumns: isFifo ? '1fr 1fr 1fr' : '160px', gap: '0.75rem' }, children: [_jsxs("label", { style: labelStyle, children: ["Delay (s)", _jsx("input", { type: "number", min: 0, max: 900, value: delay, onChange: (e) => setDelay(Number(e.target.value)), style: inputStyle })] }), isFifo && (_jsxs(_Fragment, { children: [_jsxs("label", { style: labelStyle, children: ["Message group ID", _jsx("input", { value: groupId, onChange: (e) => setGroupId(e.target.value), style: inputStyle, placeholder: "required for FIFO" })] }), _jsxs("label", { style: labelStyle, children: ["Deduplication ID", _jsx("input", { value: dedupId, onChange: (e) => setDedupId(e.target.value), style: inputStyle, placeholder: "leave blank for content-based" })] })] }))] }), mut.error && (_jsx("p", { style: { color: '#d13212', margin: '0.5rem 0', fontSize: '0.85em' }, children: mut.error.message })), lastMsgId && !mut.isPending && (_jsxs("p", { style: { color: '#1d8102', margin: '0.5rem 0', fontSize: '0.85em' }, children: ["Sent \u2014 MessageId: ", _jsx("code", { style: { background: '#fff', padding: '0.1em 0.4em', borderRadius: 3 }, children: lastMsgId })] })), _jsx("div", { style: { display: 'flex', justifyContent: 'flex-end', marginTop: '0.75rem' }, children: _jsx("button", { type: "submit", disabled: mut.isPending, style: { ...btnPrimary, opacity: mut.isPending ? 0.6 : 1 }, children: mut.isPending ? 'Sending…' : 'Send message' }) })] })] }));
}
const labelStyle = {
    display: 'flex', flexDirection: 'column', gap: '0.3rem',
    marginBottom: '0.75rem', fontSize: '0.9em', fontWeight: 500,
};
const inputStyle = {
    border: '1px solid #c9cdd4', borderRadius: 4,
    padding: '0.4rem 0.6rem', fontSize: '0.9em',
    width: '100%', boxSizing: 'border-box',
};
const btnPrimary = {
    background: '#e77600', color: '#fff', border: 'none',
    borderRadius: 4, padding: '0.5rem 1.25rem',
    cursor: 'pointer', fontSize: '0.9em', fontWeight: 500,
};
