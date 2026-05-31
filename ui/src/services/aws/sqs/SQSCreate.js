import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useMutation } from '@tanstack/react-query';
import { createQueue } from '../../../api/sqs';
export function SQSCreate({ onClose, onCreated }) {
    const [name, setName] = useState('');
    const [type, setType] = useState('Standard');
    const [visibility, setVisibility] = useState(30);
    const [retention, setRetention] = useState(345600);
    const [dlqArn, setDlqArn] = useState('');
    const [dlqMaxReceive, setDlqMaxReceive] = useState(3);
    const [tagKey, setTagKey] = useState('');
    const [tagVal, setTagVal] = useState('');
    const [tags, setTags] = useState({});
    const mut = useMutation({
        mutationFn: (req) => createQueue(req),
        onSuccess: () => onCreated(),
    });
    function addTag() {
        if (tagKey.trim()) {
            setTags((t) => ({ ...t, [tagKey.trim()]: tagVal }));
            setTagKey('');
            setTagVal('');
        }
    }
    function removeTag(k) {
        setTags((t) => { const n = { ...t }; delete n[k]; return n; });
    }
    function submit(e) {
        e.preventDefault();
        const queueName = type === 'FIFO' && !name.endsWith('.fifo') ? `${name}.fifo` : name;
        const req = {
            name: queueName,
            type,
            visibilityTimeout: visibility,
            retentionPeriod: retention,
            ...(dlqArn ? { dlqArn, dlqMaxReceive } : {}),
            ...(Object.keys(tags).length > 0 ? { tags } : {}),
        };
        mut.mutate(req);
    }
    return (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, onClick: (e) => e.stopPropagation(), children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.25rem' }, children: [_jsx("h3", { style: { margin: 0, fontSize: '1.1rem' }, children: "Create queue" }), _jsx("button", { onClick: onClose, style: { background: 'none', border: 'none', cursor: 'pointer', fontSize: '1.1rem', color: '#5f6b7a', lineHeight: 1 }, children: "\u2715" })] }), _jsxs("form", { onSubmit: submit, children: [_jsxs("label", { style: labelStyle, children: ["Queue name", _jsx("input", { required: true, value: name, onChange: (e) => setName(e.target.value), placeholder: type === 'FIFO' ? 'my-queue  (.fifo appended automatically)' : 'my-queue', style: inputStyle }), type === 'FIFO' && name && !name.endsWith('.fifo') && (_jsxs("span", { style: { fontSize: '0.78em', color: '#5f6b7a' }, children: ["Will be created as ", _jsxs("strong", { children: [name, ".fifo"] })] }))] }), _jsxs("label", { style: labelStyle, children: ["Type", _jsxs("select", { value: type, onChange: (e) => setType(e.target.value), style: inputStyle, children: [_jsx("option", { value: "Standard", children: "Standard" }), _jsx("option", { value: "FIFO", children: "FIFO" })] })] }), _jsxs("div", { style: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem' }, children: [_jsxs("label", { style: labelStyle, children: ["Visibility timeout (s)", _jsx("input", { type: "number", min: 0, max: 43200, value: visibility, onChange: (e) => setVisibility(Number(e.target.value)), style: inputStyle })] }), _jsxs("label", { style: labelStyle, children: ["Retention period (s)", _jsx("input", { type: "number", min: 60, max: 1209600, value: retention, onChange: (e) => setRetention(Number(e.target.value)), style: inputStyle })] })] }), _jsxs("details", { style: { marginTop: '0.75rem' }, children: [_jsx("summary", { style: { cursor: 'pointer', color: '#0972d3', fontSize: '0.9em', marginBottom: '0.75rem' }, children: "Dead-letter queue (optional)" }), _jsxs("label", { style: labelStyle, children: ["DLQ ARN", _jsx("input", { value: dlqArn, onChange: (e) => setDlqArn(e.target.value), placeholder: "arn:aws:sqs:us-east-1:000000000000:my-dlq", style: inputStyle })] }), dlqArn && (_jsxs("label", { style: labelStyle, children: ["Max receive count", _jsx("input", { type: "number", min: 1, max: 1000, value: dlqMaxReceive, onChange: (e) => setDlqMaxReceive(Number(e.target.value)), style: inputStyle })] }))] }), _jsxs("details", { style: { marginTop: '0.75rem' }, children: [_jsx("summary", { style: { cursor: 'pointer', color: '#0972d3', fontSize: '0.9em', marginBottom: '0.75rem' }, children: "Tags (optional)" }), _jsxs("div", { style: { display: 'flex', gap: '0.5rem', marginBottom: '0.5rem', alignItems: 'flex-end' }, children: [_jsx("input", { placeholder: "Key", value: tagKey, onChange: (e) => setTagKey(e.target.value), style: { ...inputStyle, flex: 1, marginBottom: 0 } }), _jsx("input", { placeholder: "Value", value: tagVal, onChange: (e) => setTagVal(e.target.value), style: { ...inputStyle, flex: 1, marginBottom: 0 } }), _jsx("button", { type: "button", onClick: addTag, style: btnSecondary, children: "Add" })] }), Object.entries(tags).map(([k, v]) => (_jsxs("div", { style: { display: 'flex', gap: '0.5rem', alignItems: 'center', fontSize: '0.85em', marginBottom: '0.3rem' }, children: [_jsx("code", { style: { background: '#f4f5f7', padding: '0.2em 0.5em', borderRadius: 3 }, children: k }), _jsx("span", { style: { color: '#5f6b7a' }, children: "=" }), _jsx("code", { style: { background: '#f4f5f7', padding: '0.2em 0.5em', borderRadius: 3 }, children: v }), _jsx("button", { type: "button", onClick: () => removeTag(k), style: { background: 'none', border: 'none', cursor: 'pointer', color: '#d13212', fontSize: '0.85em', padding: '0 0.25rem' }, children: "Remove" })] }, k)))] }), mut.error && (_jsx("p", { style: { color: '#d13212', margin: '1rem 0 0', fontSize: '0.9em' }, children: mut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1.5rem' }, children: [_jsx("button", { type: "button", onClick: onClose, style: btnSecondary, children: "Cancel" }), _jsx("button", { type: "submit", disabled: mut.isPending, style: { ...btnPrimary, opacity: mut.isPending ? 0.6 : 1 }, children: mut.isPending ? 'Creating…' : 'Create queue' })] })] })] }) }));
}
const overlay = {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000,
};
const dialog = {
    background: '#fff', borderRadius: 8, padding: '1.5rem',
    width: 520, maxHeight: '90vh', overflowY: 'auto',
    boxShadow: '0 4px 24px rgba(0,0,0,0.15)',
};
const labelStyle = {
    display: 'flex', flexDirection: 'column', gap: '0.3rem',
    marginBottom: '0.75rem', fontSize: '0.9em', fontWeight: 500, color: '#16191f',
};
const inputStyle = {
    border: '1px solid #c9cdd4', borderRadius: 4,
    padding: '0.4rem 0.6rem', fontSize: '0.9em',
    width: '100%', boxSizing: 'border-box', outline: 'none',
};
const btnSecondary = {
    background: '#fff', border: '1px solid #c9cdd4',
    borderRadius: 4, padding: '0.45rem 1rem',
    cursor: 'pointer', fontSize: '0.9em', whiteSpace: 'nowrap',
};
const btnPrimary = {
    background: '#e77600', color: '#fff', border: 'none',
    borderRadius: 4, padding: '0.5rem 1.25rem',
    cursor: 'pointer', fontSize: '0.9em', fontWeight: 500,
};
