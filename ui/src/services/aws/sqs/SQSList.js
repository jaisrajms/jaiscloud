import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { listQueues, deleteQueue } from '../../../api/sqs';
import { EmptyState } from '../../../components/EmptyState';
import { SQSCreate } from './SQSCreate';
function fmtDate(iso) {
    if (!iso)
        return '—';
    try {
        return new Date(iso).toLocaleString();
    }
    catch {
        return iso;
    }
}
export function SQSList() {
    const [createOpen, setCreateOpen] = useState(false);
    const [confirmDelete, setConfirmDelete] = useState(null);
    const qc = useQueryClient();
    const navigate = useNavigate();
    const { data, isLoading, error } = useQuery({
        queryKey: ['sqs', 'queues'],
        queryFn: () => listQueues(),
    });
    const deleteMut = useMutation({
        mutationFn: (url) => deleteQueue(url),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['sqs', 'queues'] });
            setConfirmDelete(null);
        },
    });
    if (isLoading) {
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading queues\u2026" });
    }
    if (error) {
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load queues: ", error.message] });
    }
    const queues = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "SQS Queues" }), data?.total != null && (_jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [data.total, " queue", data.total !== 1 ? 's' : ''] }))] }), _jsx("button", { onClick: () => setCreateOpen(true), style: btnPrimary, children: "Create queue" })] }), queues.length === 0 ? (_jsx(EmptyState, { title: "No queues", description: "SQS queues let your applications communicate asynchronously.", cta: "Create Queue", onCta: () => setCreateOpen(true) })) : (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Name" }), _jsx("th", { style: th, children: "Type" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Msgs Available" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "In Flight" }), _jsx("th", { style: th, children: "Created" }), _jsx("th", { style: { ...th, width: 80 } })] }) }), _jsx("tbody", { children: queues.map((q) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }, onClick: () => navigate(`/aws/sqs/${encodeURIComponent(q.url)}`), onMouseEnter: (e) => (e.currentTarget.style.background = '#fafbfc'), onMouseLeave: (e) => (e.currentTarget.style.background = ''), children: [_jsxs("td", { style: td, children: [_jsx("span", { style: { color: '#0972d3', fontWeight: 500 }, children: q.name }), q.dlqArn && (_jsx("span", { style: { marginLeft: '0.5rem', fontSize: '0.75em', color: '#8d9daa' }, children: "DLQ" }))] }), _jsx("td", { style: td, children: _jsx("span", { style: {
                                                display: 'inline-block', padding: '0.15em 0.55em', borderRadius: 3,
                                                fontSize: '0.8em', fontWeight: 500,
                                                background: q.type === 'FIFO' ? '#e0f0ff' : '#f4f5f7',
                                                color: q.type === 'FIFO' ? '#0972d3' : '#5f6b7a',
                                            }, children: q.type }) }), _jsx("td", { style: { ...td, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }, children: q.messagesAvailable.toLocaleString() }), _jsx("td", { style: { ...td, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }, children: q.messagesInFlight.toLocaleString() }), _jsx("td", { style: { ...td, color: '#5f6b7a' }, children: fmtDate(q.createdAt) }), _jsx("td", { style: td, onClick: (e) => e.stopPropagation(), children: _jsx("button", { onClick: () => setConfirmDelete(q), style: btnDelete, children: "Delete" }) })] }, q.url))) })] }) })), createOpen && (_jsx(SQSCreate, { onClose: () => setCreateOpen(false), onCreated: () => {
                    setCreateOpen(false);
                    void qc.invalidateQueries({ queryKey: ['sqs', 'queues'] });
                } })), confirmDelete && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 0.75rem', fontSize: '1.05rem' }, children: "Delete queue?" }), _jsxs("p", { style: { margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Permanently delete ", _jsx("strong", { children: confirmDelete.name }), "? All messages will be lost and cannot be recovered."] }), deleteMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: deleteMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => setConfirmDelete(null), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => deleteMut.mutate(confirmDelete.url), disabled: deleteMut.isPending, style: { ...btnDanger, opacity: deleteMut.isPending ? 0.6 : 1 }, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
const th = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' };
const td = { padding: '0.75rem 1rem' };
const btnPrimary = { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1.25rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em' };
const btnSecondary = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' };
const btnDanger = { background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' };
const btnDelete = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const dialogStyle = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 460, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' };
