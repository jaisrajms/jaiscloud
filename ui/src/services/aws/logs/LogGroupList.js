import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { listLogGroups, createLogGroup, deleteLogGroup } from '../../../api/logs';
import { EmptyState } from '../../../components/EmptyState';
function fmtBytes(n) {
    if (n === 0)
        return '0 B';
    if (n < 1024)
        return `${n} B`;
    if (n < 1048576)
        return `${(n / 1024).toFixed(1)} KB`;
    if (n < 1073741824)
        return `${(n / 1048576).toFixed(1)} MB`;
    return `${(n / 1073741824).toFixed(2)} GB`;
}
function fmtDate(ms) {
    if (!ms)
        return '—';
    return new Date(ms).toLocaleString();
}
export function LogGroupList() {
    const [confirmDelete, setConfirmDelete] = useState(null);
    const [createName, setCreateName] = useState('');
    const [showCreate, setShowCreate] = useState(false);
    const qc = useQueryClient();
    const navigate = useNavigate();
    const { data, isLoading, error } = useQuery({
        queryKey: ['logs', 'groups'],
        queryFn: () => listLogGroups(),
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteLogGroup(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['logs', 'groups'] });
            setConfirmDelete(null);
        },
    });
    const createMut = useMutation({
        mutationFn: (name) => createLogGroup(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['logs', 'groups'] });
            setShowCreate(false);
            setCreateName('');
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading log groups\u2026" });
    if (error)
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load log groups: ", error.message] });
    const groups = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "CloudWatch Log Groups" }), _jsx("button", { onClick: () => setShowCreate(true), style: btnPrimary, children: "Create log group" })] }), groups.length === 0 ? (_jsx(EmptyState, { title: "No log groups", description: "CloudWatch Logs lets you monitor, store, and access log files from your resources.", cta: "Create Log Group", onCta: () => setShowCreate(true) })) : (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Name" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Retention" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Stored" }), _jsx("th", { style: th, children: "Created" }), _jsx("th", { style: { ...th, width: 80 } })] }) }), _jsx("tbody", { children: groups.map((g) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }, onClick: () => navigate(`/aws/logs/groups/${encodeURIComponent(g.name)}`), onMouseEnter: (e) => (e.currentTarget.style.background = '#fafbfc'), onMouseLeave: (e) => (e.currentTarget.style.background = ''), children: [_jsx("td", { style: td, children: _jsx("span", { style: { color: '#0972d3', fontWeight: 500, fontFamily: 'monospace', fontSize: '0.9em' }, children: g.name }) }), _jsx("td", { style: { ...td, textAlign: 'right', color: '#5f6b7a' }, children: g.retentionDays ? `${g.retentionDays}d` : 'Never expire' }), _jsx("td", { style: { ...td, textAlign: 'right', color: '#5f6b7a' }, children: fmtBytes(g.storedBytes) }), _jsx("td", { style: { ...td, color: '#5f6b7a' }, children: fmtDate(g.createdAt) }), _jsx("td", { style: td, onClick: (e) => e.stopPropagation(), children: _jsx("button", { onClick: () => setConfirmDelete(g.name), style: btnSmall, children: "Delete" }) })] }, g.arn || g.name))) })] }) })), showCreate && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.25rem' }, children: [_jsx("h3", { style: { margin: 0, fontSize: '1.1rem' }, children: "Create log group" }), _jsx("button", { onClick: () => setShowCreate(false), style: { background: 'none', border: 'none', cursor: 'pointer', fontSize: '1.1rem', color: '#5f6b7a' }, children: "\u2715" })] }), _jsxs("form", { onSubmit: (e) => { e.preventDefault(); createMut.mutate(createName); }, children: [_jsxs("label", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem', fontSize: '0.9em', fontWeight: 500 }, children: ["Log group name", _jsx("input", { required: true, value: createName, onChange: (e) => setCreateName(e.target.value), placeholder: "/aws/lambda/my-function", style: { border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.4rem 0.6rem', fontSize: '0.9em', fontFamily: 'monospace' } })] }), createMut.error && _jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: createMut.error.message }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { type: "button", onClick: () => setShowCreate(false), style: btnSecondary, children: "Cancel" }), _jsx("button", { type: "submit", disabled: createMut.isPending, style: { ...btnPrimary, opacity: createMut.isPending ? 0.6 : 1 }, children: createMut.isPending ? 'Creating…' : 'Create' })] })] })] }) })), confirmDelete && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 0.75rem', fontSize: '1.05rem' }, children: "Delete log group?" }), _jsxs("p", { style: { margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Permanently delete ", _jsx("strong", { style: { fontFamily: 'monospace' }, children: confirmDelete }), " and all its log streams?"] }), deleteMut.error && _jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: deleteMut.error.message }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => setConfirmDelete(null), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => deleteMut.mutate(confirmDelete), disabled: deleteMut.isPending, style: { background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em', opacity: deleteMut.isPending ? 0.6 : 1 }, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
const th = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' };
const td = { padding: '0.75rem 1rem' };
const btnPrimary = { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1.25rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em' };
const btnSecondary = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' };
const btnSmall = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const dialogStyle = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 500, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' };
