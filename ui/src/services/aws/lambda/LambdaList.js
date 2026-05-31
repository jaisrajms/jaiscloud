import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { listFunctions, deleteFunction } from '../../../api/lambda';
import { EmptyState } from '../../../components/EmptyState';
function stateColor(state) {
    if (state === 'Active')
        return '#1d8102';
    if (state === 'Pending')
        return '#e77600';
    if (state === 'Inactive' || state === 'Failed')
        return '#d13212';
    return '#5f6b7a';
}
function fmtDate(s) {
    if (!s)
        return '—';
    try {
        return new Date(s).toLocaleString();
    }
    catch {
        return s;
    }
}
export function LambdaList() {
    const [confirmDelete, setConfirmDelete] = useState(null);
    const qc = useQueryClient();
    const navigate = useNavigate();
    const { data, isLoading, error } = useQuery({
        queryKey: ['lambda', 'functions'],
        queryFn: () => listFunctions(),
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteFunction(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['lambda', 'functions'] });
            setConfirmDelete(null);
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading functions\u2026" });
    if (error)
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load functions: ", error.message] });
    const functions = data?.items ?? [];
    return (_jsxs("div", { children: [_jsx("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: _jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "Lambda Functions" }) }), functions.length === 0 ? (_jsx(EmptyState, { title: "No functions", description: "Lambda functions let you run code without managing infrastructure." })) : (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Name" }), _jsx("th", { style: th, children: "Runtime" }), _jsx("th", { style: th, children: "Handler" }), _jsx("th", { style: th, children: "Last Modified" }), _jsx("th", { style: th, children: "State" }), _jsx("th", { style: { ...th, width: 80 } })] }) }), _jsx("tbody", { children: functions.map((fn) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }, onClick: () => navigate(`/aws/lambda/${encodeURIComponent(fn.name)}`), onMouseEnter: (e) => (e.currentTarget.style.background = '#fafbfc'), onMouseLeave: (e) => (e.currentTarget.style.background = ''), children: [_jsx("td", { style: td, children: _jsx("span", { style: { color: '#0972d3', fontWeight: 500 }, children: fn.name }) }), _jsx("td", { style: td, children: _jsx("code", { style: { fontSize: '0.85em', background: '#f4f5f7', padding: '0.15em 0.4em', borderRadius: 3 }, children: fn.runtime }) }), _jsx("td", { style: { ...td, color: '#5f6b7a', fontFamily: 'monospace', fontSize: '0.85em' }, children: fn.handler }), _jsx("td", { style: { ...td, color: '#5f6b7a' }, children: fmtDate(fn.lastModified) }), _jsx("td", { style: td, children: _jsx("span", { style: { color: stateColor(fn.state), fontWeight: 500, fontSize: '0.85em' }, children: fn.state || '—' }) }), _jsx("td", { style: td, onClick: (e) => e.stopPropagation(), children: _jsx("button", { onClick: () => setConfirmDelete(fn), style: btnSmall, children: "Delete" }) })] }, fn.arn))) })] }) })), confirmDelete && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 0.75rem', fontSize: '1.05rem' }, children: "Delete function?" }), _jsxs("p", { style: { margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Permanently delete ", _jsx("strong", { children: confirmDelete.name }), "?"] }), deleteMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: deleteMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => setConfirmDelete(null), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => deleteMut.mutate(confirmDelete.name), disabled: deleteMut.isPending, style: { background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em', opacity: deleteMut.isPending ? 0.6 : 1 }, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
const th = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' };
const td = { padding: '0.75rem 1rem' };
const btnSmall = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' };
const btnSecondary = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const dialogStyle = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 360, maxWidth: 440, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' };
