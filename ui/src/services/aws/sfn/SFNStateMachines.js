import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { listStateMachines, createStateMachine, deleteStateMachine, } from '../../../api/sfn';
import { EmptyState } from '../../../components/EmptyState';
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' };
const thStyle = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' };
const tdStyle = { padding: '0.6rem 1rem', verticalAlign: 'middle' };
const btnStyle = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' };
const inputStyle = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const modalStyle = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 440, maxWidth: 580 };
export function SFNStateMachines() {
    const qc = useQueryClient();
    const navigate = useNavigate();
    const [createOpen, setCreateOpen] = useState(false);
    const [deleteTarget, setDeleteTarget] = useState(null);
    const [form, setForm] = useState({ name: '', definition: '', roleArn: '', type: 'STANDARD' });
    const { data, isLoading } = useQuery({
        queryKey: ['sfn', 'state-machines'],
        queryFn: () => listStateMachines(),
    });
    const createMut = useMutation({
        mutationFn: () => createStateMachine({ name: form.name, definition: form.definition || undefined, roleArn: form.roleArn || undefined, type: form.type || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['sfn', 'state-machines'] });
            setCreateOpen(false);
            setForm({ name: '', definition: '', roleArn: '', type: 'STANDARD' });
        },
    });
    const deleteMut = useMutation({
        mutationFn: (arn) => deleteStateMachine(arn),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['sfn', 'state-machines'] });
            setDeleteTarget(null);
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading state machines\u2026" });
    const machines = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "Step Functions" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [machines.length, " state machine", machines.length !== 1 ? 's' : ''] })] }), _jsx("button", { style: btnStyle, onClick: () => setCreateOpen(true), children: "Create State Machine" })] }), machines.length === 0 ? (_jsx(EmptyState, { title: "No state machines. Create one to orchestrate workflows." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Name', 'Type', 'Status', 'ARN', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: machines.map(sm => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748', cursor: 'pointer' }, onClick: () => navigate(`executions?arn=${encodeURIComponent(sm.arn)}`), children: [_jsx("td", { style: { ...tdStyle, fontWeight: 600 }, children: sm.name }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }, children: sm.type || '—' }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }, children: sm.status || '—' }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.78rem', fontFamily: 'monospace', maxWidth: 220, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }, children: sm.arn }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, onClick: e => e.stopPropagation(), children: _jsx("button", { style: { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setDeleteTarget(sm), children: "Delete" }) })] }, sm.arn))) })] })), createOpen && (_jsx("div", { style: overlayStyle, onClick: () => setCreateOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: "Create State Machine" }), [
                            { key: 'name', label: 'Name *', placeholder: 'my-workflow' },
                            { key: 'roleArn', label: 'Role ARN', placeholder: 'arn:aws:iam:::role/step-functions-role' },
                            { key: 'definition', label: 'Definition (JSON, optional)', placeholder: '' },
                        ].map(f => (_jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: f.label }), _jsx("input", { style: inputStyle, placeholder: f.placeholder, value: form[f.key], onChange: e => setForm(p => ({ ...p, [f.key]: e.target.value })) })] }, f.key))), _jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1.5rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: "Type" }), _jsxs("select", { style: inputStyle, value: form.type, onChange: e => setForm(p => ({ ...p, type: e.target.value })), children: [_jsx("option", { value: "STANDARD", children: "STANDARD" }), _jsx("option", { value: "EXPRESS", children: "EXPRESS" })] })] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !form.name || createMut.isPending, onClick: () => createMut.mutate(), children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), deleteTarget && (_jsx("div", { style: overlayStyle, onClick: () => setDeleteTarget(null), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem', fontWeight: 600 }, children: "Delete State Machine?" }), _jsxs("p", { style: { color: '#b0bec5', marginBottom: '1.5rem' }, children: ["Delete ", _jsx("strong", { children: deleteTarget.name }), "?"] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setDeleteTarget(null), children: "Cancel" }), _jsx("button", { style: { ...btnStyle, background: '#d13212' }, disabled: deleteMut.isPending, onClick: () => deleteMut.mutate(deleteTarget.arn), children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
