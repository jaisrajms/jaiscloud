import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { listSecrets, createSecret, deleteSecret } from '../../../api/secretsmanager';
import { EmptyState } from '../../../components/EmptyState';
export function SecretsList() {
    const [createOpen, setCreateOpen] = useState(false);
    const [newName, setNewName] = useState('');
    const [newValue, setNewValue] = useState('');
    const [newDesc, setNewDesc] = useState('');
    const [confirmDelete, setConfirmDelete] = useState(null);
    const qc = useQueryClient();
    const navigate = useNavigate();
    const { data, isLoading, error } = useQuery({
        queryKey: ['secretsmanager', 'secrets'],
        queryFn: () => listSecrets(),
    });
    const createMut = useMutation({
        mutationFn: () => createSecret({ name: newName, secretString: newValue || undefined, description: newDesc || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['secretsmanager', 'secrets'] });
            setCreateOpen(false);
            setNewName('');
            setNewValue('');
            setNewDesc('');
        },
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteSecret(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['secretsmanager', 'secrets'] });
            setConfirmDelete(null);
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading secrets\u2026" });
    if (error)
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load: ", error.message] });
    const secrets = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "Secrets Manager" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [secrets.length, " secret", secrets.length !== 1 ? 's' : ''] })] }), _jsx("button", { onClick: () => setCreateOpen(true), style: btnPrimary, children: "Create secret" })] }), secrets.length === 0 ? (_jsx(EmptyState, { title: "No secrets", description: "Store and retrieve database credentials, API keys, and other secrets.", cta: "Create Secret", onCta: () => setCreateOpen(true) })) : (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Name" }), _jsx("th", { style: th, children: "Description" }), _jsx("th", { style: th, children: "Last changed" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Actions" })] }) }), _jsx("tbody", { children: secrets.map((s, i) => (_jsxs("tr", { style: { borderBottom: i < secrets.length - 1 ? '1px solid #e7e9ec' : 'none', cursor: 'pointer' }, onClick: () => navigate(encodeURIComponent(s.name)), children: [_jsx("td", { style: { ...td, fontWeight: 500, color: '#0972d3' }, children: s.name }), _jsx("td", { style: td, children: s.description || _jsx("span", { style: { color: '#8d9daa' }, children: "\u2014" }) }), _jsx("td", { style: td, children: s.lastChangedDate || _jsx("span", { style: { color: '#8d9daa' }, children: "\u2014" }) }), _jsx("td", { style: { ...td, textAlign: 'right' }, onClick: e => e.stopPropagation(), children: _jsx("button", { onClick: () => setConfirmDelete(s), style: { ...btnSmall, color: '#d13212', borderColor: '#d13212' }, children: "Delete" }) })] }, s.arn))) })] }) })), createOpen && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 1rem' }, children: "Create Secret" }), _jsx("label", { style: label, children: "Secret name *" }), _jsx("input", { style: input, value: newName, onChange: e => setNewName(e.target.value), placeholder: "my-secret", autoFocus: true }), _jsx("label", { style: label, children: "Secret value (optional)" }), _jsx("textarea", { style: { ...input, height: 80, resize: 'vertical', fontFamily: 'monospace', fontSize: '0.85em' }, value: newValue, onChange: e => setNewValue(e.target.value), placeholder: '{"username":"admin","password":"secret"}' }), _jsx("label", { style: label, children: "Description (optional)" }), _jsx("input", { style: input, value: newDesc, onChange: e => setNewDesc(e.target.value), placeholder: "Database credentials" }), createMut.error && (_jsx("div", { style: { color: '#d13212', marginBottom: '0.5rem', fontSize: '0.85em' }, children: createMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '1rem' }, children: [_jsx("button", { style: btnSecondary, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnPrimary, onClick: () => createMut.mutate(), disabled: !newName || createMut.isPending, children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), confirmDelete && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 0.75rem' }, children: "Delete secret?" }), _jsxs("p", { style: { margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["This will permanently delete ", _jsx("strong", { children: confirmDelete.name }), " and all its versions."] }), deleteMut.error && (_jsx("div", { style: { color: '#d13212', marginBottom: '0.5rem', fontSize: '0.85em' }, children: deleteMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: 8, justifyContent: 'flex-end' }, children: [_jsx("button", { style: btnSecondary, onClick: () => setConfirmDelete(null), children: "Cancel" }), _jsx("button", { style: { ...btnPrimary, background: '#d13212', borderColor: '#d13212' }, onClick: () => deleteMut.mutate(confirmDelete.name), disabled: deleteMut.isPending, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
const btnPrimary = {
    background: '#e87600', color: '#fff', border: '1px solid #e87600',
    borderRadius: 6, padding: '0.4rem 1rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em',
};
const btnSecondary = {
    background: 'transparent', color: '#5f6b7a', border: '1px solid #ccc',
    borderRadius: 6, padding: '0.4rem 1rem', cursor: 'pointer', fontSize: '0.9em',
};
const btnSmall = {
    background: 'transparent', color: '#0972d3', border: '1px solid #0972d3',
    borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: '0.8em',
};
const th = {
    padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, fontSize: '0.82em',
    color: '#5f6b7a', textTransform: 'uppercase', letterSpacing: '0.05em',
};
const td = { padding: '0.7rem 1rem', verticalAlign: 'middle' };
const overlay = {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex',
    alignItems: 'center', justifyContent: 'center', zIndex: 1000,
};
const dialog = {
    background: '#fff', borderRadius: 10, padding: '1.5rem', minWidth: 440, maxWidth: 560,
    boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
};
const label = { display: 'block', fontSize: '0.82em', fontWeight: 600, marginBottom: 4, color: '#2d3748' };
const input = {
    width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #ccc', borderRadius: 6,
    marginBottom: '0.75rem', fontSize: '0.9em', boxSizing: 'border-box',
};
