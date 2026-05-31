import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getSecretValue, putSecretValue, listSecretVersions } from '../../../api/secretsmanager';
export function SecretsDetail() {
    const { name } = useParams();
    const navigate = useNavigate();
    const qc = useQueryClient();
    const decodedName = decodeURIComponent(name ?? '');
    const [editOpen, setEditOpen] = useState(false);
    const [newValue, setNewValue] = useState('');
    const [showValue, setShowValue] = useState(false);
    const { data: valueData, isLoading, error } = useQuery({
        queryKey: ['secretsmanager', 'value', decodedName],
        queryFn: () => getSecretValue(decodedName),
        enabled: showValue,
        retry: false,
    });
    const { data: versionsData } = useQuery({
        queryKey: ['secretsmanager', 'versions', decodedName],
        queryFn: () => listSecretVersions(decodedName),
    });
    const putMut = useMutation({
        mutationFn: () => putSecretValue(decodedName, newValue),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['secretsmanager', 'value', decodedName] });
            void qc.invalidateQueries({ queryKey: ['secretsmanager', 'versions', decodedName] });
            setEditOpen(false);
            setNewValue('');
        },
    });
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', gap: 8, marginBottom: '1.5rem' }, children: [_jsx("button", { onClick: () => navigate('/aws/secretsmanager'), style: btnBack, children: "\u2190 Back" }), _jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: decodedName })] }), _jsxs("div", { style: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.5rem' }, children: [_jsxs("div", { style: card, children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '0.75rem' }, children: [_jsx("h3", { style: { margin: 0, fontSize: '1rem', fontWeight: 600 }, children: "Secret Value" }), _jsxs("div", { style: { display: 'flex', gap: 6 }, children: [_jsx("button", { style: btnSecondary, onClick: () => setShowValue(v => !v), children: showValue ? 'Hide' : 'Reveal' }), _jsx("button", { style: btnPrimary, onClick: () => setEditOpen(true), children: "Update" })] })] }), showValue && (isLoading ? (_jsx("div", { style: { color: '#5f6b7a', fontSize: '0.9em' }, children: "Loading\u2026" })) : error ? (_jsx("div", { style: { color: '#d13212', fontSize: '0.9em' }, children: error.message })) : (_jsx("pre", { style: {
                                    background: '#f4f5f7', borderRadius: 6, padding: '0.75rem',
                                    fontFamily: 'monospace', fontSize: '0.82em', overflowX: 'auto',
                                    whiteSpace: 'pre-wrap', wordBreak: 'break-all', margin: 0,
                                }, children: valueData?.SecretString ?? '(binary)' }))), !showValue && (_jsx("div", { style: { color: '#8d9daa', fontSize: '0.9em' }, children: "Click Reveal to view the secret value." }))] }), _jsxs("div", { style: card, children: [_jsx("h3", { style: { margin: '0 0 0.75rem', fontSize: '1rem', fontWeight: 600 }, children: "Versions" }), (versionsData?.items ?? []).length === 0 ? (_jsx("div", { style: { color: '#8d9daa', fontSize: '0.9em' }, children: "No versions yet." })) : (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 6, overflow: 'hidden', fontSize: '0.85em' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '1px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Version ID" }), _jsx("th", { style: th, children: "Stages" }), _jsx("th", { style: th, children: "Created" })] }) }), _jsx("tbody", { children: (versionsData?.items ?? []).map((v, i) => (_jsxs("tr", { style: { borderBottom: i < (versionsData?.items ?? []).length - 1 ? '1px solid #e7e9ec' : 'none' }, children: [_jsx("td", { style: td, children: _jsxs("code", { style: { fontSize: '0.9em' }, children: [v.versionId.slice(0, 8), "\u2026"] }) }), _jsx("td", { style: td, children: (v.versionStages ?? []).join(', ') }), _jsx("td", { style: td, children: v.createdDate || '—' })] }, v.versionId))) })] }) }))] })] }), editOpen && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 1rem' }, children: "Update secret value" }), _jsx("label", { style: labelStyle, children: "New secret value" }), _jsx("textarea", { style: { ...inputStyle, height: 120, fontFamily: 'monospace', fontSize: '0.85em' }, value: newValue, onChange: e => setNewValue(e.target.value), placeholder: '{"username":"admin","password":"new-secret"}', autoFocus: true }), putMut.error && (_jsx("div", { style: { color: '#d13212', marginBottom: '0.5rem', fontSize: '0.85em' }, children: putMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '0.5rem' }, children: [_jsx("button", { style: btnSecondary, onClick: () => setEditOpen(false), children: "Cancel" }), _jsx("button", { style: btnPrimary, onClick: () => putMut.mutate(), disabled: !newValue || putMut.isPending, children: putMut.isPending ? 'Saving…' : 'Save' })] })] }) }))] }));
}
const card = {
    border: '1px solid #e7e9ec', borderRadius: 8, padding: '1.25rem', background: '#fff',
};
const btnBack = {
    background: 'transparent', color: '#5f6b7a', border: 'none', cursor: 'pointer', fontSize: '0.9em', padding: 0,
};
const btnPrimary = {
    background: '#e87600', color: '#fff', border: '1px solid #e87600',
    borderRadius: 6, padding: '0.3rem 0.75rem', cursor: 'pointer', fontSize: '0.85em', fontWeight: 500,
};
const btnSecondary = {
    background: 'transparent', color: '#5f6b7a', border: '1px solid #ccc',
    borderRadius: 6, padding: '0.3rem 0.75rem', cursor: 'pointer', fontSize: '0.85em',
};
const th = {
    padding: '0.5rem 0.75rem', textAlign: 'left', fontWeight: 600, fontSize: '0.8em',
    color: '#5f6b7a', textTransform: 'uppercase',
};
const td = { padding: '0.5rem 0.75rem', verticalAlign: 'middle' };
const overlay = {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex',
    alignItems: 'center', justifyContent: 'center', zIndex: 1000,
};
const dialog = {
    background: '#fff', borderRadius: 10, padding: '1.5rem', minWidth: 440, maxWidth: 560,
    boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
};
const labelStyle = { display: 'block', fontSize: '0.82em', fontWeight: 600, marginBottom: 4, color: '#2d3748' };
const inputStyle = {
    width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #ccc', borderRadius: 6,
    marginBottom: '0.75rem', boxSizing: 'border-box',
};
