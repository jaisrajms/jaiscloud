import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listKeys, createKey, enableKey, disableKey, scheduleKeyDeletion, cancelKeyDeletion, } from '../../../api/kms';
import { EmptyState } from '../../../components/EmptyState';
const KEY_STATE_COLOR = {
    Enabled: '#037f0c',
    Disabled: '#d13212',
    PendingDeletion: '#8a6116',
    PendingImport: '#0073bb',
};
export function KMSList() {
    const [createOpen, setCreateOpen] = useState(false);
    const [newDesc, setNewDesc] = useState('');
    const [newUsage, setNewUsage] = useState('ENCRYPT_DECRYPT');
    const [newSpec, setNewSpec] = useState('SYMMETRIC_DEFAULT');
    const [deleteKey, setDeleteKey] = useState(null);
    const [deleteDays, setDeleteDays] = useState(30);
    const qc = useQueryClient();
    const { data, isLoading, error } = useQuery({
        queryKey: ['kms', 'keys'],
        queryFn: () => listKeys(),
    });
    const createMut = useMutation({
        mutationFn: () => createKey({ description: newDesc || undefined, keyUsage: newUsage, keySpec: newSpec }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['kms', 'keys'] });
            setCreateOpen(false);
            setNewDesc('');
            setNewUsage('ENCRYPT_DECRYPT');
            setNewSpec('SYMMETRIC_DEFAULT');
        },
    });
    const enableMut = useMutation({
        mutationFn: (keyId) => enableKey(keyId),
        onSuccess: () => void qc.invalidateQueries({ queryKey: ['kms', 'keys'] }),
    });
    const disableMut = useMutation({
        mutationFn: (keyId) => disableKey(keyId),
        onSuccess: () => void qc.invalidateQueries({ queryKey: ['kms', 'keys'] }),
    });
    const scheduleMut = useMutation({
        mutationFn: ({ keyId, days }) => scheduleKeyDeletion(keyId, days),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['kms', 'keys'] });
            setDeleteKey(null);
        },
    });
    const cancelMut = useMutation({
        mutationFn: (keyId) => cancelKeyDeletion(keyId),
        onSuccess: () => void qc.invalidateQueries({ queryKey: ['kms', 'keys'] }),
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading keys\u2026" });
    if (error)
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load: ", error.message] });
    const keys = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "KMS Keys" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [keys.length, " key", keys.length !== 1 ? 's' : ''] })] }), _jsx("button", { onClick: () => setCreateOpen(true), style: btnPrimary, children: "Create key" })] }), keys.length === 0 ? (_jsx(EmptyState, { title: "No KMS keys", description: "KMS keys encrypt and protect your data across AWS services.", cta: "Create Key", onCta: () => setCreateOpen(true) })) : (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Key ID" }), _jsx("th", { style: th, children: "Description" }), _jsx("th", { style: th, children: "Usage" }), _jsx("th", { style: th, children: "Spec" }), _jsx("th", { style: th, children: "State" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Actions" })] }) }), _jsx("tbody", { children: keys.map((k, i) => (_jsxs("tr", { style: { borderBottom: i < keys.length - 1 ? '1px solid #e7e9ec' : 'none' }, children: [_jsx("td", { style: td, children: _jsx("code", { style: { fontSize: '0.85em', color: '#0972d3' }, children: k.keyId }) }), _jsx("td", { style: td, children: k.description || _jsx("span", { style: { color: '#8d9daa' }, children: "\u2014" }) }), _jsx("td", { style: td, children: k.keyUsage }), _jsx("td", { style: td, children: k.keySpec }), _jsx("td", { style: td, children: _jsx("span", { style: {
                                                display: 'inline-block',
                                                padding: '2px 8px',
                                                borderRadius: 12,
                                                fontSize: '0.8em',
                                                fontWeight: 500,
                                                background: (KEY_STATE_COLOR[k.keyState] ?? '#5f6b7a') + '22',
                                                color: KEY_STATE_COLOR[k.keyState] ?? '#5f6b7a',
                                            }, children: k.keyState }) }), _jsxs("td", { style: { ...td, textAlign: 'right', whiteSpace: 'nowrap' }, children: [k.keyState === 'Disabled' && (_jsx("button", { onClick: () => enableMut.mutate(k.keyId), style: btnSmall, children: "Enable" })), k.keyState === 'Enabled' && (_jsx("button", { onClick: () => disableMut.mutate(k.keyId), style: { ...btnSmall, marginLeft: 6 }, children: "Disable" })), k.keyState === 'PendingDeletion' && (_jsx("button", { onClick: () => cancelMut.mutate(k.keyId), style: { ...btnSmall, marginLeft: 6 }, children: "Cancel deletion" })), k.keyState !== 'PendingDeletion' && (_jsx("button", { onClick: () => setDeleteKey(k), style: { ...btnSmall, marginLeft: 6, color: '#d13212', borderColor: '#d13212' }, children: "Schedule deletion" }))] })] }, k.keyId))) })] }) })), createOpen && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 1rem' }, children: "Create KMS Key" }), _jsx("label", { style: label, children: "Description (optional)" }), _jsx("input", { style: input, value: newDesc, onChange: e => setNewDesc(e.target.value), placeholder: "My encryption key" }), _jsx("label", { style: label, children: "Key usage" }), _jsxs("select", { style: input, value: newUsage, onChange: e => setNewUsage(e.target.value), children: [_jsx("option", { value: "ENCRYPT_DECRYPT", children: "ENCRYPT_DECRYPT" }), _jsx("option", { value: "SIGN_VERIFY", children: "SIGN_VERIFY" }), _jsx("option", { value: "GENERATE_VERIFY_MAC", children: "GENERATE_VERIFY_MAC" })] }), _jsx("label", { style: label, children: "Key spec" }), _jsxs("select", { style: input, value: newSpec, onChange: e => setNewSpec(e.target.value), children: [_jsx("option", { value: "SYMMETRIC_DEFAULT", children: "SYMMETRIC_DEFAULT" }), _jsx("option", { value: "RSA_2048", children: "RSA_2048" }), _jsx("option", { value: "RSA_4096", children: "RSA_4096" }), _jsx("option", { value: "ECC_NIST_P256", children: "ECC_NIST_P256" }), _jsx("option", { value: "HMAC_256", children: "HMAC_256" })] }), createMut.error && (_jsx("div", { style: { color: '#d13212', marginBottom: '0.5rem', fontSize: '0.85em' }, children: createMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '1rem' }, children: [_jsx("button", { style: btnSecondary, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnPrimary, onClick: () => createMut.mutate(), disabled: createMut.isPending, children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), deleteKey && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 0.75rem' }, children: "Schedule key deletion" }), _jsxs("p", { style: { margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Key: ", _jsx("code", { children: deleteKey.keyId })] }), _jsx("label", { style: label, children: "Waiting period (days)" }), _jsx("input", { style: input, type: "number", min: 7, max: 30, value: deleteDays, onChange: e => setDeleteDays(Number(e.target.value)) }), _jsxs("div", { style: { display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '1rem' }, children: [_jsx("button", { style: btnSecondary, onClick: () => setDeleteKey(null), children: "Cancel" }), _jsx("button", { style: { ...btnPrimary, background: '#d13212', borderColor: '#d13212' }, onClick: () => scheduleMut.mutate({ keyId: deleteKey.keyId, days: deleteDays }), disabled: scheduleMut.isPending, children: scheduleMut.isPending ? 'Scheduling…' : 'Schedule deletion' })] })] }) }))] }));
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
    background: '#fff', borderRadius: 10, padding: '1.5rem', minWidth: 420, maxWidth: 540,
    boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
};
const label = { display: 'block', fontSize: '0.82em', fontWeight: 600, marginBottom: 4, color: '#2d3748' };
const input = {
    width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #ccc', borderRadius: 6,
    marginBottom: '0.75rem', fontSize: '0.9em', boxSizing: 'border-box',
};
