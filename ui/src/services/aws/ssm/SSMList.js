import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listParameters, putParameter, deleteParameter, getParameter } from '../../../api/ssm';
import { EmptyState } from '../../../components/EmptyState';
export function SSMList() {
    const [createOpen, setCreateOpen] = useState(false);
    const [newName, setNewName] = useState('');
    const [newValue, setNewValue] = useState('');
    const [newType, setNewType] = useState('String');
    const [newDesc, setNewDesc] = useState('');
    const [pathFilter, setPathFilter] = useState('');
    const [viewParam, setViewParam] = useState(null);
    const [viewValue, setViewValue] = useState(null);
    const [confirmDelete, setConfirmDelete] = useState(null);
    const qc = useQueryClient();
    const { data, isLoading, error } = useQuery({
        queryKey: ['ssm', 'parameters', pathFilter],
        queryFn: () => listParameters(pathFilter ? { path: pathFilter } : undefined),
    });
    const putMut = useMutation({
        mutationFn: () => putParameter({ name: newName, value: newValue, type: newType, description: newDesc || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['ssm', 'parameters'] });
            setCreateOpen(false);
            setNewName('');
            setNewValue('');
            setNewType('String');
            setNewDesc('');
        },
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteParameter(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['ssm', 'parameters'] });
            setConfirmDelete(null);
        },
    });
    const handleView = async (p) => {
        setViewParam(p);
        setViewValue(null);
        try {
            const resp = await getParameter(p.name);
            setViewValue(resp.Parameter?.value ?? '(empty)');
        }
        catch {
            setViewValue('(could not retrieve value)');
        }
    };
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading parameters\u2026" });
    if (error)
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load: ", error.message] });
    const params = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "SSM Parameter Store" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [params.length, " parameter", params.length !== 1 ? 's' : ''] })] }), _jsx("button", { onClick: () => setCreateOpen(true), style: btnPrimary, children: "Create parameter" })] }), _jsx("div", { style: { marginBottom: '1rem' }, children: _jsx("input", { style: { ...inputStyle, marginBottom: 0, maxWidth: 340 }, placeholder: "Filter by path prefix (e.g. /myapp/)", value: pathFilter, onChange: e => setPathFilter(e.target.value) }) }), params.length === 0 ? (_jsx(EmptyState, { title: "No parameters", description: "Store configuration values and secrets as SSM parameters.", cta: "Create Parameter", onCta: () => setCreateOpen(true) })) : (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Name" }), _jsx("th", { style: th, children: "Type" }), _jsx("th", { style: th, children: "Version" }), _jsx("th", { style: th, children: "Last modified" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Actions" })] }) }), _jsx("tbody", { children: params.map((p, i) => (_jsxs("tr", { style: { borderBottom: i < params.length - 1 ? '1px solid #e7e9ec' : 'none' }, children: [_jsx("td", { style: { ...td, fontFamily: 'monospace', fontSize: '0.88em', color: '#0972d3' }, children: p.name }), _jsx("td", { style: td, children: _jsx("span", { style: {
                                                display: 'inline-block', padding: '1px 8px', borderRadius: 10, fontSize: '0.8em',
                                                background: p.type === 'SecureString' ? '#8a611622' : '#0972d322',
                                                color: p.type === 'SecureString' ? '#8a6116' : '#0972d3',
                                            }, children: p.type }) }), _jsx("td", { style: td, children: p.version }), _jsx("td", { style: td, children: p.lastModifiedDate || '—' }), _jsxs("td", { style: { ...td, textAlign: 'right', whiteSpace: 'nowrap' }, children: [_jsx("button", { onClick: () => handleView(p), style: btnSmall, children: "View" }), _jsx("button", { onClick: () => setConfirmDelete(p), style: { ...btnSmall, marginLeft: 6, color: '#d13212', borderColor: '#d13212' }, children: "Delete" })] })] }, p.name))) })] }) })), createOpen && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 1rem' }, children: "Create Parameter" }), _jsx("label", { style: labelStyle, children: "Name *" }), _jsx("input", { style: inputStyle, value: newName, onChange: e => setNewName(e.target.value), placeholder: "/myapp/db-password", autoFocus: true }), _jsx("label", { style: labelStyle, children: "Type" }), _jsxs("select", { style: inputStyle, value: newType, onChange: e => setNewType(e.target.value), children: [_jsx("option", { value: "String", children: "String" }), _jsx("option", { value: "StringList", children: "StringList" }), _jsx("option", { value: "SecureString", children: "SecureString" })] }), _jsx("label", { style: labelStyle, children: "Value *" }), _jsx("textarea", { style: { ...inputStyle, height: 80, fontFamily: 'monospace', fontSize: '0.85em', resize: 'vertical' }, value: newValue, onChange: e => setNewValue(e.target.value), placeholder: "parameter value" }), _jsx("label", { style: labelStyle, children: "Description (optional)" }), _jsx("input", { style: inputStyle, value: newDesc, onChange: e => setNewDesc(e.target.value), placeholder: "My parameter description" }), putMut.error && (_jsx("div", { style: { color: '#d13212', marginBottom: '0.5rem', fontSize: '0.85em' }, children: putMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '0.5rem' }, children: [_jsx("button", { style: btnSecondary, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnPrimary, onClick: () => putMut.mutate(), disabled: !newName || !newValue || putMut.isPending, children: putMut.isPending ? 'Creating…' : 'Create' })] })] }) })), viewParam && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 0.5rem' }, children: viewParam.name }), _jsxs("div", { style: { color: '#5f6b7a', fontSize: '0.85em', marginBottom: '0.75rem' }, children: ["Type: ", viewParam.type, " \u00B7 Version: ", viewParam.version] }), viewParam.description && (_jsx("div", { style: { marginBottom: '0.75rem', fontSize: '0.9em' }, children: viewParam.description })), _jsx("label", { style: labelStyle, children: "Value" }), _jsx("pre", { style: {
                                background: '#f4f5f7', borderRadius: 6, padding: '0.75rem', fontFamily: 'monospace',
                                fontSize: '0.85em', overflowX: 'auto', whiteSpace: 'pre-wrap', wordBreak: 'break-all',
                                margin: '0 0 1rem',
                            }, children: viewValue === null ? 'Loading…' : viewValue }), _jsx("div", { style: { display: 'flex', justifyContent: 'flex-end' }, children: _jsx("button", { style: btnSecondary, onClick: () => { setViewParam(null); setViewValue(null); }, children: "Close" }) })] }) })), confirmDelete && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 0.75rem' }, children: "Delete parameter?" }), _jsxs("p", { style: { margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Delete ", _jsx("code", { children: confirmDelete.name }), "? This cannot be undone."] }), deleteMut.error && (_jsx("div", { style: { color: '#d13212', marginBottom: '0.5rem', fontSize: '0.85em' }, children: deleteMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: 8, justifyContent: 'flex-end' }, children: [_jsx("button", { style: btnSecondary, onClick: () => setConfirmDelete(null), children: "Cancel" }), _jsx("button", { style: { ...btnPrimary, background: '#d13212', borderColor: '#d13212' }, onClick: () => deleteMut.mutate(confirmDelete.name), disabled: deleteMut.isPending, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
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
const labelStyle = { display: 'block', fontSize: '0.82em', fontWeight: 600, marginBottom: 4, color: '#2d3748' };
const inputStyle = {
    width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #ccc', borderRadius: 6,
    marginBottom: '0.75rem', fontSize: '0.9em', boxSizing: 'border-box',
};
