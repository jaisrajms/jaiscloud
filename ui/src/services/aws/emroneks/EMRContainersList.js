import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { listVirtualClusters, createVirtualCluster, deleteVirtualCluster, } from '../../../api/emroneks';
import { EmptyState } from '../../../components/EmptyState';
const STATE_COLOR = {
    RUNNING: '#037f0c',
    ARRESTED: '#d13212',
    TERMINATING: '#8a6116',
    TERMINATED: '#5f6b7a',
};
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' };
const thStyle = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' };
const tdStyle = { padding: '0.6rem 1rem', verticalAlign: 'middle' };
const btnStyle = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' };
const inputStyle = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const modalStyle = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 400, maxWidth: 520 };
const fieldStyle = { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' };
const labelStyle = { fontSize: '0.8rem', color: '#b0bec5' };
export function EMRContainersList() {
    const [stateFilter, setStateFilter] = useState('');
    const [createOpen, setCreateOpen] = useState(false);
    const [deleteTarget, setDeleteTarget] = useState(null);
    const [form, setForm] = useState({ name: '', eksClusterId: '', namespace: '' });
    const qc = useQueryClient();
    const navigate = useNavigate();
    const { data, isLoading, error } = useQuery({
        queryKey: ['emrc', 'virtual-clusters', stateFilter],
        queryFn: () => listVirtualClusters(stateFilter ? { state: stateFilter } : undefined),
    });
    const createMut = useMutation({
        mutationFn: () => createVirtualCluster({ name: form.name, eksClusterId: form.eksClusterId || undefined, namespace: form.namespace || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['emrc', 'virtual-clusters'] });
            setCreateOpen(false);
            setForm({ name: '', eksClusterId: '', namespace: '' });
        },
    });
    const deleteMut = useMutation({
        mutationFn: (id) => deleteVirtualCluster(id),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['emrc', 'virtual-clusters'] });
            setDeleteTarget(null);
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading virtual clusters\u2026" });
    if (error)
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load: ", error.message] });
    const vcs = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "EMR on EKS \u2014 Virtual Clusters" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [vcs.length, " cluster", vcs.length !== 1 ? 's' : ''] })] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem' }, children: [_jsxs("select", { value: stateFilter, onChange: e => setStateFilter(e.target.value), style: { ...inputStyle, width: 'auto' }, children: [_jsx("option", { value: "", children: "All states" }), ['RUNNING', 'ARRESTED', 'TERMINATING', 'TERMINATED'].map(s => (_jsx("option", { value: s, children: s }, s)))] }), _jsx("button", { style: btnStyle, onClick: () => setCreateOpen(true), children: "Create Virtual Cluster" })] })] }), vcs.length === 0 ? (_jsx(EmptyState, { title: "No virtual clusters. Create one to run jobs on EKS." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['ID', 'Name', 'State', 'EKS Cluster', 'Namespace', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: vcs.map(vc => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748', cursor: 'pointer' }, onClick: () => navigate(`/aws/emr-containers/${vc.id}`), children: [_jsx("td", { style: { ...tdStyle, fontFamily: 'monospace', fontSize: '0.82rem' }, children: vc.id }), _jsx("td", { style: tdStyle, children: vc.name }), _jsx("td", { style: tdStyle, children: _jsx("span", { style: { color: STATE_COLOR[vc.state] ?? '#5f6b7a', fontWeight: 600 }, children: vc.state }) }), _jsx("td", { style: tdStyle, children: vc.eksCluster || '—' }), _jsx("td", { style: tdStyle, children: vc.namespace || '—' }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, onClick: e => e.stopPropagation(), children: _jsx("button", { style: { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setDeleteTarget(vc), children: "Delete" }) })] }, vc.id))) })] })), createOpen && (_jsx("div", { style: overlayStyle, onClick: () => setCreateOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: "Create Virtual Cluster" }), [
                            { key: 'name', label: 'Name *', placeholder: 'my-vc' },
                            { key: 'eksClusterId', label: 'EKS Cluster ID', placeholder: 'my-eks-cluster' },
                            { key: 'namespace', label: 'Namespace', placeholder: 'default' },
                        ].map(f => (_jsxs("div", { style: fieldStyle, children: [_jsx("label", { style: labelStyle, children: f.label }), _jsx("input", { style: inputStyle, placeholder: f.placeholder, value: form[f.key], onChange: e => setForm(prev => ({ ...prev, [f.key]: e.target.value })) })] }, f.key))), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1rem' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !form.name || createMut.isPending, onClick: () => createMut.mutate(), children: createMut.isPending ? 'Creating…' : 'Create' })] }), createMut.isError && _jsx("div", { style: { color: '#d13212', marginTop: '0.5rem', fontSize: '0.85rem' }, children: createMut.error.message })] }) })), deleteTarget && (_jsx("div", { style: overlayStyle, onClick: () => setDeleteTarget(null), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem', fontWeight: 600 }, children: "Delete Virtual Cluster?" }), _jsxs("p", { style: { color: '#b0bec5', marginBottom: '1.5rem' }, children: ["Delete ", _jsx("strong", { children: deleteTarget.name }), "?"] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setDeleteTarget(null), children: "Cancel" }), _jsx("button", { style: { ...btnStyle, background: '#d13212' }, disabled: deleteMut.isPending, onClick: () => deleteMut.mutate(deleteTarget.id), children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
