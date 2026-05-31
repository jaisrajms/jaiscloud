import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { listClusters, runJobFlow, terminateCluster, } from '../../../api/emr';
import { EmptyState } from '../../../components/EmptyState';
const STATE_COLOR = {
    RUNNING: '#037f0c',
    WAITING: '#0073bb',
    STARTING: '#e77600',
    BOOTSTRAPPING: '#e77600',
    TERMINATING: '#8a6116',
    TERMINATED: '#5f6b7a',
    TERMINATED_WITH_ERRORS: '#d13212',
};
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' };
const thStyle = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' };
const tdStyle = { padding: '0.6rem 1rem', verticalAlign: 'middle' };
const btnStyle = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' };
const inputStyle = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', minWidth: 160 };
const actionBtnStyle = { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const modalStyle = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 400, maxWidth: 520 };
const fieldStyle = { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' };
const labelStyle = { fontSize: '0.8rem', color: '#b0bec5' };
export function EMRList() {
    const [stateFilter, setStateFilter] = useState('');
    const [createOpen, setCreateOpen] = useState(false);
    const [terminateTarget, setTerminateTarget] = useState(null);
    const [form, setForm] = useState({ name: '', releaseLabel: '', logUri: '', serviceRole: '', jobFlowRole: '' });
    const qc = useQueryClient();
    const navigate = useNavigate();
    const { data, isLoading, error } = useQuery({
        queryKey: ['emr', 'clusters', stateFilter],
        queryFn: () => listClusters(stateFilter ? { state: stateFilter } : undefined),
    });
    const createMut = useMutation({
        mutationFn: () => runJobFlow({ name: form.name, releaseLabel: form.releaseLabel || undefined, logUri: form.logUri || undefined, serviceRole: form.serviceRole || undefined, jobFlowRole: form.jobFlowRole || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['emr', 'clusters'] });
            setCreateOpen(false);
            setForm({ name: '', releaseLabel: '', logUri: '', serviceRole: '', jobFlowRole: '' });
        },
    });
    const terminateMut = useMutation({
        mutationFn: (id) => terminateCluster(id),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['emr', 'clusters'] });
            setTerminateTarget(null);
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading clusters\u2026" });
    if (error)
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load: ", error.message] });
    const clusters = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "EMR Clusters" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [clusters.length, " cluster", clusters.length !== 1 ? 's' : ''] })] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem' }, children: [_jsxs("select", { value: stateFilter, onChange: e => setStateFilter(e.target.value), style: inputStyle, children: [_jsx("option", { value: "", children: "All states" }), ['RUNNING', 'WAITING', 'STARTING', 'BOOTSTRAPPING', 'TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS'].map(s => (_jsx("option", { value: s, children: s }, s)))] }), _jsx("button", { style: btnStyle, onClick: () => setCreateOpen(true), children: "Create Cluster" })] })] }), clusters.length === 0 ? (_jsx(EmptyState, { title: "No clusters. Create one to get started." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['ID', 'Name', 'State', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: clusters.map(c => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748', cursor: 'pointer' }, onClick: () => navigate(`/aws/emr/${c.id}`), children: [_jsx("td", { style: { ...tdStyle, fontFamily: 'monospace', fontSize: '0.82rem' }, children: c.id }), _jsx("td", { style: tdStyle, children: c.name }), _jsx("td", { style: tdStyle, children: _jsx("span", { style: { color: STATE_COLOR[c.state] ?? '#5f6b7a', fontWeight: 600 }, children: c.state }) }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, onClick: e => e.stopPropagation(), children: _jsx("button", { style: actionBtnStyle, onClick: () => setTerminateTarget(c), children: "Terminate" }) })] }, c.id))) })] })), createOpen && (_jsx("div", { style: overlayStyle, onClick: () => setCreateOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: "Create EMR Cluster" }), [
                            { key: 'name', label: 'Name *', placeholder: 'my-cluster' },
                            { key: 'releaseLabel', label: 'Release Label', placeholder: 'emr-6.10.0' },
                            { key: 'logUri', label: 'Log URI', placeholder: 's3://my-bucket/logs' },
                            { key: 'serviceRole', label: 'Service Role', placeholder: 'EMR_DefaultRole' },
                            { key: 'jobFlowRole', label: 'Job Flow Role', placeholder: 'EMR_EC2_DefaultRole' },
                        ].map(f => (_jsxs("div", { style: fieldStyle, children: [_jsx("label", { style: labelStyle, children: f.label }), _jsx("input", { style: inputStyle, placeholder: f.placeholder, value: form[f.key], onChange: e => setForm(prev => ({ ...prev, [f.key]: e.target.value })) })] }, f.key))), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1rem' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !form.name || createMut.isPending, onClick: () => createMut.mutate(), children: createMut.isPending ? 'Creating…' : 'Create' })] }), createMut.isError && _jsx("div", { style: { color: '#d13212', marginTop: '0.5rem', fontSize: '0.85rem' }, children: createMut.error.message })] }) })), terminateTarget && (_jsx("div", { style: overlayStyle, onClick: () => setTerminateTarget(null), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem', fontWeight: 600 }, children: "Terminate Cluster?" }), _jsxs("p", { style: { color: '#b0bec5', marginBottom: '1.5rem' }, children: ["Terminate ", _jsx("strong", { children: terminateTarget.name }), " (", terminateTarget.id, ")?"] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setTerminateTarget(null), children: "Cancel" }), _jsx("button", { style: { ...btnStyle, background: '#d13212' }, disabled: terminateMut.isPending, onClick: () => terminateMut.mutate(terminateTarget.id), children: terminateMut.isPending ? 'Terminating…' : 'Terminate' })] })] }) }))] }));
}
