import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listRestAPIs, createRestAPI, deleteRestAPI, listResources, listStages, listDeployments, createDeployment, } from '../../../api/apigw';
import { EmptyState } from '../../../components/EmptyState';
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' };
const thStyle = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' };
const tdStyle = { padding: '0.6rem 1rem', verticalAlign: 'middle' };
const btnStyle = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' };
const inputStyle = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const modalStyle = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 440, maxWidth: 580 };
export function APIGatewayAPIs() {
    const qc = useQueryClient();
    const [selectedAPI, setSelectedAPI] = useState(null);
    const [activeTab, setActiveTab] = useState('resources');
    const [createOpen, setCreateOpen] = useState(false);
    const [deleteTarget, setDeleteTarget] = useState(null);
    const [deployOpen, setDeployOpen] = useState(false);
    const [form, setForm] = useState({ name: '', description: '' });
    const [deployForm, setDeployForm] = useState({ stageName: '', description: '' });
    const { data, isLoading } = useQuery({
        queryKey: ['apigw', 'apis'],
        queryFn: () => listRestAPIs(),
    });
    const { data: resourcesData } = useQuery({
        queryKey: ['apigw', 'resources', selectedAPI?.id],
        queryFn: () => listResources(selectedAPI.id),
        enabled: !!selectedAPI && activeTab === 'resources',
    });
    const { data: stagesData } = useQuery({
        queryKey: ['apigw', 'stages', selectedAPI?.id],
        queryFn: () => listStages(selectedAPI.id),
        enabled: !!selectedAPI && activeTab === 'stages',
    });
    const { data: deploymentsData } = useQuery({
        queryKey: ['apigw', 'deployments', selectedAPI?.id],
        queryFn: () => listDeployments(selectedAPI.id),
        enabled: !!selectedAPI && activeTab === 'deployments',
    });
    const createMut = useMutation({
        mutationFn: () => createRestAPI({ name: form.name, description: form.description || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['apigw', 'apis'] });
            setCreateOpen(false);
            setForm({ name: '', description: '' });
        },
    });
    const deleteMut = useMutation({
        mutationFn: (id) => deleteRestAPI(id),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['apigw', 'apis'] });
            if (deleteTarget?.id === selectedAPI?.id)
                setSelectedAPI(null);
            setDeleteTarget(null);
        },
    });
    const deployMut = useMutation({
        mutationFn: () => createDeployment(selectedAPI.id, { stageName: deployForm.stageName || undefined, description: deployForm.description || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['apigw', 'deployments', selectedAPI?.id] });
            void qc.invalidateQueries({ queryKey: ['apigw', 'stages', selectedAPI?.id] });
            setDeployOpen(false);
            setDeployForm({ stageName: '', description: '' });
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading APIs\u2026" });
    const apis = data?.items ?? [];
    const resources = resourcesData?.items ?? [];
    const stages = stagesData?.items ?? [];
    const deployments = deploymentsData?.items ?? [];
    const tabBtnStyle = (t) => ({
        padding: '0.4rem 1rem',
        fontSize: '0.82rem',
        border: 'none',
        borderRadius: 4,
        cursor: 'pointer',
        background: activeTab === t ? '#0073bb' : '#2d3748',
        color: activeTab === t ? '#fff' : '#b0bec5',
    });
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "API Gateway" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [apis.length, " REST API", apis.length !== 1 ? 's' : ''] })] }), _jsx("button", { style: btnStyle, onClick: () => setCreateOpen(true), children: "Create API" })] }), _jsxs("div", { style: { display: 'grid', gridTemplateColumns: selectedAPI ? '1fr 1.4fr' : '1fr', gap: '1.5rem' }, children: [_jsx("div", { children: apis.length === 0 ? (_jsx(EmptyState, { title: "No REST APIs. Create one to get started." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Name', 'ID', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: apis.map(api => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748', cursor: 'pointer', background: selectedAPI?.id === api.id ? '#1e2d3d' : 'transparent' }, onClick: () => setSelectedAPI(api), children: [_jsx("td", { style: { ...tdStyle, fontWeight: 600 }, children: api.name }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontFamily: 'monospace', fontSize: '0.82rem' }, children: api.id }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, onClick: e => e.stopPropagation(), children: _jsx("button", { style: { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setDeleteTarget(api), children: "Delete" }) })] }, api.id))) })] })) }), selectedAPI && (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }, children: [_jsx("h3", { style: { margin: 0, fontWeight: 600, fontSize: '1rem' }, children: selectedAPI.name }), _jsxs("div", { style: { display: 'flex', gap: '0.5rem' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0', padding: '0.3rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setSelectedAPI(null), children: "\u2715" }), activeTab === 'deployments' && (_jsx("button", { style: { ...btnStyle, fontSize: '0.8rem', padding: '0.3rem 0.75rem' }, onClick: () => setDeployOpen(true), children: "Deploy" }))] })] }), _jsx("div", { style: { display: 'flex', gap: '0.5rem', marginBottom: '1rem' }, children: ['resources', 'stages', 'deployments'].map(t => (_jsx("button", { style: tabBtnStyle(t), onClick: () => setActiveTab(t), children: t.charAt(0).toUpperCase() + t.slice(1) }, t))) }), activeTab === 'resources' && (resources.length === 0 ? _jsx(EmptyState, { title: "No resources defined." }) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Path', 'ID', 'Parent'].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: resources.map(r => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: { ...tdStyle, fontWeight: 600, fontFamily: 'monospace' }, children: r.path }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.82rem', fontFamily: 'monospace' }, children: r.id }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.82rem', fontFamily: 'monospace' }, children: r.parentId || '—' })] }, r.id))) })] }))), activeTab === 'stages' && (stages.length === 0 ? _jsx(EmptyState, { title: "No stages. Deploy to create a stage." }) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Stage', 'Deployment ID', 'Description'].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: stages.map(s => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: { ...tdStyle, fontWeight: 600 }, children: s.name }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.82rem', fontFamily: 'monospace' }, children: s.deploymentId || '—' }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }, children: s.description || '—' })] }, s.name))) })] }))), activeTab === 'deployments' && (deployments.length === 0 ? _jsx(EmptyState, { title: "No deployments yet." }) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['ID', 'Description', 'Created'].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: deployments.map(d => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: { ...tdStyle, fontFamily: 'monospace', fontSize: '0.82rem' }, children: d.id }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }, children: d.description || '—' }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.82rem' }, children: d.createdDate || '—' })] }, d.id))) })] })))] }))] }), createOpen && (_jsx("div", { style: overlayStyle, onClick: () => setCreateOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: "Create REST API" }), [
                            { key: 'name', label: 'Name *', placeholder: 'my-api' },
                            { key: 'description', label: 'Description', placeholder: '' },
                        ].map(f => (_jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: f.label }), _jsx("input", { style: inputStyle, placeholder: f.placeholder, value: form[f.key], onChange: e => setForm(p => ({ ...p, [f.key]: e.target.value })) })] }, f.key))), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !form.name || createMut.isPending, onClick: () => createMut.mutate(), children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), deleteTarget && (_jsx("div", { style: overlayStyle, onClick: () => setDeleteTarget(null), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem', fontWeight: 600 }, children: "Delete REST API?" }), _jsxs("p", { style: { color: '#b0bec5', marginBottom: '1.5rem' }, children: ["Delete API ", _jsx("strong", { children: deleteTarget.name }), "?"] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setDeleteTarget(null), children: "Cancel" }), _jsx("button", { style: { ...btnStyle, background: '#d13212' }, disabled: deleteMut.isPending, onClick: () => deleteMut.mutate(deleteTarget.id), children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) })), deployOpen && selectedAPI && (_jsx("div", { style: overlayStyle, onClick: () => setDeployOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsxs("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: ["Deploy ", selectedAPI.name] }), [
                            { key: 'stageName', label: 'Stage Name', placeholder: 'prod' },
                            { key: 'description', label: 'Description', placeholder: '' },
                        ].map(f => (_jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: f.label }), _jsx("input", { style: inputStyle, placeholder: f.placeholder, value: deployForm[f.key], onChange: e => setDeployForm(p => ({ ...p, [f.key]: e.target.value })) })] }, f.key))), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setDeployOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: deployMut.isPending, onClick: () => deployMut.mutate(), children: deployMut.isPending ? 'Deploying…' : 'Deploy' })] })] }) }))] }));
}
