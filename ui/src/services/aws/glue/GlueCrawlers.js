import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listCrawlers, createCrawler, deleteCrawler, startCrawler, } from '../../../api/glue';
import { EmptyState } from '../../../components/EmptyState';
const STATE_COLOR = {
    READY: '#037f0c',
    RUNNING: '#0073bb',
    STOPPING: '#8a6116',
};
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' };
const thStyle = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' };
const tdStyle = { padding: '0.6rem 1rem', verticalAlign: 'middle' };
const btnStyle = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' };
const inputStyle = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const modalStyle = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 420, maxWidth: 560 };
export function GlueCrawlers() {
    const qc = useQueryClient();
    const [createOpen, setCreateOpen] = useState(false);
    const [deleteTarget, setDeleteTarget] = useState(null);
    const [form, setForm] = useState({ name: '', role: '', databaseName: '', s3Targets: '' });
    const { data, isLoading } = useQuery({
        queryKey: ['glue', 'crawlers'],
        queryFn: () => listCrawlers(),
    });
    const createMut = useMutation({
        mutationFn: () => createCrawler({
            name: form.name,
            role: form.role || undefined,
            databaseName: form.databaseName || undefined,
            s3Targets: form.s3Targets ? form.s3Targets.split(',').map(s => s.trim()).filter(Boolean) : undefined,
        }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['glue', 'crawlers'] });
            setCreateOpen(false);
            setForm({ name: '', role: '', databaseName: '', s3Targets: '' });
        },
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteCrawler(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['glue', 'crawlers'] });
            setDeleteTarget(null);
        },
    });
    const startMut = useMutation({
        mutationFn: (name) => startCrawler(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['glue', 'crawlers'] });
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading crawlers\u2026" });
    const crawlers = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "Glue Crawlers" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [crawlers.length, " crawler", crawlers.length !== 1 ? 's' : ''] })] }), _jsx("button", { style: btnStyle, onClick: () => setCreateOpen(true), children: "Create Crawler" })] }), crawlers.length === 0 ? (_jsx(EmptyState, { title: "No crawlers. Create one to discover and catalog data from S3." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Name', 'Role', 'State', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: crawlers.map(c => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: { ...tdStyle, fontWeight: 600 }, children: c.name }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }, children: c.role || '—' }), _jsx("td", { style: tdStyle, children: _jsx("span", { style: { color: STATE_COLOR[c.state ?? ''] ?? '#5f6b7a', fontWeight: 600 }, children: c.state || '—' }) }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, children: _jsxs("div", { style: { display: 'flex', gap: '0.4rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, padding: '0.25rem 0.6rem', fontSize: '0.8rem' }, disabled: c.state === 'RUNNING' || startMut.isPending, onClick: () => startMut.mutate(c.name), children: "Start" }), _jsx("button", { style: { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setDeleteTarget(c), children: "Delete" })] }) })] }, c.name))) })] })), createOpen && (_jsx("div", { style: overlayStyle, onClick: () => setCreateOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: "Create Crawler" }), [
                            { key: 'name', label: 'Name *', placeholder: 'my-crawler' },
                            { key: 'role', label: 'IAM Role', placeholder: 'AWSGlueServiceRole' },
                            { key: 'databaseName', label: 'Target Database', placeholder: 'my_database' },
                            { key: 's3Targets', label: 'S3 Paths (comma-separated)', placeholder: 's3://bucket/prefix' },
                        ].map(f => (_jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: f.label }), _jsx("input", { style: inputStyle, placeholder: f.placeholder, value: form[f.key], onChange: e => setForm(p => ({ ...p, [f.key]: e.target.value })) })] }, f.key))), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !form.name || createMut.isPending, onClick: () => createMut.mutate(), children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), deleteTarget && (_jsx("div", { style: overlayStyle, onClick: () => setDeleteTarget(null), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem', fontWeight: 600 }, children: "Delete Crawler?" }), _jsxs("p", { style: { color: '#b0bec5', marginBottom: '1.5rem' }, children: ["Delete crawler ", _jsx("strong", { children: deleteTarget.name }), "?"] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setDeleteTarget(null), children: "Cancel" }), _jsx("button", { style: { ...btnStyle, background: '#d13212' }, disabled: deleteMut.isPending, onClick: () => deleteMut.mutate(deleteTarget.name), children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
