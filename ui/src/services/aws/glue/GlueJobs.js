import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listJobs, createJob, deleteJob, startJobRun, listJobRuns, } from '../../../api/glue';
import { EmptyState } from '../../../components/EmptyState';
const STATE_COLOR = {
    SUCCEEDED: '#037f0c',
    FAILED: '#d13212',
    RUNNING: '#0073bb',
    STARTING: '#e77600',
    STOPPING: '#8a6116',
    STOPPED: '#5f6b7a',
    TIMEOUT: '#d13212',
};
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' };
const thStyle = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' };
const tdStyle = { padding: '0.6rem 1rem', verticalAlign: 'middle' };
const btnStyle = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' };
const inputStyle = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const modalStyle = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 420, maxWidth: 560 };
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
export function GlueJobs() {
    const qc = useQueryClient();
    const [selectedJob, setSelectedJob] = useState(null);
    const [createOpen, setCreateOpen] = useState(false);
    const [deleteTarget, setDeleteTarget] = useState(null);
    const [form, setForm] = useState({ name: '', role: '', command: '' });
    const { data: jobsData, isLoading } = useQuery({
        queryKey: ['glue', 'jobs'],
        queryFn: () => listJobs(),
    });
    const { data: runsData } = useQuery({
        queryKey: ['glue', 'runs', selectedJob?.name],
        queryFn: () => listJobRuns(selectedJob.name),
        enabled: !!selectedJob,
    });
    const createMut = useMutation({
        mutationFn: () => createJob({ name: form.name, role: form.role || undefined, command: form.command || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['glue', 'jobs'] });
            setCreateOpen(false);
            setForm({ name: '', role: '', command: '' });
        },
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteJob(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['glue', 'jobs'] });
            if (deleteTarget?.name === selectedJob?.name)
                setSelectedJob(null);
            setDeleteTarget(null);
        },
    });
    const runMut = useMutation({
        mutationFn: (name) => startJobRun(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['glue', 'runs', selectedJob?.name] });
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading jobs\u2026" });
    const jobs = jobsData?.items ?? [];
    const runs = runsData?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "Glue Jobs" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [jobs.length, " job", jobs.length !== 1 ? 's' : ''] })] }), _jsx("button", { style: btnStyle, onClick: () => setCreateOpen(true), children: "Create Job" })] }), _jsxs("div", { style: { display: 'grid', gridTemplateColumns: selectedJob ? '1fr 1fr' : '1fr', gap: '1.5rem' }, children: [_jsx("div", { children: jobs.length === 0 ? (_jsx(EmptyState, { title: "No jobs. Create a Glue ETL job to get started." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Name', 'Role', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: jobs.map(j => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748', cursor: 'pointer', background: selectedJob?.name === j.name ? '#1e2d3d' : 'transparent' }, onClick: () => setSelectedJob(j), children: [_jsx("td", { style: { ...tdStyle, fontWeight: 600 }, children: j.name }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }, children: j.role || '—' }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, onClick: e => e.stopPropagation(), children: _jsxs("div", { style: { display: 'flex', gap: '0.4rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, padding: '0.25rem 0.6rem', fontSize: '0.8rem' }, onClick: () => runMut.mutate(j.name), disabled: runMut.isPending, children: "Run" }), _jsx("button", { style: { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setDeleteTarget(j), children: "Delete" })] }) })] }, j.name))) })] })) }), selectedJob && (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }, children: [_jsxs("h3", { style: { margin: 0, fontWeight: 600, fontSize: '1rem' }, children: ["Runs for ", _jsx("em", { children: selectedJob.name }), " (", runs.length, ")"] }), _jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0', padding: '0.3rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setSelectedJob(null), children: "\u2715" })] }), runs.length === 0 ? (_jsx(EmptyState, { title: "No runs yet. Click Run to start a job run." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Run ID', 'State', 'Started', 'Completed'].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: runs.map((r) => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: { ...tdStyle, fontFamily: 'monospace', fontSize: '0.82rem' }, children: r.id }), _jsx("td", { style: tdStyle, children: _jsx("span", { style: { color: STATE_COLOR[r.state] ?? '#5f6b7a', fontWeight: 600 }, children: r.state }) }), _jsx("td", { style: { ...tdStyle, fontSize: '0.82rem', color: '#b0bec5' }, children: fmtDate(r.startedOn) }), _jsx("td", { style: { ...tdStyle, fontSize: '0.82rem', color: '#b0bec5' }, children: fmtDate(r.completedOn) })] }, r.id))) })] }))] }))] }), createOpen && (_jsx("div", { style: overlayStyle, onClick: () => setCreateOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: "Create Glue Job" }), [
                            { key: 'name', label: 'Name *', placeholder: 'my-etl-job' },
                            { key: 'role', label: 'IAM Role', placeholder: 'AWSGlueServiceRole' },
                            { key: 'command', label: 'Command Script', placeholder: 'glueetl' },
                        ].map(f => (_jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: f.label }), _jsx("input", { style: inputStyle, placeholder: f.placeholder, value: form[f.key], onChange: e => setForm(p => ({ ...p, [f.key]: e.target.value })) })] }, f.key))), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !form.name || createMut.isPending, onClick: () => createMut.mutate(), children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), deleteTarget && (_jsx("div", { style: overlayStyle, onClick: () => setDeleteTarget(null), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem', fontWeight: 600 }, children: "Delete Job?" }), _jsxs("p", { style: { color: '#b0bec5', marginBottom: '1.5rem' }, children: ["Delete job ", _jsx("strong", { children: deleteTarget.name }), "?"] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setDeleteTarget(null), children: "Cancel" }), _jsx("button", { style: { ...btnStyle, background: '#d13212' }, disabled: deleteMut.isPending, onClick: () => deleteMut.mutate(deleteTarget.name), children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
