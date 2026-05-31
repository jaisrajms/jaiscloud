import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { describeVirtualCluster, listJobRuns, startJobRun, cancelJobRun, } from '../../../api/emroneks';
import { EmptyState } from '../../../components/EmptyState';
const STATE_COLOR = {
    RUNNING: '#037f0c',
    COMPLETED: '#037f0c',
    FAILED: '#d13212',
    CANCELLED: '#5f6b7a',
    SUBMITTED: '#e77600',
    PENDING: '#e77600',
};
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' };
const thStyle = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' };
const tdStyle = { padding: '0.6rem 1rem', verticalAlign: 'middle' };
const btnStyle = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' };
const inputStyle = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const modalStyle = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 400, maxWidth: 520 };
export function EMRContainersDetail() {
    const { id } = useParams();
    const qc = useQueryClient();
    const [submitOpen, setSubmitOpen] = useState(false);
    const [cancelTarget, setCancelTarget] = useState(null);
    const [form, setForm] = useState({ name: '', releaseLabel: '', executionRoleArn: '' });
    const { data: vc, isLoading, error } = useQuery({
        queryKey: ['emrc', 'vc', id],
        queryFn: () => describeVirtualCluster(id),
        enabled: !!id,
    });
    const { data: jobsData } = useQuery({
        queryKey: ['emrc', 'jobs', id],
        queryFn: () => listJobRuns(id),
        enabled: !!id,
    });
    const submitMut = useMutation({
        mutationFn: () => startJobRun(id, { name: form.name, releaseLabel: form.releaseLabel || undefined, executionRoleArn: form.executionRoleArn || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['emrc', 'jobs', id] });
            setSubmitOpen(false);
            setForm({ name: '', releaseLabel: '', executionRoleArn: '' });
        },
    });
    const cancelMut = useMutation({
        mutationFn: (jobId) => cancelJobRun(id, jobId),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['emrc', 'jobs', id] });
            setCancelTarget(null);
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading virtual cluster\u2026" });
    if (error || !vc)
        return _jsx("div", { style: { padding: '2rem', color: '#d13212' }, children: "Failed to load virtual cluster." });
    const jobs = jobsData?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { marginBottom: '1.5rem' }, children: [_jsx(Link, { to: "/aws/emr-containers", style: { color: '#0073bb', textDecoration: 'none', fontSize: '0.85rem' }, children: "\u2190 Virtual Clusters" }), _jsx("h2", { style: { margin: '0.5rem 0 0', fontWeight: 600, fontSize: '1.4rem' }, children: vc.name }), _jsx("span", { style: { color: STATE_COLOR[vc.state] ?? '#5f6b7a', fontWeight: 600 }, children: vc.state }), vc.eksCluster && _jsxs("span", { style: { color: '#b0bec5', marginLeft: '0.75rem', fontSize: '0.85rem' }, children: ["EKS: ", vc.eksCluster, " / ", vc.namespace] })] }), _jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }, children: [_jsxs("h3", { style: { margin: 0, fontWeight: 600, fontSize: '1rem' }, children: ["Job Runs (", jobs.length, ")"] }), _jsx("button", { style: btnStyle, onClick: () => setSubmitOpen(true), children: "Submit Job Run" })] }), jobs.length === 0 ? (_jsx(EmptyState, { title: "No job runs yet. Submit a job to get started." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['ID', 'Name', 'State', 'Release Label', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: jobs.map(j => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: { ...tdStyle, fontFamily: 'monospace', fontSize: '0.82rem' }, children: j.id }), _jsx("td", { style: tdStyle, children: j.name }), _jsx("td", { style: tdStyle, children: _jsx("span", { style: { color: STATE_COLOR[j.state] ?? '#5f6b7a', fontWeight: 600 }, children: j.state }) }), _jsx("td", { style: tdStyle, children: j.releaseLabel || '—' }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, children: ['RUNNING', 'SUBMITTED', 'PENDING'].includes(j.state) && (_jsx("button", { style: { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setCancelTarget(j), children: "Cancel" })) })] }, j.id))) })] })), submitOpen && (_jsx("div", { style: overlayStyle, onClick: () => setSubmitOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: "Submit Job Run" }), [
                            { key: 'name', label: 'Job Name *', placeholder: 'my-spark-job' },
                            { key: 'releaseLabel', label: 'Release Label', placeholder: 'emr-6.10.0-latest' },
                            { key: 'executionRoleArn', label: 'Execution Role ARN', placeholder: 'arn:aws:iam::…:role/EMRRole' },
                        ].map(f => (_jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: f.label }), _jsx("input", { style: inputStyle, placeholder: f.placeholder, value: form[f.key], onChange: e => setForm(prev => ({ ...prev, [f.key]: e.target.value })) })] }, f.key))), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1rem' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setSubmitOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !form.name || submitMut.isPending, onClick: () => submitMut.mutate(), children: submitMut.isPending ? 'Submitting…' : 'Submit' })] })] }) })), cancelTarget && (_jsx("div", { style: overlayStyle, onClick: () => setCancelTarget(null), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem', fontWeight: 600 }, children: "Cancel Job Run?" }), _jsxs("p", { style: { color: '#b0bec5', marginBottom: '1.5rem' }, children: ["Cancel job ", _jsx("strong", { children: cancelTarget.name }), "?"] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setCancelTarget(null), children: "Back" }), _jsx("button", { style: { ...btnStyle, background: '#d13212' }, disabled: cancelMut.isPending, onClick: () => cancelMut.mutate(cancelTarget.id), children: cancelMut.isPending ? 'Cancelling…' : 'Cancel Job' })] })] }) }))] }));
}
