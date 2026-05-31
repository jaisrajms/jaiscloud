import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { describeCluster, listSteps, addSteps, cancelStep } from '../../../api/emr';
import { EmptyState } from '../../../components/EmptyState';
const STATE_COLOR = {
    RUNNING: '#037f0c',
    WAITING: '#0073bb',
    STARTING: '#e77600',
    COMPLETED: '#037f0c',
    FAILED: '#d13212',
    CANCELLED: '#5f6b7a',
    PENDING: '#e77600',
    INTERRUPTED: '#8a6116',
};
const cardStyle = { background: '#1a2332', borderRadius: 8, padding: '1.5rem', marginBottom: '1.5rem' };
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' };
const thStyle = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' };
const tdStyle = { padding: '0.6rem 1rem', verticalAlign: 'middle' };
const btnStyle = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' };
const inputStyle = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const modalStyle = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 420, maxWidth: 560 };
export function EMRDetail() {
    const { id } = useParams();
    const qc = useQueryClient();
    const [addStepOpen, setAddStepOpen] = useState(false);
    const [stepName, setStepName] = useState('');
    const [cancelTarget, setCancelTarget] = useState(null);
    const { data: cluster, isLoading, error } = useQuery({
        queryKey: ['emr', 'cluster', id],
        queryFn: () => describeCluster(id),
        enabled: !!id,
    });
    const { data: stepsData } = useQuery({
        queryKey: ['emr', 'steps', id],
        queryFn: () => listSteps(id),
        enabled: !!id,
    });
    const addMut = useMutation({
        mutationFn: () => addSteps(id, [{ Name: stepName, ActionOnFailure: 'CONTINUE', HadoopJarStep: { Jar: 'command-runner.jar', Args: ['echo', stepName] } }]),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['emr', 'steps', id] });
            setAddStepOpen(false);
            setStepName('');
        },
    });
    const cancelMut = useMutation({
        mutationFn: (stepId) => cancelStep(id, stepId),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['emr', 'steps', id] });
            setCancelTarget(null);
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading cluster\u2026" });
    if (error || !cluster)
        return _jsx("div", { style: { padding: '2rem', color: '#d13212' }, children: "Failed to load cluster." });
    const steps = stepsData?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { marginBottom: '1.5rem' }, children: [_jsx(Link, { to: "/aws/emr", style: { color: '#0073bb', textDecoration: 'none', fontSize: '0.85rem' }, children: "\u2190 Clusters" }), _jsx("h2", { style: { margin: '0.5rem 0 0', fontWeight: 600, fontSize: '1.4rem' }, children: cluster.name }), _jsx("span", { style: { color: STATE_COLOR[cluster.state] ?? '#5f6b7a', fontWeight: 600 }, children: cluster.state }), cluster.stateChangeReason && _jsxs("span", { style: { color: '#b0bec5', marginLeft: '0.5rem', fontSize: '0.85rem' }, children: ["\u2014 ", cluster.stateChangeReason] })] }), _jsxs("div", { style: cardStyle, children: [_jsx("h3", { style: { margin: '0 0 1rem', fontWeight: 600, fontSize: '1rem' }, children: "Details" }), _jsx("div", { style: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.75rem', fontSize: '0.85rem' }, children: [
                            ['ID', cluster.id],
                            ['ARN', cluster.arn || '—'],
                            ['Release Label', cluster.releaseLabel || '—'],
                            ['Log URI', cluster.logUri || '—'],
                            ['Auto Terminate', String(cluster.autoTerminate)],
                            ['Termination Protected', String(cluster.terminationProtected)],
                        ].map(([k, v]) => (_jsxs("div", { children: [_jsx("div", { style: { color: '#b0bec5', marginBottom: '0.2rem' }, children: k }), _jsx("div", { style: { fontFamily: 'monospace', wordBreak: 'break-all' }, children: v })] }, k))) }), cluster.applications.length > 0 && (_jsxs("div", { style: { marginTop: '1rem' }, children: [_jsx("div", { style: { color: '#b0bec5', fontSize: '0.8rem', marginBottom: '0.3rem' }, children: "Applications" }), _jsx("div", { style: { display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }, children: cluster.applications.map(a => (_jsx("span", { style: { background: '#2d3748', borderRadius: 4, padding: '0.2rem 0.5rem', fontSize: '0.8rem' }, children: a }, a))) })] }))] }), _jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }, children: [_jsxs("h3", { style: { margin: 0, fontWeight: 600, fontSize: '1rem' }, children: ["Steps (", steps.length, ")"] }), _jsx("button", { style: btnStyle, onClick: () => setAddStepOpen(true), children: "Add Step" })] }), steps.length === 0 ? (_jsx(EmptyState, { title: "No steps. Add a step to run work on this cluster." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['ID', 'Name', 'State', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: steps.map(s => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: { ...tdStyle, fontFamily: 'monospace', fontSize: '0.82rem' }, children: s.id }), _jsx("td", { style: tdStyle, children: s.name }), _jsx("td", { style: tdStyle, children: _jsx("span", { style: { color: STATE_COLOR[s.state] ?? '#5f6b7a', fontWeight: 600 }, children: s.state }) }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, children: ['RUNNING', 'PENDING'].includes(s.state) && (_jsx("button", { style: { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setCancelTarget(s), children: "Cancel" })) })] }, s.id))) })] })), addStepOpen && (_jsx("div", { style: overlayStyle, onClick: () => setAddStepOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: "Add Step" }), _jsxs("div", { style: { marginBottom: '1rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5', display: 'block', marginBottom: '0.3rem' }, children: "Step Name *" }), _jsx("input", { style: inputStyle, placeholder: "my-step", value: stepName, onChange: e => setStepName(e.target.value) })] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1rem' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setAddStepOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !stepName || addMut.isPending, onClick: () => addMut.mutate(), children: addMut.isPending ? 'Adding…' : 'Add Step' })] })] }) })), cancelTarget && (_jsx("div", { style: overlayStyle, onClick: () => setCancelTarget(null), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem', fontWeight: 600 }, children: "Cancel Step?" }), _jsxs("p", { style: { color: '#b0bec5', marginBottom: '1.5rem' }, children: ["Cancel step ", _jsx("strong", { children: cancelTarget.name }), "?"] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setCancelTarget(null), children: "Back" }), _jsx("button", { style: { ...btnStyle, background: '#d13212' }, disabled: cancelMut.isPending, onClick: () => cancelMut.mutate(cancelTarget.id), children: cancelMut.isPending ? 'Cancelling…' : 'Cancel Step' })] })] }) }))] }));
}
