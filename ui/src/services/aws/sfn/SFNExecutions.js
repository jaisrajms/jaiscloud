import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useSearchParams, useNavigate } from 'react-router-dom';
import { listExecutions, startExecution, stopExecution, getExecutionHistory, } from '../../../api/sfn';
import { EmptyState } from '../../../components/EmptyState';
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' };
const thStyle = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' };
const tdStyle = { padding: '0.6rem 1rem', verticalAlign: 'middle' };
const btnStyle = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' };
const inputStyle = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const modalStyle = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 440, maxWidth: 620 };
const statusColor = (s) => {
    if (s === 'RUNNING')
        return '#3498db';
    if (s === 'SUCCEEDED')
        return '#2ecc71';
    if (s === 'FAILED' || s === 'TIMED_OUT' || s === 'ABORTED')
        return '#e74c3c';
    return '#b0bec5';
};
export function SFNExecutions() {
    const qc = useQueryClient();
    const navigate = useNavigate();
    const [searchParams] = useSearchParams();
    const smArn = searchParams.get('arn') ?? '';
    const [startOpen, setStartOpen] = useState(false);
    const [historyExec, setHistoryExec] = useState(null);
    const [form, setForm] = useState({ name: '', input: '{}' });
    const { data, isLoading } = useQuery({
        queryKey: ['sfn', 'executions', smArn],
        queryFn: () => listExecutions(smArn),
        enabled: !!smArn,
    });
    const { data: historyData } = useQuery({
        queryKey: ['sfn', 'history', historyExec?.arn],
        queryFn: () => getExecutionHistory(historyExec.arn),
        enabled: !!historyExec,
    });
    const startMut = useMutation({
        mutationFn: () => startExecution(smArn, { name: form.name || undefined, input: form.input }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['sfn', 'executions', smArn] });
            setStartOpen(false);
            setForm({ name: '', input: '{}' });
        },
    });
    const stopMut = useMutation({
        mutationFn: (arn) => stopExecution(arn),
        onSuccess: () => { void qc.invalidateQueries({ queryKey: ['sfn', 'executions', smArn] }); },
    });
    if (!smArn) {
        return (_jsxs("div", { style: { padding: '2rem' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0', marginBottom: '1rem' }, onClick: () => navigate('../state-machines'), children: "\u2190 State Machines" }), _jsx(EmptyState, { title: "No state machine selected." })] }));
    }
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading executions\u2026" });
    const executions = data?.items ?? [];
    const events = historyData?.events ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0', padding: '0.3rem 0.75rem', fontSize: '0.82rem', marginBottom: '0.5rem' }, onClick: () => navigate('../state-machines'), children: "\u2190 State Machines" }), _jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "Executions" }), _jsx("span", { style: { fontSize: '0.8rem', color: '#5f6b7a', fontFamily: 'monospace' }, children: smArn })] }), _jsx("button", { style: btnStyle, onClick: () => setStartOpen(true), children: "Start Execution" })] }), _jsxs("div", { style: { display: 'grid', gridTemplateColumns: historyExec ? '1fr 1fr' : '1fr', gap: '1.5rem' }, children: [_jsx("div", { children: executions.length === 0 ? (_jsx(EmptyState, { title: "No executions. Start one to run this state machine." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Name', 'Status', 'Started', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: executions.map(exec => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748', cursor: 'pointer', background: historyExec?.arn === exec.arn ? '#1e2d3d' : 'transparent' }, onClick: () => setHistoryExec(exec), children: [_jsx("td", { style: { ...tdStyle, fontWeight: 600, maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }, children: exec.name }), _jsx("td", { style: tdStyle, children: _jsx("span", { style: { color: statusColor(exec.status), fontSize: '0.8rem', fontWeight: 600 }, children: exec.status }) }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.82rem' }, children: exec.startDate ? new Date(exec.startDate * 1000).toLocaleString() : '—' }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, onClick: e => e.stopPropagation(), children: exec.status === 'RUNNING' && (_jsx("button", { style: { ...btnStyle, background: 'transparent', color: '#e87600', border: '1px solid #e87600', padding: '0.25rem 0.6rem', fontSize: '0.78rem' }, onClick: () => stopMut.mutate(exec.arn), children: "Stop" })) })] }, exec.arn))) })] })) }), historyExec && (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }, children: [_jsxs("h3", { style: { margin: 0, fontWeight: 600, fontSize: '1rem' }, children: ["History (", events.length, ")"] }), _jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0', padding: '0.3rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setHistoryExec(null), children: "\u2715" })] }), events.length === 0 ? _jsx(EmptyState, { title: "No events." }) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['ID', 'Type', 'Timestamp'].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: events.map(ev => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.82rem' }, children: ev.id }), _jsx("td", { style: { ...tdStyle, fontSize: '0.85rem' }, children: ev.type }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.82rem' }, children: ev.timestamp ? new Date(ev.timestamp * 1000).toLocaleString() : '—' })] }, ev.id))) })] }))] }))] }), startOpen && (_jsx("div", { style: overlayStyle, onClick: () => setStartOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: "Start Execution" }), _jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: "Execution Name (optional)" }), _jsx("input", { style: inputStyle, placeholder: "my-execution", value: form.name, onChange: e => setForm(p => ({ ...p, name: e.target.value })) })] }), _jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1.5rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: "Input (JSON)" }), _jsx("textarea", { style: { ...inputStyle, minHeight: 100, resize: 'vertical', fontFamily: 'monospace' }, value: form.input, onChange: e => setForm(p => ({ ...p, input: e.target.value })) })] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setStartOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: startMut.isPending, onClick: () => startMut.mutate(), children: startMut.isPending ? 'Starting…' : 'Start' })] })] }) }))] }));
}
