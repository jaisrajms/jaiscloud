import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listAlarms, putAlarm, deleteAlarm, setAlarmState, enableAlarmActions, disableAlarmActions, } from '../../../api/cloudwatch';
import { EmptyState } from '../../../components/EmptyState';
const STATE_COLOR = {
    OK: '#037f0c',
    ALARM: '#d13212',
    INSUFFICIENT_DATA: '#8a6116',
};
export function CloudWatchAlarms() {
    const [createOpen, setCreateOpen] = useState(false);
    const [stateFilter, setStateFilter] = useState('');
    const [deleteTarget, setDeleteTarget] = useState(null);
    const [stateTarget, setStateTarget] = useState(null);
    const [newStateValue, setNewStateValue] = useState('OK');
    const [newStateReason, setNewStateReason] = useState('');
    // Create alarm form
    const [form, setForm] = useState({
        alarmName: '',
        namespace: '',
        metricName: '',
        statistic: 'Average',
        period: 60,
        threshold: 0,
        comparisonOperator: 'GreaterThanThreshold',
        evaluationPeriods: 1,
    });
    const qc = useQueryClient();
    const { data, isLoading, error } = useQuery({
        queryKey: ['cloudwatch', 'alarms', stateFilter],
        queryFn: () => listAlarms(stateFilter ? { stateValue: stateFilter } : undefined),
    });
    const createMut = useMutation({
        mutationFn: () => putAlarm({ ...form, actionsEnabled: true }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] });
            setCreateOpen(false);
            setForm({ alarmName: '', namespace: '', metricName: '', statistic: 'Average', period: 60, threshold: 0, comparisonOperator: 'GreaterThanThreshold', evaluationPeriods: 1 });
        },
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteAlarm(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] });
            setDeleteTarget(null);
        },
    });
    const stateMut = useMutation({
        mutationFn: () => setAlarmState({ alarmName: stateTarget.alarmName, stateValue: newStateValue, stateReason: newStateReason }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] });
            setStateTarget(null);
        },
    });
    const enableMut = useMutation({
        mutationFn: (name) => enableAlarmActions([name]),
        onSuccess: () => void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] }),
    });
    const disableMut = useMutation({
        mutationFn: (name) => disableAlarmActions([name]),
        onSuccess: () => void qc.invalidateQueries({ queryKey: ['cloudwatch', 'alarms'] }),
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading alarms\u2026" });
    if (error)
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load: ", error.message] });
    const alarms = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "CloudWatch Alarms" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [alarms.length, " alarm", alarms.length !== 1 ? 's' : ''] })] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem' }, children: [_jsxs("select", { value: stateFilter, onChange: e => setStateFilter(e.target.value), style: selectStyle, children: [_jsx("option", { value: "", children: "All states" }), _jsx("option", { value: "OK", children: "OK" }), _jsx("option", { value: "ALARM", children: "ALARM" }), _jsx("option", { value: "INSUFFICIENT_DATA", children: "INSUFFICIENT_DATA" })] }), _jsx("button", { onClick: () => setCreateOpen(true), style: primaryBtnStyle, children: "Create Alarm" })] })] }), alarms.length === 0 ? (_jsx(EmptyState, { title: "No alarms found." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Name', 'Metric', 'State', 'Actions', ''].map(h => (_jsx("th", { style: thStyle, children: h }, h))) }) }), _jsx("tbody", { children: alarms.map(a => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: tdStyle, children: a.alarmName }), _jsxs("td", { style: tdStyle, children: [a.namespace, "/", a.metricName] }), _jsx("td", { style: tdStyle, children: _jsx("span", { style: { color: STATE_COLOR[a.stateValue ?? ''] ?? '#c9cdd4', fontWeight: 500 }, children: a.stateValue ?? '—' }) }), _jsx("td", { style: tdStyle, children: _jsx("span", { style: { color: a.actionsEnabled ? '#037f0c' : '#8892a4', fontSize: '0.82em' }, children: a.actionsEnabled ? 'enabled' : 'disabled' }) }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, children: _jsxs("div", { style: { display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => { setStateTarget(a); setNewStateValue('OK'); setNewStateReason(''); }, style: actionBtnStyle, children: "Set State" }), a.actionsEnabled
                                                ? _jsx("button", { onClick: () => disableMut.mutate(a.alarmName), style: actionBtnStyle, children: "Disable" })
                                                : _jsx("button", { onClick: () => enableMut.mutate(a.alarmName), style: actionBtnStyle, children: "Enable" }), _jsx("button", { onClick: () => setDeleteTarget(a), style: { ...actionBtnStyle, color: '#d13212', borderColor: '#d13212' }, children: "Delete" })] }) })] }, a.alarmName))) })] })), createOpen && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, children: [_jsx("h3", { style: { margin: '0 0 1rem', color: '#e2e8f0' }, children: "Create Alarm" }), [
                            { label: 'Alarm Name *', key: 'alarmName', type: 'text' },
                            { label: 'Namespace', key: 'namespace', type: 'text' },
                            { label: 'Metric Name', key: 'metricName', type: 'text' },
                            { label: 'Period (s)', key: 'period', type: 'number' },
                            { label: 'Threshold', key: 'threshold', type: 'number' },
                            { label: 'Evaluation Periods', key: 'evaluationPeriods', type: 'number' },
                        ].map(({ label, key, type }) => (_jsxs("label", { style: labelStyle, children: [label, _jsx("input", { type: type, value: form[key], onChange: e => setForm(f => ({ ...f, [key]: type === 'number' ? Number(e.target.value) : e.target.value })), style: inputStyle })] }, key))), _jsxs("label", { style: labelStyle, children: ["Statistic", _jsx("select", { value: form.statistic, onChange: e => setForm(f => ({ ...f, statistic: e.target.value })), style: inputStyle, children: ['Average', 'Sum', 'Minimum', 'Maximum', 'SampleCount'].map(s => _jsx("option", { value: s, children: s }, s)) })] }), _jsxs("label", { style: labelStyle, children: ["Comparison Operator", _jsx("select", { value: form.comparisonOperator, onChange: e => setForm(f => ({ ...f, comparisonOperator: e.target.value })), style: inputStyle, children: ['GreaterThanThreshold', 'GreaterThanOrEqualToThreshold', 'LessThanThreshold', 'LessThanOrEqualToThreshold'].map(o => (_jsx("option", { value: o, children: o }, o))) })] }), createMut.error && _jsx("div", { style: { color: '#d13212', fontSize: '0.85em', marginTop: '0.5rem' }, children: createMut.error.message }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', marginTop: '1.25rem' }, children: [_jsx("button", { onClick: () => createMut.mutate(), disabled: !form.alarmName, style: primaryBtnStyle, children: "Create" }), _jsx("button", { onClick: () => setCreateOpen(false), style: cancelBtnStyle, children: "Cancel" })] })] }) })), stateTarget && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, children: [_jsx("h3", { style: { margin: '0 0 1rem', color: '#e2e8f0' }, children: "Set Alarm State" }), _jsx("p", { style: { color: '#8892a4', fontSize: '0.88em', margin: '0 0 1rem' }, children: stateTarget.alarmName }), _jsxs("label", { style: labelStyle, children: ["State", _jsx("select", { value: newStateValue, onChange: e => setNewStateValue(e.target.value), style: inputStyle, children: ['OK', 'ALARM', 'INSUFFICIENT_DATA'].map(s => _jsx("option", { value: s, children: s }, s)) })] }), _jsxs("label", { style: labelStyle, children: ["Reason", _jsx("input", { type: "text", value: newStateReason, onChange: e => setNewStateReason(e.target.value), style: inputStyle, placeholder: "Manual state change" })] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', marginTop: '1.25rem' }, children: [_jsx("button", { onClick: () => stateMut.mutate(), style: primaryBtnStyle, children: "Apply" }), _jsx("button", { onClick: () => setStateTarget(null), style: cancelBtnStyle, children: "Cancel" })] })] }) })), deleteTarget && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, children: [_jsx("h3", { style: { margin: '0 0 0.75rem', color: '#e2e8f0' }, children: "Delete Alarm?" }), _jsxs("p", { style: { color: '#8892a4', fontSize: '0.88em' }, children: ["Delete ", _jsx("strong", { style: { color: '#e2e8f0' }, children: deleteTarget.alarmName }), "? This cannot be undone."] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', marginTop: '1.25rem' }, children: [_jsx("button", { onClick: () => deleteMut.mutate(deleteTarget.alarmName), style: { ...primaryBtnStyle, background: '#d13212', borderColor: '#d13212' }, children: "Delete" }), _jsx("button", { onClick: () => setDeleteTarget(null), style: cancelBtnStyle, children: "Cancel" })] })] }) }))] }));
}
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.88em' };
const thStyle = { textAlign: 'left', padding: '0.5rem 0.75rem', color: '#8892a4', fontWeight: 500, borderBottom: '1px solid #2d3748', fontSize: '0.8em', textTransform: 'uppercase', letterSpacing: '0.05em' };
const tdStyle = { padding: '0.6rem 0.75rem', color: '#c9cdd4', verticalAlign: 'middle' };
const primaryBtnStyle = { background: '#0972d3', border: '1px solid #0972d3', borderRadius: 4, color: '#fff', cursor: 'pointer', padding: '0.4rem 1rem', fontSize: '0.85em', fontWeight: 500 };
const cancelBtnStyle = { background: 'none', border: '1px solid #2d3748', borderRadius: 4, color: '#c9cdd4', cursor: 'pointer', padding: '0.4rem 1rem', fontSize: '0.85em' };
const actionBtnStyle = { background: 'none', border: '1px solid #2d3748', borderRadius: 4, color: '#0972d3', cursor: 'pointer', padding: '0.25rem 0.75rem', fontSize: '0.82em' };
const selectStyle = { background: '#1b2a3b', border: '1px solid #2d3748', borderRadius: 4, color: '#e2e8f0', padding: '0.4rem 0.75rem', fontSize: '0.85em' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 50 };
const dialogStyle = { background: '#1b2530', border: '1px solid #2d3748', borderRadius: 8, padding: '1.75rem', minWidth: 440, maxWidth: 560 };
const labelStyle = { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '0.75rem', color: '#c9cdd4', fontSize: '0.85em' };
const inputStyle = { background: '#0d1a26', border: '1px solid #2d3748', borderRadius: 4, color: '#e2e8f0', padding: '0.4rem 0.75rem', fontSize: '0.9em' };
