import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listRules, putRule, deleteRule, enableRule, disableRule, listTargets, putTargets, removeTarget, } from '../../../api/eventbridge';
import { EmptyState } from '../../../components/EmptyState';
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' };
const thStyle = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' };
const tdStyle = { padding: '0.6rem 1rem', verticalAlign: 'middle' };
const btnStyle = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' };
const inputStyle = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const modalStyle = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 440, maxWidth: 580 };
const stateColor = (s) => s === 'ENABLED' ? '#2ecc71' : '#e74c3c';
export function EventBridgeRules() {
    const qc = useQueryClient();
    const [selectedRule, setSelectedRule] = useState(null);
    const [createOpen, setCreateOpen] = useState(false);
    const [deleteTarget, setDeleteTarget] = useState(null);
    const [addTargetOpen, setAddTargetOpen] = useState(false);
    const [form, setForm] = useState({ name: '', eventPattern: '', scheduleExpression: '', state: 'ENABLED', description: '' });
    const [targetForm, setTargetForm] = useState({ id: '', arn: '' });
    const { data, isLoading } = useQuery({
        queryKey: ['eventbridge', 'rules'],
        queryFn: () => listRules(),
    });
    const { data: targetsData } = useQuery({
        queryKey: ['eventbridge', 'targets', selectedRule?.name],
        queryFn: () => listTargets(selectedRule.name),
        enabled: !!selectedRule,
    });
    const createMut = useMutation({
        mutationFn: () => putRule({ name: form.name, eventPattern: form.eventPattern || undefined, scheduleExpression: form.scheduleExpression || undefined, state: form.state, description: form.description || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['eventbridge', 'rules'] });
            setCreateOpen(false);
            setForm({ name: '', eventPattern: '', scheduleExpression: '', state: 'ENABLED', description: '' });
        },
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteRule(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['eventbridge', 'rules'] });
            if (deleteTarget?.name === selectedRule?.name)
                setSelectedRule(null);
            setDeleteTarget(null);
        },
    });
    const toggleMut = useMutation({
        mutationFn: ({ name, enabled }) => enabled ? disableRule(name) : enableRule(name),
        onSuccess: () => { void qc.invalidateQueries({ queryKey: ['eventbridge', 'rules'] }); },
    });
    const addTargetMut = useMutation({
        mutationFn: () => putTargets(selectedRule.name, [{ id: targetForm.id, arn: targetForm.arn }]),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['eventbridge', 'targets', selectedRule?.name] });
            setAddTargetOpen(false);
            setTargetForm({ id: '', arn: '' });
        },
    });
    const removeTargetMut = useMutation({
        mutationFn: ({ rule, id }) => removeTarget(rule, id),
        onSuccess: () => { void qc.invalidateQueries({ queryKey: ['eventbridge', 'targets', selectedRule?.name] }); },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading rules\u2026" });
    const rules = data?.items ?? [];
    const targets = targetsData?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "EventBridge Rules" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [rules.length, " rule", rules.length !== 1 ? 's' : ''] })] }), _jsx("button", { style: btnStyle, onClick: () => setCreateOpen(true), children: "Create Rule" })] }), _jsxs("div", { style: { display: 'grid', gridTemplateColumns: selectedRule ? '1fr 1fr' : '1fr', gap: '1.5rem' }, children: [_jsx("div", { children: rules.length === 0 ? (_jsx(EmptyState, { title: "No rules. Create one to route events to targets." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Name', 'State', 'Pattern / Schedule', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: rules.map(rule => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748', cursor: 'pointer', background: selectedRule?.name === rule.name ? '#1e2d3d' : 'transparent' }, onClick: () => setSelectedRule(rule), children: [_jsx("td", { style: { ...tdStyle, fontWeight: 600 }, children: rule.name }), _jsx("td", { style: tdStyle, children: _jsx("span", { style: { color: stateColor(rule.state), fontSize: '0.8rem', fontWeight: 600 }, children: rule.state ?? '—' }) }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.8rem', fontFamily: 'monospace', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }, children: rule.scheduleExpression || rule.eventPattern || '—' }), _jsxs("td", { style: { ...tdStyle, textAlign: 'right', whiteSpace: 'nowrap' }, onClick: e => e.stopPropagation(), children: [_jsx("button", { style: { ...btnStyle, background: 'transparent', color: rule.state === 'ENABLED' ? '#e87600' : '#2ecc71', border: `1px solid ${rule.state === 'ENABLED' ? '#e87600' : '#2ecc71'}`, padding: '0.25rem 0.6rem', fontSize: '0.78rem', marginRight: '0.4rem' }, onClick: () => toggleMut.mutate({ name: rule.name, enabled: rule.state === 'ENABLED' }), children: rule.state === 'ENABLED' ? 'Disable' : 'Enable' }), _jsx("button", { style: { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.78rem' }, onClick: () => setDeleteTarget(rule), children: "Delete" })] })] }, rule.name))) })] })) }), selectedRule && (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }, children: [_jsxs("h3", { style: { margin: 0, fontWeight: 600, fontSize: '1rem' }, children: ["Targets for ", _jsx("em", { children: selectedRule.name }), " (", targets.length, ")"] }), _jsxs("div", { style: { display: 'flex', gap: '0.5rem' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0', padding: '0.3rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setSelectedRule(null), children: "\u2715" }), _jsx("button", { style: { ...btnStyle, fontSize: '0.8rem', padding: '0.3rem 0.75rem' }, onClick: () => setAddTargetOpen(true), children: "Add Target" })] })] }), targets.length === 0 ? (_jsx(EmptyState, { title: "No targets for this rule." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['ID', 'ARN', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: targets.map(t => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: { ...tdStyle, fontWeight: 600 }, children: t.id }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.8rem', fontFamily: 'monospace' }, children: t.arn }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, children: _jsx("button", { style: { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.78rem' }, onClick: () => removeTargetMut.mutate({ rule: selectedRule.name, id: t.id }), children: "Remove" }) })] }, t.id))) })] }))] }))] }), createOpen && (_jsx("div", { style: overlayStyle, onClick: () => setCreateOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: "Create Rule" }), [
                            { key: 'name', label: 'Name *', placeholder: 'my-rule' },
                            { key: 'eventPattern', label: 'Event Pattern (JSON)', placeholder: '{"source":["aws.ec2"]}' },
                            { key: 'scheduleExpression', label: 'Schedule Expression', placeholder: 'rate(5 minutes)' },
                            { key: 'description', label: 'Description', placeholder: '' },
                        ].map(f => (_jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: f.label }), _jsx("input", { style: inputStyle, placeholder: f.placeholder, value: form[f.key], onChange: e => setForm(p => ({ ...p, [f.key]: e.target.value })) })] }, f.key))), _jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1.5rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: "State" }), _jsxs("select", { style: inputStyle, value: form.state, onChange: e => setForm(p => ({ ...p, state: e.target.value })), children: [_jsx("option", { value: "ENABLED", children: "ENABLED" }), _jsx("option", { value: "DISABLED", children: "DISABLED" })] })] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !form.name || createMut.isPending, onClick: () => createMut.mutate(), children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), deleteTarget && (_jsx("div", { style: overlayStyle, onClick: () => setDeleteTarget(null), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem', fontWeight: 600 }, children: "Delete Rule?" }), _jsxs("p", { style: { color: '#b0bec5', marginBottom: '1.5rem' }, children: ["Delete rule ", _jsx("strong", { children: deleteTarget.name }), "?"] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setDeleteTarget(null), children: "Cancel" }), _jsx("button", { style: { ...btnStyle, background: '#d13212' }, disabled: deleteMut.isPending, onClick: () => deleteMut.mutate(deleteTarget.name), children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) })), addTargetOpen && selectedRule && (_jsx("div", { style: overlayStyle, onClick: () => setAddTargetOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsxs("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: ["Add Target to ", selectedRule.name] }), [
                            { key: 'id', label: 'Target ID *', placeholder: 'target-1' },
                            { key: 'arn', label: 'ARN *', placeholder: 'arn:aws:sqs:us-east-1:000000000000:my-queue' },
                        ].map(f => (_jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: f.label }), _jsx("input", { style: inputStyle, placeholder: f.placeholder, value: targetForm[f.key], onChange: e => setTargetForm(p => ({ ...p, [f.key]: e.target.value })) })] }, f.key))), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setAddTargetOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !targetForm.id || !targetForm.arn || addTargetMut.isPending, onClick: () => addTargetMut.mutate(), children: addTargetMut.isPending ? 'Adding…' : 'Add' })] })] }) }))] }));
}
