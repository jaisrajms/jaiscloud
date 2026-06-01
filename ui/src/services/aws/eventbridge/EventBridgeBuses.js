import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listEventBuses, createEventBus, deleteEventBus, putEvents, } from '../../../api/eventbridge';
import { EmptyState } from '../../../components/EmptyState';
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' };
const thStyle = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' };
const tdStyle = { padding: '0.6rem 1rem', verticalAlign: 'middle' };
const btnStyle = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' };
const inputStyle = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const modalStyle = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 440, maxWidth: 580 };
export function EventBridgeBuses() {
    const qc = useQueryClient();
    const [createOpen, setCreateOpen] = useState(false);
    const [deleteTarget, setDeleteTarget] = useState(null);
    const [sendEventsOpen, setSendEventsOpen] = useState(false);
    const [busName, setBusName] = useState('');
    const [eventForm, setEventForm] = useState({ source: '', detailType: '', detail: '{}', bus: '' });
    const { data, isLoading } = useQuery({
        queryKey: ['eventbridge', 'buses'],
        queryFn: () => listEventBuses(),
    });
    const createMut = useMutation({
        mutationFn: () => createEventBus(busName),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['eventbridge', 'buses'] });
            setCreateOpen(false);
            setBusName('');
        },
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteEventBus(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['eventbridge', 'buses'] });
            setDeleteTarget(null);
        },
    });
    const sendEventsMut = useMutation({
        mutationFn: () => putEvents([{ source: eventForm.source, detailType: eventForm.detailType, detail: eventForm.detail, bus: eventForm.bus || undefined }]),
        onSuccess: () => {
            setSendEventsOpen(false);
            setEventForm({ source: '', detailType: '', detail: '{}', bus: '' });
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading event buses\u2026" });
    const buses = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "Event Buses" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [buses.length, " bus", buses.length !== 1 ? 'es' : ''] })] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748' }, onClick: () => setSendEventsOpen(true), children: "Send Events" }), _jsx("button", { style: btnStyle, onClick: () => setCreateOpen(true), children: "Create Bus" })] })] }), buses.length === 0 ? (_jsx(EmptyState, { title: "No custom event buses. The default bus is always available." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Name', 'ARN', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: buses.map(bus => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: { ...tdStyle, fontWeight: 600 }, children: bus.name }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.82rem', fontFamily: 'monospace' }, children: bus.arn ?? '—' }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, children: bus.name !== 'default' && (_jsx("button", { style: { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setDeleteTarget(bus), children: "Delete" })) })] }, bus.name))) })] })), createOpen && (_jsx("div", { style: overlayStyle, onClick: () => setCreateOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: "Create Event Bus" }), _jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1.5rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: "Name *" }), _jsx("input", { style: inputStyle, placeholder: "my-custom-bus", value: busName, onChange: e => setBusName(e.target.value) })] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !busName || createMut.isPending, onClick: () => createMut.mutate(), children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), deleteTarget && (_jsx("div", { style: overlayStyle, onClick: () => setDeleteTarget(null), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem', fontWeight: 600 }, children: "Delete Event Bus?" }), _jsxs("p", { style: { color: '#b0bec5', marginBottom: '1.5rem' }, children: ["Delete bus ", _jsx("strong", { children: deleteTarget.name }), "?"] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setDeleteTarget(null), children: "Cancel" }), _jsx("button", { style: { ...btnStyle, background: '#d13212' }, disabled: deleteMut.isPending, onClick: () => deleteMut.mutate(deleteTarget.name), children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) })), sendEventsOpen && (_jsx("div", { style: overlayStyle, onClick: () => setSendEventsOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: "Send Event" }), [
                            { key: 'source', label: 'Source *', placeholder: 'my.app' },
                            { key: 'detailType', label: 'Detail Type *', placeholder: 'StateChange' },
                            { key: 'detail', label: 'Detail (JSON) *', placeholder: '{}' },
                            { key: 'bus', label: 'Event Bus (optional)', placeholder: 'default' },
                        ].map(f => (_jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: f.label }), _jsx("input", { style: inputStyle, placeholder: f.placeholder, value: eventForm[f.key], onChange: e => setEventForm(p => ({ ...p, [f.key]: e.target.value })) })] }, f.key))), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setSendEventsOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !eventForm.source || !eventForm.detailType || sendEventsMut.isPending, onClick: () => sendEventsMut.mutate(), children: sendEventsMut.isPending ? 'Sending…' : 'Send' })] })] }) }))] }));
}
