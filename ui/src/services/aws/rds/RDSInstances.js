import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listInstances, createInstance, deleteInstance, startInstance, stopInstance } from '../../../api/rds';
function statusColor(status) {
    if (status === 'available')
        return 'bg-green-100 text-green-700';
    if (status === 'stopped')
        return 'bg-gray-100 text-gray-600';
    if (status === 'deleting')
        return 'bg-red-100 text-red-700';
    return 'bg-yellow-100 text-yellow-700';
}
export function RDSInstances() {
    const qc = useQueryClient();
    const [createOpen, setCreateOpen] = useState(false);
    const [form, setForm] = useState({ id: '', engine: 'mysql', class: 'db.t3.micro', username: 'admin', password: '' });
    const { data, isLoading } = useQuery({
        queryKey: ['rds', 'instances'],
        queryFn: listInstances,
    });
    const create = useMutation({
        mutationFn: () => createInstance(form),
        onSuccess: () => { qc.invalidateQueries({ queryKey: ['rds', 'instances'] }); setCreateOpen(false); },
    });
    const del = useMutation({
        mutationFn: (id) => deleteInstance(id),
        onSuccess: () => qc.invalidateQueries({ queryKey: ['rds', 'instances'] }),
    });
    const start = useMutation({
        mutationFn: (id) => startInstance(id),
        onSuccess: () => qc.invalidateQueries({ queryKey: ['rds', 'instances'] }),
    });
    const stop = useMutation({
        mutationFn: (id) => stopInstance(id),
        onSuccess: () => qc.invalidateQueries({ queryKey: ['rds', 'instances'] }),
    });
    const items = data?.items ?? [];
    return (_jsxs("div", { className: "p-6", children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h1", { className: "text-2xl font-semibold", children: "RDS Instances" }), _jsxs("div", { className: "flex items-center gap-2", children: [_jsx("span", { className: "text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded", children: "metadata only" }), _jsx("button", { onClick: () => setCreateOpen(true), className: "px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600", children: "Create Instance" })] })] }), isLoading && _jsx("p", { className: "text-gray-500", children: "Loading..." }), !isLoading && items.length === 0 && (_jsx("div", { className: "text-center py-16 text-gray-400", children: "No RDS instances found" })), items.length > 0 && (_jsx("div", { className: "overflow-x-auto rounded border border-gray-200", children: _jsxs("table", { className: "min-w-full text-sm", children: [_jsx("thead", { className: "bg-gray-50 text-gray-600 uppercase text-xs", children: _jsxs("tr", { children: [_jsx("th", { className: "px-4 py-2 text-left", children: "Identifier" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Status" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Engine" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Class" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Endpoint" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Actions" })] }) }), _jsx("tbody", { className: "divide-y divide-gray-100", children: items.map(inst => (_jsxs("tr", { className: "hover:bg-gray-50", children: [_jsx("td", { className: "px-4 py-2 font-medium", children: inst.id }), _jsx("td", { className: "px-4 py-2", children: _jsx("span", { className: `text-xs px-2 py-0.5 rounded ${statusColor(inst.status)}`, children: inst.status }) }), _jsx("td", { className: "px-4 py-2", children: inst.engine }), _jsx("td", { className: "px-4 py-2", children: inst.class }), _jsx("td", { className: "px-4 py-2 font-mono text-xs", children: inst.endpoint ? `${inst.endpoint}:${inst.port}` : '—' }), _jsxs("td", { className: "px-4 py-2 space-x-1", children: [inst.status === 'stopped' && (_jsx("button", { onClick: () => start.mutate(inst.id), className: "text-xs px-2 py-0.5 rounded bg-green-100 text-green-700 hover:bg-green-200", children: "Start" })), inst.status === 'available' && (_jsx("button", { onClick: () => stop.mutate(inst.id), className: "text-xs px-2 py-0.5 rounded bg-yellow-100 text-yellow-700 hover:bg-yellow-200", children: "Stop" })), _jsx("button", { onClick: () => del.mutate(inst.id), className: "text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200", children: "Delete" })] })] }, inst.id))) })] }) })), createOpen && (_jsx("div", { className: "fixed inset-0 bg-black/30 flex items-center justify-center z-50", children: _jsxs("div", { className: "bg-white rounded-lg shadow-xl p-6 w-[480px]", children: [_jsx("h2", { className: "text-lg font-semibold mb-4", children: "Create RDS Instance" }), _jsx("div", { className: "space-y-3", children: [
                                { label: 'Identifier', key: 'id', placeholder: 'my-db' },
                                { label: 'Engine', key: 'engine', placeholder: 'mysql' },
                                { label: 'Class', key: 'class', placeholder: 'db.t3.micro' },
                                { label: 'Master Username', key: 'username', placeholder: 'admin' },
                                { label: 'Master Password', key: 'password', placeholder: '••••••••' },
                            ].map(f => (_jsxs("div", { children: [_jsx("label", { className: "block text-xs font-medium text-gray-700 mb-1", children: f.label }), _jsx("input", { type: f.key === 'password' ? 'password' : 'text', className: "w-full border border-gray-300 rounded px-3 py-2 text-sm", placeholder: f.placeholder, value: form[f.key], onChange: e => setForm(p => ({ ...p, [f.key]: e.target.value })) })] }, f.key))) }), _jsxs("div", { className: "flex gap-2 justify-end mt-4", children: [_jsx("button", { onClick: () => setCreateOpen(false), className: "px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50", children: "Cancel" }), _jsx("button", { onClick: () => create.mutate(), disabled: !form.id.trim(), className: "px-4 py-2 text-sm rounded bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-50", children: "Create" })] })] }) }))] }));
}
