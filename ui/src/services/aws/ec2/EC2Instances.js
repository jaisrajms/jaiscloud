import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listInstances, terminateInstance, startInstance, stopInstance } from '../../../api/ec2';
function stateColor(state) {
    if (state === 'running')
        return 'text-green-600';
    if (state === 'stopped')
        return 'text-gray-500';
    if (state === 'terminated')
        return 'text-red-500';
    return 'text-yellow-500';
}
export function EC2Instances() {
    const qc = useQueryClient();
    const [selected, setSelected] = useState(null);
    const { data, isLoading } = useQuery({
        queryKey: ['ec2', 'instances'],
        queryFn: () => listInstances(),
    });
    const terminate = useMutation({
        mutationFn: (id) => terminateInstance(id),
        onSuccess: () => { qc.invalidateQueries({ queryKey: ['ec2', 'instances'] }); setSelected(null); },
    });
    const start = useMutation({
        mutationFn: (id) => startInstance(id),
        onSuccess: () => qc.invalidateQueries({ queryKey: ['ec2', 'instances'] }),
    });
    const stop = useMutation({
        mutationFn: (id) => stopInstance(id),
        onSuccess: () => qc.invalidateQueries({ queryKey: ['ec2', 'instances'] }),
    });
    const items = data?.items ?? [];
    return (_jsxs("div", { className: "p-6", children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h1", { className: "text-2xl font-semibold", children: "EC2 Instances" }), _jsx("span", { className: "text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded", children: "metadata only" })] }), isLoading && _jsx("p", { className: "text-gray-500", children: "Loading..." }), !isLoading && items.length === 0 && (_jsx("div", { className: "text-center py-16 text-gray-400", children: "No instances found" })), items.length > 0 && (_jsx("div", { className: "overflow-x-auto rounded border border-gray-200", children: _jsxs("table", { className: "min-w-full text-sm", children: [_jsx("thead", { className: "bg-gray-50 text-gray-600 uppercase text-xs", children: _jsxs("tr", { children: [_jsx("th", { className: "px-4 py-2 text-left", children: "Instance ID" }), _jsx("th", { className: "px-4 py-2 text-left", children: "State" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Type" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Image ID" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Private IP" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Public IP" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Actions" })] }) }), _jsx("tbody", { className: "divide-y divide-gray-100", children: items.map(inst => (_jsxs("tr", { className: "hover:bg-gray-50", children: [_jsx("td", { className: "px-4 py-2 font-mono text-xs", children: inst.id }), _jsx("td", { className: `px-4 py-2 font-medium ${stateColor(inst.state)}`, children: inst.state }), _jsx("td", { className: "px-4 py-2", children: inst.instanceType }), _jsx("td", { className: "px-4 py-2 font-mono text-xs", children: inst.imageId }), _jsx("td", { className: "px-4 py-2 font-mono text-xs", children: inst.privateIp || '—' }), _jsx("td", { className: "px-4 py-2 font-mono text-xs", children: inst.publicIp || '—' }), _jsxs("td", { className: "px-4 py-2 space-x-1", children: [inst.state === 'stopped' && (_jsx("button", { onClick: () => start.mutate(inst.id), className: "text-xs px-2 py-0.5 rounded bg-green-100 text-green-700 hover:bg-green-200", children: "Start" })), inst.state === 'running' && (_jsx("button", { onClick: () => stop.mutate(inst.id), className: "text-xs px-2 py-0.5 rounded bg-yellow-100 text-yellow-700 hover:bg-yellow-200", children: "Stop" })), inst.state !== 'terminated' && (_jsx("button", { onClick: () => setSelected(inst), className: "text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200", children: "Terminate" }))] })] }, inst.id))) })] }) })), selected && (_jsx("div", { className: "fixed inset-0 bg-black/30 flex items-center justify-center z-50", children: _jsxs("div", { className: "bg-white rounded-lg shadow-xl p-6 w-96", children: [_jsx("h2", { className: "text-lg font-semibold mb-2", children: "Terminate Instance" }), _jsxs("p", { className: "text-sm text-gray-600 mb-4", children: ["Terminate ", _jsx("span", { className: "font-mono", children: selected.id }), "? This cannot be undone."] }), _jsxs("div", { className: "flex gap-2 justify-end", children: [_jsx("button", { onClick: () => setSelected(null), className: "px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50", children: "Cancel" }), _jsx("button", { onClick: () => terminate.mutate(selected.id), className: "px-4 py-2 text-sm rounded bg-red-600 text-white hover:bg-red-700", children: "Terminate" })] })] }) }))] }));
}
