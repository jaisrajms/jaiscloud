import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listClusters, createCluster, deleteCluster, listTasks, listServices } from '../../../api/ecs';
export function ECSClusters() {
    const qc = useQueryClient();
    const [createOpen, setCreateOpen] = useState(false);
    const [clusterName, setClusterName] = useState('');
    const [expanded, setExpanded] = useState(null);
    const { data, isLoading } = useQuery({
        queryKey: ['ecs', 'clusters'],
        queryFn: listClusters,
    });
    const tasksQuery = useQuery({
        queryKey: ['ecs', 'tasks', expanded],
        queryFn: () => listTasks(expanded),
        enabled: !!expanded,
    });
    const servicesQuery = useQuery({
        queryKey: ['ecs', 'services', expanded],
        queryFn: () => listServices(expanded),
        enabled: !!expanded,
    });
    const create = useMutation({
        mutationFn: (name) => createCluster(name),
        onSuccess: () => { qc.invalidateQueries({ queryKey: ['ecs', 'clusters'] }); setCreateOpen(false); setClusterName(''); },
    });
    const del = useMutation({
        mutationFn: (name) => deleteCluster(name),
        onSuccess: () => { qc.invalidateQueries({ queryKey: ['ecs', 'clusters'] }); setExpanded(null); },
    });
    const items = data?.items ?? [];
    return (_jsxs("div", { className: "p-6", children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h1", { className: "text-2xl font-semibold", children: "ECS Clusters" }), _jsxs("div", { className: "flex items-center gap-2", children: [_jsx("span", { className: "text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded", children: "metadata only" }), _jsx("button", { onClick: () => setCreateOpen(true), className: "px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600", children: "Create Cluster" })] })] }), isLoading && _jsx("p", { className: "text-gray-500", children: "Loading..." }), !isLoading && items.length === 0 && (_jsx("div", { className: "text-center py-16 text-gray-400", children: "No ECS clusters found" })), _jsx("div", { className: "space-y-2", children: items.map(c => (_jsxs("div", { className: "border border-gray-200 rounded-lg overflow-hidden", children: [_jsxs("div", { className: "flex items-center justify-between px-4 py-3 cursor-pointer hover:bg-gray-50", onClick: () => setExpanded(expanded === c.name ? null : c.name), children: [_jsxs("div", { className: "flex items-center gap-3", children: [_jsx("span", { className: "font-medium", children: c.name }), _jsx("span", { className: `text-xs px-2 py-0.5 rounded ${c.status === 'ACTIVE' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-600'}`, children: c.status })] }), _jsxs("div", { className: "flex items-center gap-2", children: [_jsx("button", { onClick: e => { e.stopPropagation(); del.mutate(c.name); }, className: "text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200", children: "Delete" }), _jsx("span", { className: "text-gray-400", children: expanded === c.name ? '▲' : '▼' })] })] }), expanded === c.name && (_jsxs("div", { className: "border-t border-gray-100 bg-gray-50 px-4 py-3 space-y-3", children: [_jsxs("div", { children: [_jsxs("p", { className: "text-xs font-semibold text-gray-500 uppercase mb-1", children: ["Tasks (", tasksQuery.data?.total ?? 0, ")"] }), tasksQuery.data?.items.map(t => (_jsx("p", { className: "text-xs font-mono text-gray-600", children: t.arn }, t.arn))), tasksQuery.data?.items.length === 0 && _jsx("p", { className: "text-xs text-gray-400", children: "No tasks" })] }), _jsxs("div", { children: [_jsxs("p", { className: "text-xs font-semibold text-gray-500 uppercase mb-1", children: ["Services (", servicesQuery.data?.total ?? 0, ")"] }), servicesQuery.data?.items.map(s => (_jsx("p", { className: "text-xs font-mono text-gray-600", children: s.arn }, s.arn))), servicesQuery.data?.items.length === 0 && _jsx("p", { className: "text-xs text-gray-400", children: "No services" })] })] }))] }, c.name))) }), createOpen && (_jsx("div", { className: "fixed inset-0 bg-black/30 flex items-center justify-center z-50", children: _jsxs("div", { className: "bg-white rounded-lg shadow-xl p-6 w-96", children: [_jsx("h2", { className: "text-lg font-semibold mb-4", children: "Create ECS Cluster" }), _jsx("input", { className: "w-full border border-gray-300 rounded px-3 py-2 text-sm mb-4", placeholder: "Cluster name", value: clusterName, onChange: e => setClusterName(e.target.value) }), _jsxs("div", { className: "flex gap-2 justify-end", children: [_jsx("button", { onClick: () => setCreateOpen(false), className: "px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50", children: "Cancel" }), _jsx("button", { onClick: () => create.mutate(clusterName), disabled: !clusterName.trim(), className: "px-4 py-2 text-sm rounded bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-50", children: "Create" })] })] }) }))] }));
}
