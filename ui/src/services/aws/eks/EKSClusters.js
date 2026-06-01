import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listClusters, createCluster, deleteCluster } from '../../../api/eks';
export function EKSClusters() {
    const qc = useQueryClient();
    const [createOpen, setCreateOpen] = useState(false);
    const [clusterName, setClusterName] = useState('');
    const { data, isLoading } = useQuery({
        queryKey: ['eks', 'clusters'],
        queryFn: listClusters,
    });
    const create = useMutation({
        mutationFn: (name) => createCluster(name),
        onSuccess: () => { qc.invalidateQueries({ queryKey: ['eks', 'clusters'] }); setCreateOpen(false); setClusterName(''); },
    });
    const del = useMutation({
        mutationFn: (name) => deleteCluster(name),
        onSuccess: () => qc.invalidateQueries({ queryKey: ['eks', 'clusters'] }),
    });
    const items = data?.items ?? [];
    return (_jsxs("div", { className: "p-6", children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h1", { className: "text-2xl font-semibold", children: "EKS Clusters" }), _jsxs("div", { className: "flex items-center gap-2", children: [_jsx("span", { className: "text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded", children: "metadata only" }), _jsx("button", { onClick: () => setCreateOpen(true), className: "px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600", children: "Create Cluster" })] })] }), isLoading && _jsx("p", { className: "text-gray-500", children: "Loading..." }), !isLoading && items.length === 0 && (_jsx("div", { className: "text-center py-16 text-gray-400", children: "No EKS clusters found" })), items.length > 0 && (_jsx("div", { className: "overflow-x-auto rounded border border-gray-200", children: _jsxs("table", { className: "min-w-full text-sm", children: [_jsx("thead", { className: "bg-gray-50 text-gray-600 uppercase text-xs", children: _jsxs("tr", { children: [_jsx("th", { className: "px-4 py-2 text-left", children: "Name" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Status" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Version" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Created" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Actions" })] }) }), _jsx("tbody", { className: "divide-y divide-gray-100", children: items.map(c => (_jsxs("tr", { className: "hover:bg-gray-50", children: [_jsx("td", { className: "px-4 py-2 font-medium", children: c.name }), _jsx("td", { className: "px-4 py-2", children: _jsx("span", { className: `text-xs px-2 py-0.5 rounded ${c.status === 'ACTIVE' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'}`, children: c.status || '—' }) }), _jsx("td", { className: "px-4 py-2", children: c.version || '—' }), _jsx("td", { className: "px-4 py-2 text-gray-500", children: c.createdAt ? new Date(c.createdAt).toLocaleString() : '—' }), _jsx("td", { className: "px-4 py-2", children: _jsx("button", { onClick: () => del.mutate(c.name), className: "text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200", children: "Delete" }) })] }, c.name))) })] }) })), createOpen && (_jsx("div", { className: "fixed inset-0 bg-black/30 flex items-center justify-center z-50", children: _jsxs("div", { className: "bg-white rounded-lg shadow-xl p-6 w-96", children: [_jsx("h2", { className: "text-lg font-semibold mb-4", children: "Create EKS Cluster" }), _jsx("input", { className: "w-full border border-gray-300 rounded px-3 py-2 text-sm mb-4", placeholder: "Cluster name", value: clusterName, onChange: e => setClusterName(e.target.value) }), _jsxs("div", { className: "flex gap-2 justify-end", children: [_jsx("button", { onClick: () => setCreateOpen(false), className: "px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50", children: "Cancel" }), _jsx("button", { onClick: () => create.mutate(clusterName), disabled: !clusterName.trim(), className: "px-4 py-2 text-sm rounded bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-50", children: "Create" })] })] }) }))] }));
}
