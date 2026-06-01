import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listZones, createZone, deleteZone, listRecords } from '../../../api/route53';
export function Route53Zones() {
    const qc = useQueryClient();
    const [createOpen, setCreateOpen] = useState(false);
    const [zoneName, setZoneName] = useState('');
    const [expandedId, setExpandedId] = useState(null);
    const { data, isLoading } = useQuery({
        queryKey: ['route53', 'zones'],
        queryFn: listZones,
    });
    const recordsQuery = useQuery({
        queryKey: ['route53', 'records', expandedId],
        queryFn: () => listRecords(expandedId),
        enabled: !!expandedId,
    });
    const create = useMutation({
        mutationFn: (name) => createZone(name),
        onSuccess: () => { qc.invalidateQueries({ queryKey: ['route53', 'zones'] }); setCreateOpen(false); setZoneName(''); },
    });
    const del = useMutation({
        mutationFn: (id) => deleteZone(id),
        onSuccess: () => { qc.invalidateQueries({ queryKey: ['route53', 'zones'] }); setExpandedId(null); },
    });
    const items = data?.items ?? [];
    return (_jsxs("div", { className: "p-6", children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h1", { className: "text-2xl font-semibold", children: "Route 53 Hosted Zones" }), _jsxs("div", { className: "flex items-center gap-2", children: [_jsx("span", { className: "text-xs bg-yellow-100 text-yellow-800 px-2 py-0.5 rounded", children: "metadata only" }), _jsx("button", { onClick: () => setCreateOpen(true), className: "px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600", children: "Create Zone" })] })] }), isLoading && _jsx("p", { className: "text-gray-500", children: "Loading..." }), !isLoading && items.length === 0 && (_jsx("div", { className: "text-center py-16 text-gray-400", children: "No hosted zones found" })), _jsx("div", { className: "space-y-2", children: items.map(z => (_jsxs("div", { className: "border border-gray-200 rounded-lg overflow-hidden", children: [_jsxs("div", { className: "flex items-center justify-between px-4 py-3 cursor-pointer hover:bg-gray-50", onClick: () => setExpandedId(expandedId === z.id ? null : z.id), children: [_jsxs("div", { className: "flex items-center gap-3", children: [_jsx("span", { className: "font-medium", children: z.name }), _jsx("span", { className: "text-xs text-gray-500 font-mono", children: z.id }), z.private && _jsx("span", { className: "text-xs bg-blue-100 text-blue-700 px-1.5 py-0.5 rounded", children: "Private" })] }), _jsxs("div", { className: "flex items-center gap-3", children: [_jsxs("span", { className: "text-sm text-gray-500", children: [z.recordCount, " records"] }), _jsx("button", { onClick: e => { e.stopPropagation(); del.mutate(z.id); }, className: "text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200", children: "Delete" }), _jsx("span", { className: "text-gray-400", children: expandedId === z.id ? '▲' : '▼' })] })] }), expandedId === z.id && (_jsxs("div", { className: "border-t border-gray-100 bg-gray-50 px-4 py-3", children: [_jsx("p", { className: "text-xs font-semibold text-gray-500 uppercase mb-2", children: "Record Sets" }), recordsQuery.isLoading && _jsx("p", { className: "text-xs text-gray-400", children: "Loading records..." }), recordsQuery.data?.items.length === 0 && _jsx("p", { className: "text-xs text-gray-400", children: "No records found" }), recordsQuery.data?.items.map((r, i) => (_jsxs("div", { className: "text-xs font-mono flex gap-4 py-0.5", children: [_jsx("span", { className: "text-blue-700 w-32 truncate", children: r.name }), _jsx("span", { className: "text-purple-700 w-10", children: r.type }), _jsxs("span", { className: "text-gray-500 w-12", children: ["TTL ", r.ttl] }), _jsx("span", { className: "text-gray-700", children: r.records?.join(', ') })] }, i)))] }))] }, z.id))) }), createOpen && (_jsx("div", { className: "fixed inset-0 bg-black/30 flex items-center justify-center z-50", children: _jsxs("div", { className: "bg-white rounded-lg shadow-xl p-6 w-96", children: [_jsx("h2", { className: "text-lg font-semibold mb-4", children: "Create Hosted Zone" }), _jsx("input", { className: "w-full border border-gray-300 rounded px-3 py-2 text-sm mb-4", placeholder: "example.com", value: zoneName, onChange: e => setZoneName(e.target.value) }), _jsxs("div", { className: "flex gap-2 justify-end", children: [_jsx("button", { onClick: () => setCreateOpen(false), className: "px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50", children: "Cancel" }), _jsx("button", { onClick: () => create.mutate(zoneName), disabled: !zoneName.trim(), className: "px-4 py-2 text-sm rounded bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-50", children: "Create" })] })] }) }))] }));
}
