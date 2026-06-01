import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listStacks, createStack, deleteStack } from '../../../api/cfn';
function statusColor(status) {
    if (status.includes('COMPLETE') && !status.includes('ROLLBACK'))
        return 'bg-green-100 text-green-700';
    if (status.includes('FAILED') || status.includes('ROLLBACK'))
        return 'bg-red-100 text-red-700';
    if (status.includes('IN_PROGRESS'))
        return 'bg-yellow-100 text-yellow-700';
    return 'bg-gray-100 text-gray-600';
}
const DEFAULT_TEMPLATE = JSON.stringify({ AWSTemplateFormatVersion: '2010-09-09', Resources: {} }, null, 2);
export function CFNStacks() {
    const qc = useQueryClient();
    const [createOpen, setCreateOpen] = useState(false);
    const [form, setForm] = useState({ name: '', templateBody: DEFAULT_TEMPLATE });
    const { data, isLoading } = useQuery({
        queryKey: ['cfn', 'stacks'],
        queryFn: listStacks,
    });
    const create = useMutation({
        mutationFn: () => createStack({ name: form.name, templateBody: form.templateBody }),
        onSuccess: () => { qc.invalidateQueries({ queryKey: ['cfn', 'stacks'] }); setCreateOpen(false); },
    });
    const del = useMutation({
        mutationFn: (name) => deleteStack(name),
        onSuccess: () => qc.invalidateQueries({ queryKey: ['cfn', 'stacks'] }),
    });
    const items = data?.items ?? [];
    return (_jsxs("div", { className: "p-6", children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h1", { className: "text-2xl font-semibold", children: "CloudFormation Stacks" }), _jsx("button", { onClick: () => setCreateOpen(true), className: "px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600", children: "Create Stack" })] }), isLoading && _jsx("p", { className: "text-gray-500", children: "Loading..." }), !isLoading && items.length === 0 && (_jsx("div", { className: "text-center py-16 text-gray-400", children: "No stacks found" })), items.length > 0 && (_jsx("div", { className: "overflow-x-auto rounded border border-gray-200", children: _jsxs("table", { className: "min-w-full text-sm", children: [_jsx("thead", { className: "bg-gray-50 text-gray-600 uppercase text-xs", children: _jsxs("tr", { children: [_jsx("th", { className: "px-4 py-2 text-left", children: "Name" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Status" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Description" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Created" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Actions" })] }) }), _jsx("tbody", { className: "divide-y divide-gray-100", children: items.map(s => (_jsxs("tr", { className: "hover:bg-gray-50", children: [_jsx("td", { className: "px-4 py-2 font-medium", children: s.name }), _jsx("td", { className: "px-4 py-2", children: _jsx("span", { className: `text-xs px-2 py-0.5 rounded ${statusColor(s.status)}`, children: s.status }) }), _jsx("td", { className: "px-4 py-2 text-gray-500 truncate max-w-xs", children: s.description || '—' }), _jsx("td", { className: "px-4 py-2 text-gray-500", children: s.createdAt ? new Date(s.createdAt).toLocaleString() : '—' }), _jsx("td", { className: "px-4 py-2", children: _jsx("button", { onClick: () => del.mutate(s.name), className: "text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200", children: "Delete" }) })] }, s.name))) })] }) })), createOpen && (_jsx("div", { className: "fixed inset-0 bg-black/30 flex items-center justify-center z-50", children: _jsxs("div", { className: "bg-white rounded-lg shadow-xl p-6 w-[560px]", children: [_jsx("h2", { className: "text-lg font-semibold mb-4", children: "Create Stack" }), _jsxs("div", { className: "space-y-3", children: [_jsxs("div", { children: [_jsx("label", { className: "block text-xs font-medium text-gray-700 mb-1", children: "Stack Name" }), _jsx("input", { className: "w-full border border-gray-300 rounded px-3 py-2 text-sm", placeholder: "my-stack", value: form.name, onChange: e => setForm(p => ({ ...p, name: e.target.value })) })] }), _jsxs("div", { children: [_jsx("label", { className: "block text-xs font-medium text-gray-700 mb-1", children: "Template Body (JSON/YAML)" }), _jsx("textarea", { rows: 8, className: "w-full border border-gray-300 rounded px-3 py-2 text-xs font-mono", value: form.templateBody, onChange: e => setForm(p => ({ ...p, templateBody: e.target.value })) })] })] }), _jsxs("div", { className: "flex gap-2 justify-end mt-4", children: [_jsx("button", { onClick: () => setCreateOpen(false), className: "px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50", children: "Cancel" }), _jsx("button", { onClick: () => create.mutate(), disabled: !form.name.trim(), className: "px-4 py-2 text-sm rounded bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-50", children: "Create" })] })] }) }))] }));
}
