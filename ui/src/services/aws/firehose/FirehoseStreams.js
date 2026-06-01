import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listDeliveryStreams, createDeliveryStream, deleteDeliveryStream } from '../../../api/firehose';
export function FirehoseStreams() {
    const qc = useQueryClient();
    const [createOpen, setCreateOpen] = useState(false);
    const [form, setForm] = useState({ name: '', type: 'DirectPut' });
    const { data, isLoading } = useQuery({
        queryKey: ['firehose', 'streams'],
        queryFn: listDeliveryStreams,
    });
    const create = useMutation({
        mutationFn: () => createDeliveryStream(form),
        onSuccess: () => { qc.invalidateQueries({ queryKey: ['firehose', 'streams'] }); setCreateOpen(false); },
    });
    const del = useMutation({
        mutationFn: (name) => deleteDeliveryStream(name),
        onSuccess: () => qc.invalidateQueries({ queryKey: ['firehose', 'streams'] }),
    });
    const items = data?.items ?? [];
    return (_jsxs("div", { className: "p-6", children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h1", { className: "text-2xl font-semibold", children: "Firehose Delivery Streams" }), _jsx("button", { onClick: () => setCreateOpen(true), className: "px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600", children: "Create Stream" })] }), isLoading && _jsx("p", { className: "text-gray-500", children: "Loading..." }), !isLoading && items.length === 0 && (_jsx("div", { className: "text-center py-16 text-gray-400", children: "No Firehose delivery streams found" })), items.length > 0 && (_jsx("div", { className: "overflow-x-auto rounded border border-gray-200", children: _jsxs("table", { className: "min-w-full text-sm", children: [_jsx("thead", { className: "bg-gray-50 text-gray-600 uppercase text-xs", children: _jsxs("tr", { children: [_jsx("th", { className: "px-4 py-2 text-left", children: "Stream Name" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Actions" })] }) }), _jsx("tbody", { className: "divide-y divide-gray-100", children: items.map(s => (_jsxs("tr", { className: "hover:bg-gray-50", children: [_jsx("td", { className: "px-4 py-2 font-medium", children: s.name }), _jsx("td", { className: "px-4 py-2", children: _jsx("button", { onClick: () => del.mutate(s.name), className: "text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200", children: "Delete" }) })] }, s.name))) })] }) })), createOpen && (_jsx("div", { className: "fixed inset-0 bg-black/30 flex items-center justify-center z-50", children: _jsxs("div", { className: "bg-white rounded-lg shadow-xl p-6 w-[420px]", children: [_jsx("h2", { className: "text-lg font-semibold mb-4", children: "Create Delivery Stream" }), _jsxs("div", { className: "space-y-3", children: [_jsxs("div", { children: [_jsx("label", { className: "block text-xs font-medium text-gray-700 mb-1", children: "Stream Name" }), _jsx("input", { className: "w-full border border-gray-300 rounded px-3 py-2 text-sm", placeholder: "my-delivery-stream", value: form.name, onChange: e => setForm(p => ({ ...p, name: e.target.value })) })] }), _jsxs("div", { children: [_jsx("label", { className: "block text-xs font-medium text-gray-700 mb-1", children: "Type" }), _jsxs("select", { className: "w-full border border-gray-300 rounded px-3 py-2 text-sm", value: form.type, onChange: e => setForm(p => ({ ...p, type: e.target.value })), children: [_jsx("option", { value: "DirectPut", children: "DirectPut" }), _jsx("option", { value: "KinesisStreamAsSource", children: "KinesisStreamAsSource" })] })] })] }), _jsxs("div", { className: "flex gap-2 justify-end mt-4", children: [_jsx("button", { onClick: () => setCreateOpen(false), className: "px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50", children: "Cancel" }), _jsx("button", { onClick: () => create.mutate(), disabled: !form.name.trim(), className: "px-4 py-2 text-sm rounded bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-50", children: "Create" })] })] }) }))] }));
}
