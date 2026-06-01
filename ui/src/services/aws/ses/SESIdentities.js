import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listIdentities, verifyEmailIdentity, deleteIdentity } from '../../../api/ses';
export function SESIdentities() {
    const qc = useQueryClient();
    const [createOpen, setCreateOpen] = useState(false);
    const [identityInput, setIdentityInput] = useState('');
    const { data, isLoading } = useQuery({
        queryKey: ['ses', 'identities'],
        queryFn: listIdentities,
    });
    const verify = useMutation({
        mutationFn: () => verifyEmailIdentity(identityInput),
        onSuccess: () => { qc.invalidateQueries({ queryKey: ['ses', 'identities'] }); setCreateOpen(false); setIdentityInput(''); },
    });
    const del = useMutation({
        mutationFn: (identity) => deleteIdentity(identity),
        onSuccess: () => qc.invalidateQueries({ queryKey: ['ses', 'identities'] }),
    });
    const items = data?.items ?? [];
    return (_jsxs("div", { className: "p-6", children: [_jsxs("div", { className: "flex items-center justify-between mb-4", children: [_jsx("h1", { className: "text-2xl font-semibold", children: "SES Identities" }), _jsx("button", { onClick: () => setCreateOpen(true), className: "px-3 py-1.5 text-sm rounded bg-orange-500 text-white hover:bg-orange-600", children: "Verify Identity" })] }), isLoading && _jsx("p", { className: "text-gray-500", children: "Loading..." }), !isLoading && items.length === 0 && (_jsx("div", { className: "text-center py-16 text-gray-400", children: "No SES identities found" })), items.length > 0 && (_jsx("div", { className: "overflow-x-auto rounded border border-gray-200", children: _jsxs("table", { className: "min-w-full text-sm", children: [_jsx("thead", { className: "bg-gray-50 text-gray-600 uppercase text-xs", children: _jsxs("tr", { children: [_jsx("th", { className: "px-4 py-2 text-left", children: "Identity" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Type" }), _jsx("th", { className: "px-4 py-2 text-left", children: "Actions" })] }) }), _jsx("tbody", { className: "divide-y divide-gray-100", children: items.map(id => (_jsxs("tr", { className: "hover:bg-gray-50", children: [_jsx("td", { className: "px-4 py-2 font-medium", children: id.identity }), _jsx("td", { className: "px-4 py-2", children: _jsx("span", { className: "text-xs px-2 py-0.5 rounded bg-blue-100 text-blue-700", children: id.type }) }), _jsx("td", { className: "px-4 py-2", children: _jsx("button", { onClick: () => del.mutate(id.identity), className: "text-xs px-2 py-0.5 rounded bg-red-100 text-red-700 hover:bg-red-200", children: "Delete" }) })] }, id.identity))) })] }) })), createOpen && (_jsx("div", { className: "fixed inset-0 bg-black/30 flex items-center justify-center z-50", children: _jsxs("div", { className: "bg-white rounded-lg shadow-xl p-6 w-[420px]", children: [_jsx("h2", { className: "text-lg font-semibold mb-4", children: "Verify Email Identity" }), _jsxs("div", { children: [_jsx("label", { className: "block text-xs font-medium text-gray-700 mb-1", children: "Email Address or Domain" }), _jsx("input", { className: "w-full border border-gray-300 rounded px-3 py-2 text-sm", placeholder: "user@example.com or example.com", value: identityInput, onChange: e => setIdentityInput(e.target.value) })] }), _jsxs("div", { className: "flex gap-2 justify-end mt-4", children: [_jsx("button", { onClick: () => setCreateOpen(false), className: "px-4 py-2 text-sm rounded border border-gray-300 hover:bg-gray-50", children: "Cancel" }), _jsx("button", { onClick: () => verify.mutate(), disabled: !identityInput.trim(), className: "px-4 py-2 text-sm rounded bg-orange-500 text-white hover:bg-orange-600 disabled:opacity-50", children: "Verify" })] })] }) }))] }));
}
