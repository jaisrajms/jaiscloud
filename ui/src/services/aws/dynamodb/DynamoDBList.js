import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { listTables, createTable, deleteTable } from '../../../api/dynamodb';
import { EmptyState } from '../../../components/EmptyState';
function fmtBytes(n) {
    if (n < 1024)
        return `${n} B`;
    if (n < 1024 * 1024)
        return `${(n / 1024).toFixed(1)} KB`;
    return `${(n / 1024 / 1024).toFixed(1)} MB`;
}
export function DynamoDBList() {
    const [createOpen, setCreateOpen] = useState(false);
    const [confirmDelete, setConfirmDelete] = useState(null);
    const qc = useQueryClient();
    const navigate = useNavigate();
    const { data, isLoading, error } = useQuery({
        queryKey: ['dynamodb', 'tables'],
        queryFn: () => listTables(),
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteTable(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['dynamodb', 'tables'] });
            setConfirmDelete(null);
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading tables\u2026" });
    if (error)
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load: ", error.message] });
    const tables = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "DynamoDB Tables" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [tables.length, " table", tables.length !== 1 ? 's' : ''] })] }), _jsx("button", { onClick: () => setCreateOpen(true), style: btnPrimary, children: "Create table" })] }), tables.length === 0 ? (_jsx(EmptyState, { title: "No tables", description: "DynamoDB tables store your NoSQL data.", cta: "Create Table", onCta: () => setCreateOpen(true) })) : (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Name" }), _jsx("th", { style: th, children: "Status" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Items" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Size" }), _jsx("th", { style: th, children: "Billing" }), _jsx("th", { style: { ...th, width: 80 } })] }) }), _jsx("tbody", { children: tables.map((t) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }, onClick: () => navigate(`/aws/dynamodb/${encodeURIComponent(t.name)}`), onMouseEnter: (e) => (e.currentTarget.style.background = '#fafbfc'), onMouseLeave: (e) => (e.currentTarget.style.background = ''), children: [_jsx("td", { style: td, children: _jsx("span", { style: { color: '#0972d3', fontWeight: 500 }, children: t.name }) }), _jsx("td", { style: td, children: _jsx("span", { style: {
                                                display: 'inline-block', padding: '0.15em 0.55em', borderRadius: 3,
                                                fontSize: '0.8em', fontWeight: 500,
                                                background: t.status === 'ACTIVE' ? '#e0f9e0' : '#fff8e0',
                                                color: t.status === 'ACTIVE' ? '#1d6b2e' : '#8a6500',
                                            }, children: t.status }) }), _jsx("td", { style: { ...td, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }, children: t.itemCount.toLocaleString() }), _jsx("td", { style: { ...td, textAlign: 'right', color: '#5f6b7a' }, children: fmtBytes(t.sizeBytes) }), _jsx("td", { style: { ...td, color: '#5f6b7a', fontSize: '0.85em' }, children: t.billingMode }), _jsx("td", { style: td, onClick: (e) => e.stopPropagation(), children: _jsx("button", { onClick: () => setConfirmDelete(t), style: btnDelete, children: "Delete" }) })] }, t.name))) })] }) })), createOpen && (_jsx(CreateTableDialog, { onClose: () => setCreateOpen(false), onCreated: () => {
                    setCreateOpen(false);
                    void qc.invalidateQueries({ queryKey: ['dynamodb', 'tables'] });
                } })), confirmDelete && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 0.75rem', fontSize: '1.05rem' }, children: "Delete table?" }), _jsxs("p", { style: { margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Permanently delete ", _jsx("strong", { children: confirmDelete.name }), " and all its items?"] }), deleteMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: deleteMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => setConfirmDelete(null), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => deleteMut.mutate(confirmDelete.name), disabled: deleteMut.isPending, style: { ...btnDanger, opacity: deleteMut.isPending ? 0.6 : 1 }, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
function CreateTableDialog({ onClose, onCreated }) {
    const [form, setForm] = useState({
        tableName: '',
        keySchema: [{ attributeName: 'pk', keyType: 'HASH' }],
        attributeDefinitions: [{ attributeName: 'pk', attributeType: 'S' }],
        billingMode: 'PAY_PER_REQUEST',
    });
    const [withSort, setWithSort] = useState(false);
    const createMut = useMutation({
        mutationFn: (req) => createTable(req),
        onSuccess: onCreated,
    });
    const handleCreate = () => {
        const req = { ...form };
        if (!withSort) {
            req.keySchema = req.keySchema.filter((k) => k.keyType === 'HASH');
            req.attributeDefinitions = req.attributeDefinitions.filter((a) => req.keySchema.some((k) => k.attributeName === a.attributeName));
        }
        createMut.mutate(req);
    };
    const pkName = form.keySchema.find((k) => k.keyType === 'HASH')?.attributeName ?? 'pk';
    const skName = form.keySchema.find((k) => k.keyType === 'RANGE')?.attributeName ?? 'sk';
    return (_jsx("div", { style: overlayStyle, onClick: onClose, children: _jsxs("div", { style: { ...dialogStyle, minWidth: 420 }, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.25rem' }, children: "Create table" }), _jsx("label", { style: labelStyle, children: "Table name" }), _jsx("input", { autoFocus: true, value: form.tableName, onChange: (e) => setForm({ ...form, tableName: e.target.value }), style: { ...inputStyle, marginBottom: '1rem' }, placeholder: "my-table" }), _jsx("label", { style: labelStyle, children: "Partition key (HASH)" }), _jsxs("div", { style: { display: 'flex', gap: '0.5rem', marginBottom: '0.75rem' }, children: [_jsx("input", { value: pkName, onChange: (e) => setForm({
                                ...form,
                                keySchema: form.keySchema.map((k) => k.keyType === 'HASH' ? { ...k, attributeName: e.target.value } : k),
                                attributeDefinitions: form.attributeDefinitions.map((a) => a.attributeName === pkName ? { ...a, attributeName: e.target.value } : a),
                            }), style: { ...inputStyle, flex: 1 } }), _jsxs("select", { value: form.attributeDefinitions.find((a) => a.attributeName === pkName)?.attributeType ?? 'S', onChange: (e) => setForm({
                                ...form,
                                attributeDefinitions: form.attributeDefinitions.map((a) => a.attributeName === pkName ? { ...a, attributeType: e.target.value } : a),
                            }), style: { ...inputStyle, width: 60 }, children: [_jsx("option", { value: "S", children: "S" }), _jsx("option", { value: "N", children: "N" }), _jsx("option", { value: "B", children: "B" })] })] }), _jsxs("label", { style: { ...labelStyle, display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer', marginBottom: '0.75rem' }, children: [_jsx("input", { type: "checkbox", checked: withSort, onChange: (e) => {
                                setWithSort(e.target.checked);
                                if (e.target.checked) {
                                    setForm({
                                        ...form,
                                        keySchema: [...form.keySchema.filter((k) => k.keyType === 'HASH'), { attributeName: 'sk', keyType: 'RANGE' }],
                                        attributeDefinitions: [...form.attributeDefinitions.filter((a) => a.attributeName !== 'sk'), { attributeName: 'sk', attributeType: 'S' }],
                                    });
                                }
                            } }), "Add sort key (RANGE)"] }), withSort && (_jsxs("div", { style: { display: 'flex', gap: '0.5rem', marginBottom: '1rem' }, children: [_jsx("input", { value: skName, onChange: (e) => setForm({
                                ...form,
                                keySchema: form.keySchema.map((k) => k.keyType === 'RANGE' ? { ...k, attributeName: e.target.value } : k),
                                attributeDefinitions: form.attributeDefinitions.map((a) => a.attributeName === skName ? { ...a, attributeName: e.target.value } : a),
                            }), style: { ...inputStyle, flex: 1 } }), _jsxs("select", { value: form.attributeDefinitions.find((a) => a.attributeName === skName)?.attributeType ?? 'S', onChange: (e) => setForm({
                                ...form,
                                attributeDefinitions: form.attributeDefinitions.map((a) => a.attributeName === skName ? { ...a, attributeType: e.target.value } : a),
                            }), style: { ...inputStyle, width: 60 }, children: [_jsx("option", { value: "S", children: "S" }), _jsx("option", { value: "N", children: "N" }), _jsx("option", { value: "B", children: "B" })] })] })), _jsx("label", { style: labelStyle, children: "Billing mode" }), _jsxs("select", { value: form.billingMode, onChange: (e) => setForm({ ...form, billingMode: e.target.value }), style: { ...inputStyle, marginBottom: '1.25rem' }, children: [_jsx("option", { value: "PAY_PER_REQUEST", children: "On-demand (PAY_PER_REQUEST)" }), _jsx("option", { value: "PROVISIONED", children: "Provisioned" })] }), createMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: createMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: onClose, style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: handleCreate, disabled: !form.tableName || createMut.isPending, style: { ...btnPrimary, opacity: !form.tableName || createMut.isPending ? 0.6 : 1 }, children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) }));
}
const th = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' };
const td = { padding: '0.75rem 1rem' };
const btnPrimary = { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1.25rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em' };
const btnSecondary = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' };
const btnDanger = { background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' };
const btnDelete = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const dialogStyle = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 520, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' };
const labelStyle = { display: 'block', fontSize: '0.85em', fontWeight: 500, marginBottom: '0.35rem', color: '#3d4c5e' };
const inputStyle = { width: '100%', boxSizing: 'border-box', padding: '0.45rem 0.7rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em' };
