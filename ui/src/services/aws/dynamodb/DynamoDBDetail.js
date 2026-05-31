import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { scanTable, deleteItem, putItem } from '../../../api/dynamodb';
function renderValue(v) {
    if (v === null || v === undefined)
        return '—';
    if (typeof v === 'object')
        return JSON.stringify(v);
    return String(v);
}
export function DynamoDBDetail() {
    const { table: encodedTable } = useParams();
    const table = decodeURIComponent(encodedTable ?? '');
    const navigate = useNavigate();
    const qc = useQueryClient();
    const [limit, setLimit] = useState(50);
    const [nextToken, setNextToken] = useState();
    const [page, setPage] = useState(0);
    const [pages, setPages] = useState([undefined]);
    const [editItem, setEditItem] = useState(null);
    const [editJson, setEditJson] = useState('');
    const [editErr, setEditErr] = useState('');
    const [confirmDelete, setConfirmDelete] = useState(null);
    const [jsonErr, setJsonErr] = useState('');
    const currentToken = pages[page];
    const { data, isLoading, error } = useQuery({
        queryKey: ['dynamodb', 'scan', table, currentToken, limit],
        queryFn: () => scanTable(table, { limit, nextToken: currentToken }),
    });
    const deleteMut = useMutation({
        mutationFn: (key) => deleteItem(table, key),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['dynamodb', 'scan', table] });
            setConfirmDelete(null);
        },
    });
    const putMut = useMutation({
        mutationFn: (item) => putItem(table, item),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['dynamodb', 'scan', table] });
            setEditItem(null);
        },
    });
    const items = data?.items ?? [];
    // Derive columns from first few items
    const columns = Array.from(items.slice(0, 20).reduce((cols, item) => {
        Object.keys(item).forEach((k) => cols.add(k));
        return cols;
    }, new Set())).slice(0, 12);
    const goNext = () => {
        if (data?.lastEvaluatedKey) {
            const tok = JSON.stringify(data.lastEvaluatedKey);
            const next = page + 1;
            if (next >= pages.length)
                setPages([...pages, tok]);
            setPage(next);
            setNextToken(tok);
        }
    };
    const goPrev = () => {
        if (page > 0) {
            const prev = page - 1;
            setPage(prev);
            setNextToken(pages[prev]);
        }
    };
    const openEdit = (item) => {
        setEditItem(item);
        setEditJson(JSON.stringify(item, null, 2));
        setEditErr('');
    };
    const handleSave = () => {
        try {
            const parsed = JSON.parse(editJson);
            setEditErr('');
            putMut.mutate(parsed);
        }
        catch {
            setEditErr('Invalid JSON');
        }
    };
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1.5rem' }, children: [_jsx("button", { onClick: () => navigate('/aws/dynamodb'), style: btnBack, children: "\u2190 Tables" }), _jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: table }), _jsx("button", { onClick: () => { setEditJson('{}'); setEditItem({}); setEditErr(''); }, style: { ...btnPrimary, marginLeft: 'auto' }, children: "Put Item" })] }), _jsxs("div", { style: { display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1rem', fontSize: '0.88em', color: '#5f6b7a' }, children: [_jsx("span", { children: data ? `${data.count} / ${data.scannedCount} scanned` : '…' }), _jsxs("label", { style: { display: 'flex', alignItems: 'center', gap: '0.4rem' }, children: ["Rows", _jsx("select", { value: limit, onChange: (e) => { setLimit(Number(e.target.value)); setPage(0); setPages([undefined]); }, style: { padding: '0.2rem 0.4rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em' }, children: [25, 50, 100, 250].map((n) => _jsx("option", { value: n, children: n }, n)) })] })] }), isLoading && _jsx("div", { style: { color: '#5f6b7a', padding: '1rem' }, children: "Scanning\u2026" }), error && _jsxs("div", { style: { color: '#d13212', padding: '1rem' }, children: ["Error: ", error.message] }), !isLoading && !error && items.length === 0 && (_jsx("div", { style: { padding: '3rem', textAlign: 'center', color: '#8d9daa', border: '1px solid #e7e9ec', borderRadius: 8 }, children: "No items in this table." })), items.length > 0 && (_jsx("div", { style: { overflowX: 'auto', border: '1px solid #e7e9ec', borderRadius: 8 }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.85em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [columns.map((col) => (_jsx("th", { style: th, children: col }, col))), _jsx("th", { style: { ...th, width: 80 } })] }) }), _jsx("tbody", { children: items.map((item, i) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }, onClick: () => openEdit(item), onMouseEnter: (e) => (e.currentTarget.style.background = '#fafbfc'), onMouseLeave: (e) => (e.currentTarget.style.background = ''), children: [columns.map((col) => (_jsx("td", { style: { ...td, maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }, children: renderDynamo(item[col]) }, col))), _jsx("td", { style: td, onClick: (e) => e.stopPropagation(), children: _jsx("button", { onClick: () => setConfirmDelete(item), style: btnDelete, children: "Delete" }) })] }, i))) })] }) })), (page > 0 || data?.lastEvaluatedKey) && (_jsxs("div", { style: { display: 'flex', gap: '0.5rem', justifyContent: 'flex-end', marginTop: '0.75rem' }, children: [_jsx("button", { onClick: goPrev, disabled: page === 0, style: btnSecondary, children: "\u2190 Prev" }), _jsx("button", { onClick: goNext, disabled: !data?.lastEvaluatedKey, style: btnSecondary, children: "Next \u2192" })] })), editItem !== null && (_jsx("div", { style: overlayStyle, onClick: () => setEditItem(null), children: _jsxs("div", { style: { ...dialogStyle, minWidth: 480 }, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem' }, children: Object.keys(editItem).length === 0 ? 'Put Item' : 'Edit Item' }), _jsx("p", { style: { margin: '0 0 0.5rem', fontSize: '0.8em', color: '#5f6b7a' }, children: "Edit as DynamoDB JSON ({\"pk\": {\"S\": \"value\"}, ...})" }), _jsx("textarea", { value: editJson, onChange: (e) => setEditJson(e.target.value), style: { width: '100%', boxSizing: 'border-box', height: 240, fontFamily: 'monospace', fontSize: '0.85em', padding: '0.5rem', border: '1px solid #c9cdd4', borderRadius: 4, resize: 'vertical' } }), editErr && _jsx("p", { style: { color: '#d13212', margin: '0.25rem 0 0', fontSize: '0.85em' }, children: editErr }), putMut.error && _jsx("p", { style: { color: '#d13212', margin: '0.25rem 0 0', fontSize: '0.85em' }, children: putMut.error.message }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1rem' }, children: [_jsx("button", { onClick: () => setEditItem(null), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: handleSave, disabled: putMut.isPending, style: { ...btnPrimary, opacity: putMut.isPending ? 0.6 : 1 }, children: putMut.isPending ? 'Saving…' : 'Save' })] })] }) })), confirmDelete && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 0.75rem', fontSize: '1.05rem' }, children: "Delete item?" }), _jsx("p", { style: { margin: '0 0 0.5rem', color: '#5f6b7a', fontSize: '0.9em' }, children: "Enter the key for this item to delete it." }), _jsx("textarea", { value: jsonErr || JSON.stringify(extractKey(confirmDelete), null, 2), onChange: (e) => setJsonErr(e.target.value), style: { width: '100%', boxSizing: 'border-box', height: 80, fontFamily: 'monospace', fontSize: '0.82em', padding: '0.5rem', border: '1px solid #c9cdd4', borderRadius: 4 } }), deleteMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0.25rem 0 0.75rem', fontSize: '0.85em' }, children: deleteMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '0.75rem' }, children: [_jsx("button", { onClick: () => setConfirmDelete(null), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => deleteMut.mutate(confirmDelete), disabled: deleteMut.isPending, style: { ...btnDanger, opacity: deleteMut.isPending ? 0.6 : 1 }, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
function renderDynamo(val) {
    if (val === undefined || val === null)
        return '—';
    if (typeof val === 'object') {
        const obj = val;
        // DynamoDB typed attribute: { S: "..." } | { N: "..." } | { BOOL: true } ...
        if ('S' in obj)
            return String(obj['S']);
        if ('N' in obj)
            return String(obj['N']);
        if ('BOOL' in obj)
            return String(obj['BOOL']);
        if ('NULL' in obj)
            return 'null';
        if ('L' in obj)
            return `[${obj['L'].length} items]`;
        if ('M' in obj)
            return `{${Object.keys(obj['M']).length} keys}`;
        return renderValue(val);
    }
    return String(val);
}
function extractKey(item) {
    // Heuristic: keep only fields that look like partition/sort key candidates
    return item;
}
const th = { padding: '0.55rem 0.75rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.78em', textTransform: 'uppercase', letterSpacing: '0.03em', whiteSpace: 'nowrap' };
const td = { padding: '0.55rem 0.75rem' };
const btnBack = { background: 'none', border: 'none', color: '#0972d3', cursor: 'pointer', fontSize: '0.9em', padding: '0.25rem 0' };
const btnPrimary = { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.45rem 1.1rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em' };
const btnSecondary = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.4rem 0.9rem', cursor: 'pointer', fontSize: '0.88em' };
const btnDanger = { background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.4rem 0.9rem', cursor: 'pointer', fontSize: '0.88em' };
const btnDelete = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.2rem 0.55rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const dialogStyle = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 520, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' };
