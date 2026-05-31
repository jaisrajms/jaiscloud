import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listObjects, deleteObject, deleteObjects, downloadObjectUrl } from '../../../api/s3';
function fmtSize(bytes) {
    if (bytes < 1024)
        return `${bytes} B`;
    if (bytes < 1024 * 1024)
        return `${(bytes / 1024).toFixed(1)} KB`;
    if (bytes < 1024 * 1024 * 1024)
        return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
    return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`;
}
function fmtDate(s) {
    if (!s)
        return '—';
    try {
        return new Date(s).toLocaleString();
    }
    catch {
        return s;
    }
}
export function S3Detail() {
    const { bucket: encodedBucket } = useParams();
    const bucket = decodeURIComponent(encodedBucket ?? '');
    const navigate = useNavigate();
    const qc = useQueryClient();
    const [prefix, setPrefix] = useState('');
    const [prefixInput, setPrefixInput] = useState('');
    const [selected, setSelected] = useState(new Set());
    const [confirmDelete, setConfirmDelete] = useState(null);
    const { data, isLoading, error } = useQuery({
        queryKey: ['s3', 'objects', bucket, prefix],
        queryFn: () => listObjects(bucket, { prefix, delimiter: '/' }),
    });
    const deleteMut = useMutation({
        mutationFn: (key) => deleteObject(bucket, key),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['s3', 'objects', bucket] });
            setConfirmDelete(null);
        },
    });
    const deleteBatchMut = useMutation({
        mutationFn: (keys) => deleteObjects(bucket, keys),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['s3', 'objects', bucket] });
            setSelected(new Set());
        },
    });
    const objects = data?.items ?? [];
    const prefixes = data?.commonPrefixes ?? [];
    const toggleSelect = (key) => {
        setSelected((prev) => {
            const next = new Set(prev);
            if (next.has(key))
                next.delete(key);
            else
                next.add(key);
            return next;
        });
    };
    const navigatePrefix = (p) => {
        setPrefix(p);
        setPrefixInput(p);
        setSelected(new Set());
    };
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1.5rem' }, children: [_jsx("button", { onClick: () => navigate('/aws/s3'), style: btnBack, children: "\u2190 Buckets" }), _jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: bucket })] }), _jsxs("div", { style: { display: 'flex', gap: '0.5rem', marginBottom: '1rem', alignItems: 'center' }, children: [_jsx("input", { value: prefixInput, onChange: (e) => setPrefixInput(e.target.value), onKeyDown: (e) => e.key === 'Enter' && navigatePrefix(prefixInput), placeholder: "Filter by prefix\u2026", style: { ...inputStyle, flex: 1 } }), _jsx("button", { onClick: () => navigatePrefix(prefixInput), style: btnSecondary, children: "Go" }), prefix && (_jsx("button", { onClick: () => navigatePrefix(''), style: btnSecondary, children: "Clear" })), selected.size > 0 && (_jsxs("button", { onClick: () => deleteBatchMut.mutate(Array.from(selected)), disabled: deleteBatchMut.isPending, style: { ...btnDanger, opacity: deleteBatchMut.isPending ? 0.6 : 1 }, children: ["Delete ", selected.size, " selected"] }))] }), prefix && (_jsxs("div", { style: { marginBottom: '0.75rem', fontSize: '0.85em', color: '#5f6b7a' }, children: [_jsx("span", { style: { cursor: 'pointer', color: '#0972d3' }, onClick: () => navigatePrefix(''), children: bucket }), prefix.split('/').filter(Boolean).map((part, i, arr) => {
                        const p = arr.slice(0, i + 1).join('/') + '/';
                        return (_jsxs("span", { children: [' / ', _jsx("span", { style: { cursor: 'pointer', color: i < arr.length - 1 ? '#0972d3' : '#3d4c5e' }, onClick: () => i < arr.length - 1 && navigatePrefix(p), children: part })] }, p));
                    })] })), isLoading && _jsx("div", { style: { color: '#5f6b7a', padding: '1rem' }, children: "Loading objects\u2026" }), error && _jsxs("div", { style: { color: '#d13212', padding: '1rem' }, children: ["Error: ", error.message] }), !isLoading && !error && prefixes.length === 0 && objects.length === 0 && (_jsxs("div", { style: { padding: '3rem', textAlign: 'center', color: '#8d9daa', border: '1px solid #e7e9ec', borderRadius: 8 }, children: ["No objects", prefix ? ` with prefix "${prefix}"` : ' in this bucket', "."] })), (prefixes.length > 0 || objects.length > 0) && (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: { ...th, width: 36 }, children: _jsx("input", { type: "checkbox", checked: selected.size === objects.length && objects.length > 0, onChange: (e) => setSelected(e.target.checked ? new Set(objects.map((o) => o.key)) : new Set()) }) }), _jsx("th", { style: th, children: "Key" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Size" }), _jsx("th", { style: th, children: "Last Modified" }), _jsx("th", { style: { ...th, width: 120 } })] }) }), _jsxs("tbody", { children: [prefixes.map((p) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec', cursor: 'pointer', background: '#fffbe6' }, onClick: () => navigatePrefix(p), onMouseEnter: (e) => (e.currentTarget.style.background = '#fff8e0'), onMouseLeave: (e) => (e.currentTarget.style.background = '#fffbe6'), children: [_jsx("td", { style: td }), _jsx("td", { style: td, children: _jsxs("span", { style: { color: '#0972d3' }, children: ["\uD83D\uDCC1 ", p.replace(prefix, '')] }) }), _jsx("td", { style: td, colSpan: 3 })] }, p))), objects.map((obj) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec' }, onMouseEnter: (e) => (e.currentTarget.style.background = '#fafbfc'), onMouseLeave: (e) => (e.currentTarget.style.background = ''), children: [_jsx("td", { style: td, children: _jsx("input", { type: "checkbox", checked: selected.has(obj.key), onChange: () => toggleSelect(obj.key) }) }), _jsx("td", { style: td, children: _jsx("span", { style: { fontFamily: 'monospace', fontSize: '0.88em' }, children: obj.key.replace(prefix, '') }) }), _jsx("td", { style: { ...td, textAlign: 'right', color: '#5f6b7a', fontVariantNumeric: 'tabular-nums' }, children: fmtSize(obj.size) }), _jsx("td", { style: { ...td, color: '#5f6b7a' }, children: fmtDate(obj.lastModified) }), _jsxs("td", { style: { ...td, display: 'flex', gap: '0.4rem' }, children: [_jsx("a", { href: downloadObjectUrl(bucket, obj.key), download: true, style: btnSmall, onClick: (e) => e.stopPropagation(), children: "Download" }), _jsx("button", { onClick: (e) => { e.stopPropagation(); setConfirmDelete(obj); }, style: btnDelete, children: "Delete" })] })] }, obj.key)))] })] }) })), data?.isTruncated && (_jsx("div", { style: { marginTop: '0.75rem', textAlign: 'center', color: '#5f6b7a', fontSize: '0.85em' }, children: "More objects available \u2014 use prefix filter to narrow results." })), confirmDelete && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 0.75rem', fontSize: '1.05rem' }, children: "Delete object?" }), _jsxs("p", { style: { margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Permanently delete ", _jsx("strong", { style: { wordBreak: 'break-all' }, children: confirmDelete.key }), "?"] }), deleteMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: deleteMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => setConfirmDelete(null), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => deleteMut.mutate(confirmDelete.key), disabled: deleteMut.isPending, style: { ...btnDanger, opacity: deleteMut.isPending ? 0.6 : 1 }, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
const th = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' };
const td = { padding: '0.6rem 1rem' };
const btnBack = { background: 'none', border: 'none', color: '#0972d3', cursor: 'pointer', fontSize: '0.9em', padding: '0.25rem 0' };
const btnSecondary = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.4rem 0.9rem', cursor: 'pointer', fontSize: '0.88em' };
const btnDanger = { background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.4rem 0.9rem', cursor: 'pointer', fontSize: '0.88em' };
const btnDelete = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.2rem 0.55rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' };
const btnSmall = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.2rem 0.55rem', cursor: 'pointer', fontSize: '0.8em', color: '#0972d3', textDecoration: 'none' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const dialogStyle = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 500, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' };
const inputStyle = { padding: '0.4rem 0.75rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.88em' };
