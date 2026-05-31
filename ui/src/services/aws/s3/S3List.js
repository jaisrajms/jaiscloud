import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { listBuckets, createBucket, deleteBucket } from '../../../api/s3';
import { EmptyState } from '../../../components/EmptyState';
export function S3List() {
    const [createOpen, setCreateOpen] = useState(false);
    const [newName, setNewName] = useState('');
    const [confirmDelete, setConfirmDelete] = useState(null);
    const qc = useQueryClient();
    const navigate = useNavigate();
    const { data, isLoading, error } = useQuery({
        queryKey: ['s3', 'buckets'],
        queryFn: () => listBuckets(),
    });
    const createMut = useMutation({
        mutationFn: () => createBucket({ name: newName }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['s3', 'buckets'] });
            setCreateOpen(false);
            setNewName('');
        },
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteBucket(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['s3', 'buckets'] });
            setConfirmDelete(null);
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading buckets\u2026" });
    if (error)
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load: ", error.message] });
    const buckets = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "S3 Buckets" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [buckets.length, " bucket", buckets.length !== 1 ? 's' : ''] })] }), _jsx("button", { onClick: () => setCreateOpen(true), style: btnPrimary, children: "Create bucket" })] }), buckets.length === 0 ? (_jsx(EmptyState, { title: "No buckets", description: "S3 buckets store your objects and files.", cta: "Create Bucket", onCta: () => setCreateOpen(true) })) : (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Name" }), _jsx("th", { style: th, children: "Region" }), _jsx("th", { style: th, children: "Versioning" }), _jsx("th", { style: { ...th, width: 80 } })] }) }), _jsx("tbody", { children: buckets.map((b) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }, onClick: () => navigate(`/aws/s3/${encodeURIComponent(b.name)}`), onMouseEnter: (e) => (e.currentTarget.style.background = '#fafbfc'), onMouseLeave: (e) => (e.currentTarget.style.background = ''), children: [_jsx("td", { style: td, children: _jsx("span", { style: { color: '#0972d3', fontWeight: 500 }, children: b.name }) }), _jsx("td", { style: { ...td, color: '#5f6b7a' }, children: b.region }), _jsx("td", { style: td, children: _jsx("span", { style: {
                                                display: 'inline-block', padding: '0.15em 0.55em', borderRadius: 3,
                                                fontSize: '0.8em', fontWeight: 500,
                                                background: b.versioning === 'Enabled' ? '#e0f9e0' : '#f4f5f7',
                                                color: b.versioning === 'Enabled' ? '#1d6b2e' : '#5f6b7a',
                                            }, children: b.versioning }) }), _jsx("td", { style: td, onClick: (e) => e.stopPropagation(), children: _jsx("button", { onClick: () => setConfirmDelete(b), style: btnDelete, children: "Delete" }) })] }, b.name))) })] }) })), createOpen && (_jsx("div", { style: overlayStyle, onClick: () => setCreateOpen(false), children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem' }, children: "Create bucket" }), _jsx("label", { style: labelStyle, children: "Bucket name" }), _jsx("input", { autoFocus: true, value: newName, onChange: (e) => setNewName(e.target.value), onKeyDown: (e) => e.key === 'Enter' && newName && createMut.mutate(), style: inputStyle, placeholder: "my-bucket" }), createMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0.5rem 0 0', fontSize: '0.85em' }, children: createMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1.25rem' }, children: [_jsx("button", { onClick: () => setCreateOpen(false), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => createMut.mutate(), disabled: !newName || createMut.isPending, style: { ...btnPrimary, opacity: !newName || createMut.isPending ? 0.6 : 1 }, children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), confirmDelete && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 0.75rem', fontSize: '1.05rem' }, children: "Delete bucket?" }), _jsxs("p", { style: { margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Permanently delete ", _jsx("strong", { children: confirmDelete.name }), "? The bucket must be empty."] }), deleteMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: deleteMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => setConfirmDelete(null), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => deleteMut.mutate(confirmDelete.name), disabled: deleteMut.isPending, style: { ...btnDanger, opacity: deleteMut.isPending ? 0.6 : 1 }, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
const th = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' };
const td = { padding: '0.75rem 1rem' };
const btnPrimary = { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1.25rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em' };
const btnSecondary = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' };
const btnDanger = { background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' };
const btnDelete = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const dialogStyle = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 460, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' };
const labelStyle = { display: 'block', fontSize: '0.85em', fontWeight: 500, marginBottom: '0.4rem', color: '#3d4c5e' };
const inputStyle = { width: '100%', boxSizing: 'border-box', padding: '0.5rem 0.75rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em' };
