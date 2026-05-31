import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getAdminStatus, resetState, getClock, setClock, listSnapshots, createSnapshot, revertSnapshot, deleteSnapshot, } from '../api/admin';
export function AdminPanel() {
    const qc = useQueryClient();
    const { data: status } = useQuery({
        queryKey: ['admin', 'status'],
        queryFn: getAdminStatus,
        refetchInterval: 10000,
    });
    const { data: clock } = useQuery({
        queryKey: ['admin', 'clock'],
        queryFn: getClock,
        refetchInterval: 5000,
    });
    const { data: snapshotsData } = useQuery({
        queryKey: ['admin', 'snapshots'],
        queryFn: listSnapshots,
    });
    return (_jsxs("div", { children: [_jsx("h2", { style: { margin: '0 0 1.5rem', fontSize: '1.4rem', fontWeight: 600 }, children: "Admin Panel" }), _jsxs("div", { style: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.25rem' }, children: [_jsx(StatusCard, { status: status, onRefresh: () => void qc.invalidateQueries({ queryKey: ['admin'] }) }), _jsx(ClockCard, { clock: clock, onChanged: () => void qc.invalidateQueries({ queryKey: ['admin', 'clock'] }) }), _jsx(ResetCard, {}), _jsx(ExportCard, {})] }), _jsx("h3", { style: { margin: '2rem 0 0.75rem', fontSize: '1.05rem', fontWeight: 600 }, children: "Named Snapshots" }), _jsx(SnapshotsSection, { snapshots: snapshotsData?.snapshots ?? [], onChanged: () => void qc.invalidateQueries({ queryKey: ['admin', 'snapshots'] }) })] }));
}
function StatusCard({ status, onRefresh }) {
    return (_jsxs("div", { style: card, children: [_jsxs("div", { style: cardHeader, children: [_jsx("span", { style: cardTitle, children: "Status" }), _jsx("button", { onClick: onRefresh, style: btnSmall, children: "Refresh" })] }), status ? (_jsxs("dl", { style: dl, children: [_jsx("dt", { style: dt, children: "Status" }), _jsx("dd", { style: { ...dd, color: status.status === 'ok' ? '#1d6b2e' : '#d13212', fontWeight: 600 }, children: status.status }), _jsx("dt", { style: dt, children: "Cloud" }), _jsx("dd", { style: dd, children: status.cloud }), status.backend && _jsxs(_Fragment, { children: [_jsx("dt", { style: dt, children: "Backend" }), _jsx("dd", { style: dd, children: status.backend })] }), _jsx("dt", { style: dt, children: "Snapshotters" }), _jsx("dd", { style: dd, children: status.snapshotters?.length ? status.snapshotters.join(', ') : '—' })] })) : (_jsx("p", { style: { color: '#8d9daa', margin: 0 }, children: "Loading\u2026" }))] }));
}
function ClockCard({ clock, onChanged }) {
    const [mode, setMode] = useState('real');
    const [timeStr, setTimeStr] = useState('');
    const [open, setOpen] = useState(false);
    const setMut = useMutation({
        mutationFn: (req) => setClock(req),
        onSuccess: () => {
            onChanged();
            setOpen(false);
        },
    });
    const handleSet = () => {
        const req = { mode };
        if (mode !== 'real')
            req.time = timeStr;
        setMut.mutate(req);
    };
    return (_jsxs("div", { style: card, children: [_jsxs("div", { style: cardHeader, children: [_jsx("span", { style: cardTitle, children: "Clock" }), _jsx("button", { onClick: () => { setOpen(true); setMode(clock?.mode ?? 'real'); setTimeStr(clock?.time ?? ''); }, style: btnSmall, children: "Set clock" })] }), clock ? (_jsxs("dl", { style: dl, children: [_jsx("dt", { style: dt, children: "Mode" }), _jsx("dd", { style: dd, children: _jsx("span", { style: {
                                display: 'inline-block', padding: '0.15em 0.55em', borderRadius: 3,
                                fontSize: '0.82em', fontWeight: 500,
                                background: clock.mode === 'real' ? '#e0f9e0' : '#fff8e0',
                                color: clock.mode === 'real' ? '#1d6b2e' : '#8a6500',
                            }, children: clock.mode }) }), clock.time && _jsxs(_Fragment, { children: [_jsx("dt", { style: dt, children: "Time" }), _jsx("dd", { style: { ...dd, fontFamily: 'monospace', fontSize: '0.85em' }, children: clock.time })] })] })) : (_jsx("p", { style: { color: '#8d9daa', margin: 0 }, children: "Loading\u2026" })), open && (_jsx("div", { style: overlayStyle, onClick: () => setOpen(false), children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem' }, children: "Set clock mode" }), _jsx("label", { style: labelStyle, children: "Mode" }), _jsxs("select", { value: mode, onChange: (e) => setMode(e.target.value), style: { ...inputStyle, marginBottom: '0.75rem' }, children: [_jsx("option", { value: "real", children: "real (wall clock)" }), _jsx("option", { value: "fixed", children: "fixed (frozen time)" }), _jsx("option", { value: "offset", children: "offset (shifted time)" })] }), mode !== 'real' && (_jsxs(_Fragment, { children: [_jsx("label", { style: labelStyle, children: "Time (RFC3339, e.g. 2025-01-01T00:00:00Z)" }), _jsx("input", { autoFocus: true, value: timeStr, onChange: (e) => setTimeStr(e.target.value), style: { ...inputStyle, marginBottom: '1rem' }, placeholder: "2025-01-01T00:00:00Z" })] })), setMut.error && _jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: setMut.error.message }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => setOpen(false), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: handleSet, disabled: setMut.isPending, style: { ...btnPrimary, opacity: setMut.isPending ? 0.6 : 1 }, children: setMut.isPending ? 'Setting…' : 'Set' })] })] }) }))] }));
}
function ResetCard() {
    const [confirm, setConfirm] = useState(false);
    const qc = useQueryClient();
    const resetMut = useMutation({
        mutationFn: resetState,
        onSuccess: () => {
            void qc.invalidateQueries();
            setConfirm(false);
        },
    });
    return (_jsxs("div", { style: card, children: [_jsx("div", { style: cardHeader, children: _jsx("span", { style: cardTitle, children: "Reset State" }) }), _jsx("p", { style: { color: '#5f6b7a', fontSize: '0.88em', margin: '0 0 1rem' }, children: "Wipe all emulator state. This is irreversible \u2014 all resources will be deleted." }), !confirm ? (_jsx("button", { onClick: () => setConfirm(true), style: btnDanger, children: "Reset all state\u2026" })) : (_jsxs("div", { children: [_jsx("p", { style: { color: '#d13212', fontWeight: 500, margin: '0 0 0.75rem', fontSize: '0.9em' }, children: "Are you sure? This will delete ALL resources." }), resetMut.error && _jsx("p", { style: { color: '#d13212', margin: '0 0 0.5rem', fontSize: '0.85em' }, children: resetMut.error.message }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem' }, children: [_jsx("button", { onClick: () => setConfirm(false), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => resetMut.mutate(), disabled: resetMut.isPending, style: { ...btnDanger, opacity: resetMut.isPending ? 0.6 : 1 }, children: resetMut.isPending ? 'Resetting…' : 'Yes, reset' })] })] }))] }));
}
function ExportCard() {
    return (_jsxs("div", { style: card, children: [_jsx("div", { style: cardHeader, children: _jsx("span", { style: cardTitle, children: "Export / Import" }) }), _jsx("p", { style: { color: '#5f6b7a', fontSize: '0.88em', margin: '0 0 1rem' }, children: "Export state as a gzip tarball or import a snapshot via the CLI." }), _jsx("a", { href: "/_jaiscloud/export", download: "jaiscloud-state.tar.gz", style: { ...btnPrimary, textDecoration: 'none', display: 'inline-block' }, children: "Download export" })] }));
}
function SnapshotsSection({ snapshots, onChanged }) {
    const [createOpen, setCreateOpen] = useState(false);
    const [name, setName] = useState('');
    const [desc, setDesc] = useState('');
    const [confirmRevert, setConfirmRevert] = useState(null);
    const [confirmDel, setConfirmDel] = useState(null);
    const createMut = useMutation({
        mutationFn: () => createSnapshot({ name, description: desc }),
        onSuccess: () => {
            onChanged();
            setCreateOpen(false);
            setName('');
            setDesc('');
        },
    });
    const revertMut = useMutation({
        mutationFn: (n) => revertSnapshot(n),
        onSuccess: () => {
            onChanged();
            setConfirmRevert(null);
        },
    });
    const deleteMut = useMutation({
        mutationFn: (n) => deleteSnapshot(n),
        onSuccess: () => {
            onChanged();
            setConfirmDel(null);
        },
    });
    return (_jsxs("div", { children: [_jsx("div", { style: { display: 'flex', justifyContent: 'flex-end', marginBottom: '0.75rem' }, children: _jsx("button", { onClick: () => setCreateOpen(true), style: btnSecondary, children: "Create snapshot" }) }), snapshots.length === 0 ? (_jsx("div", { style: { padding: '2rem', textAlign: 'center', color: '#8d9daa', border: '1px solid #e7e9ec', borderRadius: 8 }, children: "No named snapshots. Create one to save the current state." })) : (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Name" }), _jsx("th", { style: th, children: "Description" }), _jsx("th", { style: th, children: "Created" }), _jsx("th", { style: { ...th, width: 160 } })] }) }), _jsx("tbody", { children: snapshots.map((s) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec' }, children: [_jsx("td", { style: td, children: _jsx("span", { style: { fontWeight: 500 }, children: s.name }) }), _jsx("td", { style: { ...td, color: '#5f6b7a' }, children: s.description ?? '—' }), _jsx("td", { style: { ...td, color: '#8d9daa', fontSize: '0.85em' }, children: s.createdAt ? new Date(s.createdAt).toLocaleString() : '—' }), _jsxs("td", { style: { ...td, display: 'flex', gap: '0.4rem' }, children: [_jsx("button", { onClick: () => setConfirmRevert(s), style: btnSmall, children: "Revert" }), _jsx("button", { onClick: () => setConfirmDel(s), style: btnDelete, children: "Delete" })] })] }, s.name))) })] }) })), createOpen && (_jsx("div", { style: overlayStyle, onClick: () => setCreateOpen(false), children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem' }, children: "Create snapshot" }), _jsx("label", { style: labelStyle, children: "Name" }), _jsx("input", { autoFocus: true, value: name, onChange: (e) => setName(e.target.value), style: { ...inputStyle, marginBottom: '0.75rem' }, placeholder: "my-snapshot" }), _jsx("label", { style: labelStyle, children: "Description (optional)" }), _jsx("input", { value: desc, onChange: (e) => setDesc(e.target.value), style: { ...inputStyle, marginBottom: '1rem' }, placeholder: "Before the big test\u2026" }), createMut.error && _jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: createMut.error.message }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => setCreateOpen(false), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => createMut.mutate(), disabled: !name || createMut.isPending, style: { ...btnPrimary, opacity: !name || createMut.isPending ? 0.6 : 1 }, children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), confirmRevert && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 0.75rem', fontSize: '1.05rem' }, children: "Revert to snapshot?" }), _jsxs("p", { style: { margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Current state will be replaced with ", _jsx("strong", { children: confirmRevert.name }), ". This cannot be undone."] }), revertMut.error && _jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: revertMut.error.message }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => setConfirmRevert(null), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => revertMut.mutate(confirmRevert.name), disabled: revertMut.isPending, style: { ...btnDanger, opacity: revertMut.isPending ? 0.6 : 1 }, children: revertMut.isPending ? 'Reverting…' : 'Revert' })] })] }) })), confirmDel && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 0.75rem', fontSize: '1.05rem' }, children: "Delete snapshot?" }), _jsxs("p", { style: { margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Permanently delete snapshot ", _jsx("strong", { children: confirmDel.name }), "?"] }), deleteMut.error && _jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: deleteMut.error.message }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => setConfirmDel(null), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => deleteMut.mutate(confirmDel.name), disabled: deleteMut.isPending, style: { ...btnDanger, opacity: deleteMut.isPending ? 0.6 : 1 }, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
const card = { background: '#fff', border: '1px solid #e7e9ec', borderRadius: 8, padding: '1.25rem' };
const cardHeader = { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' };
const cardTitle = { fontWeight: 600, fontSize: '1rem' };
const dl = { margin: 0, display: 'grid', gridTemplateColumns: 'auto 1fr', gap: '0.3rem 1rem', alignItems: 'baseline' };
const dt = { fontSize: '0.82em', color: '#8d9daa', fontWeight: 500, textTransform: 'uppercase', letterSpacing: '0.03em', whiteSpace: 'nowrap' };
const dd = { margin: 0, fontSize: '0.9em' };
const th = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' };
const td = { padding: '0.75rem 1rem' };
const btnPrimary = { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.45rem 1.1rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em' };
const btnSecondary = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.4rem 0.9rem', cursor: 'pointer', fontSize: '0.88em' };
const btnDanger = { background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.45rem 1.1rem', cursor: 'pointer', fontSize: '0.9em' };
const btnSmall = { background: '#f4f5f7', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#3d4c5e' };
const btnDelete = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const dialogStyle = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 480, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' };
const labelStyle = { display: 'block', fontSize: '0.85em', fontWeight: 500, marginBottom: '0.35rem', color: '#3d4c5e' };
const inputStyle = { width: '100%', boxSizing: 'border-box', padding: '0.5rem 0.75rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em' };
