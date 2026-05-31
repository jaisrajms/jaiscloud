import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listDashboards, getDashboard, putDashboard, deleteDashboard, } from '../../../api/cloudwatch';
import { EmptyState } from '../../../components/EmptyState';
const DEFAULT_BODY = JSON.stringify({ widgets: [] }, null, 2);
export function CloudWatchDashboards() {
    const [createOpen, setCreateOpen] = useState(false);
    const [newName, setNewName] = useState('');
    const [newBody, setNewBody] = useState(DEFAULT_BODY);
    const [viewDash, setViewDash] = useState(null);
    const [viewBody, setViewBody] = useState('');
    const [deleteTarget, setDeleteTarget] = useState(null);
    const [loadingView, setLoadingView] = useState(false);
    const qc = useQueryClient();
    const { data, isLoading, error } = useQuery({
        queryKey: ['cloudwatch', 'dashboards'],
        queryFn: listDashboards,
    });
    const createMut = useMutation({
        mutationFn: () => putDashboard({ dashboardName: newName, dashboardBody: newBody }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['cloudwatch', 'dashboards'] });
            setCreateOpen(false);
            setNewName('');
            setNewBody(DEFAULT_BODY);
        },
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteDashboard(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['cloudwatch', 'dashboards'] });
            setDeleteTarget(null);
        },
    });
    async function viewDashboard(d) {
        setViewDash(d);
        setLoadingView(true);
        setViewBody('');
        try {
            const full = await getDashboard(d.dashboardName);
            try {
                setViewBody(JSON.stringify(JSON.parse(full.dashboardBody ?? '{}'), null, 2));
            }
            catch {
                setViewBody(full.dashboardBody ?? '');
            }
        }
        catch {
            setViewBody('Failed to load dashboard body');
        }
        finally {
            setLoadingView(false);
        }
    }
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading dashboards\u2026" });
    if (error)
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load: ", error.message] });
    const dashboards = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "CloudWatch Dashboards" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [dashboards.length, " dashboard", dashboards.length !== 1 ? 's' : ''] })] }), _jsx("button", { onClick: () => setCreateOpen(true), style: primaryBtnStyle, children: "Create Dashboard" })] }), dashboards.length === 0 ? (_jsx(EmptyState, { title: "No dashboards found." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Name', 'Last Modified', ''].map(h => (_jsx("th", { style: thStyle, children: h }, h))) }) }), _jsx("tbody", { children: dashboards.map(d => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: tdStyle, children: d.dashboardName }), _jsx("td", { style: tdStyle, children: d.lastModified ?? '—' }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, children: _jsxs("div", { style: { display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => viewDashboard(d), style: actionBtnStyle, children: "View" }), _jsx("button", { onClick: () => setDeleteTarget(d), style: { ...actionBtnStyle, color: '#d13212', borderColor: '#d13212' }, children: "Delete" })] }) })] }, d.dashboardName))) })] })), createOpen && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: { ...dialogStyle, minWidth: 520 }, children: [_jsx("h3", { style: { margin: '0 0 1rem', color: '#e2e8f0' }, children: "Create Dashboard" }), _jsxs("label", { style: labelStyle, children: ["Dashboard Name *", _jsx("input", { type: "text", value: newName, onChange: e => setNewName(e.target.value), style: inputStyle, placeholder: "my-dashboard" })] }), _jsxs("label", { style: labelStyle, children: ["Dashboard Body (JSON)", _jsx("textarea", { value: newBody, onChange: e => setNewBody(e.target.value), rows: 8, style: { ...inputStyle, fontFamily: 'monospace', resize: 'vertical' } })] }), createMut.error && _jsx("div", { style: { color: '#d13212', fontSize: '0.85em' }, children: createMut.error.message }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', marginTop: '1.25rem' }, children: [_jsx("button", { onClick: () => createMut.mutate(), disabled: !newName, style: primaryBtnStyle, children: "Create" }), _jsx("button", { onClick: () => setCreateOpen(false), style: cancelBtnStyle, children: "Cancel" })] })] }) })), viewDash && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: { ...dialogStyle, minWidth: 560, maxWidth: 700 }, children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }, children: [_jsx("h3", { style: { margin: 0, color: '#e2e8f0' }, children: viewDash.dashboardName }), _jsx("button", { onClick: () => setViewDash(null), style: closeBtnStyle, children: "\u2715" })] }), loadingView ? (_jsx("div", { style: { color: '#5f6b7a' }, children: "Loading\u2026" })) : (_jsx("pre", { style: { background: '#0d1a26', border: '1px solid #2d3748', borderRadius: 4, padding: '0.75rem', color: '#c9cdd4', fontSize: '0.82em', overflow: 'auto', maxHeight: 360, margin: 0 }, children: viewBody }))] }) })), deleteTarget && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, children: [_jsx("h3", { style: { margin: '0 0 0.75rem', color: '#e2e8f0' }, children: "Delete Dashboard?" }), _jsxs("p", { style: { color: '#8892a4', fontSize: '0.88em' }, children: ["Delete ", _jsx("strong", { style: { color: '#e2e8f0' }, children: deleteTarget.dashboardName }), "? This cannot be undone."] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', marginTop: '1.25rem' }, children: [_jsx("button", { onClick: () => deleteMut.mutate(deleteTarget.dashboardName), style: { ...primaryBtnStyle, background: '#d13212', borderColor: '#d13212' }, children: "Delete" }), _jsx("button", { onClick: () => setDeleteTarget(null), style: cancelBtnStyle, children: "Cancel" })] })] }) }))] }));
}
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.88em' };
const thStyle = { textAlign: 'left', padding: '0.5rem 0.75rem', color: '#8892a4', fontWeight: 500, borderBottom: '1px solid #2d3748', fontSize: '0.8em', textTransform: 'uppercase', letterSpacing: '0.05em' };
const tdStyle = { padding: '0.6rem 0.75rem', color: '#c9cdd4', verticalAlign: 'middle' };
const primaryBtnStyle = { background: '#0972d3', border: '1px solid #0972d3', borderRadius: 4, color: '#fff', cursor: 'pointer', padding: '0.4rem 1rem', fontSize: '0.85em', fontWeight: 500 };
const cancelBtnStyle = { background: 'none', border: '1px solid #2d3748', borderRadius: 4, color: '#c9cdd4', cursor: 'pointer', padding: '0.4rem 1rem', fontSize: '0.85em' };
const actionBtnStyle = { background: 'none', border: '1px solid #2d3748', borderRadius: 4, color: '#0972d3', cursor: 'pointer', padding: '0.25rem 0.75rem', fontSize: '0.82em' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.5)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 50 };
const dialogStyle = { background: '#1b2530', border: '1px solid #2d3748', borderRadius: 8, padding: '1.75rem', minWidth: 400, maxWidth: 560 };
const labelStyle = { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '0.75rem', color: '#c9cdd4', fontSize: '0.85em' };
const inputStyle = { background: '#0d1a26', border: '1px solid #2d3748', borderRadius: 4, color: '#e2e8f0', padding: '0.4rem 0.75rem', fontSize: '0.9em' };
const closeBtnStyle = { background: 'none', border: 'none', color: '#8892a4', cursor: 'pointer', fontSize: '1rem' };
