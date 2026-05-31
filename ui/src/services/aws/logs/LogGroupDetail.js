import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useParams, Link, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { listLogStreams } from '../../../api/logs';
function fmtTs(ms) {
    if (!ms)
        return '—';
    return new Date(ms).toLocaleString();
}
export function LogGroupDetail() {
    const { name: encodedName } = useParams();
    const groupName = encodedName ? decodeURIComponent(encodedName) : '';
    const navigate = useNavigate();
    const { data, isLoading, error } = useQuery({
        queryKey: ['logs', 'streams', groupName],
        queryFn: () => listLogStreams(groupName),
        enabled: !!groupName,
    });
    const streams = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { marginBottom: '1.5rem' }, children: [_jsx(Link, { to: "/aws/logs/groups", style: { color: '#0972d3', fontSize: '0.85em', textDecoration: 'none' }, children: "\u2190 Log Groups" }), _jsx("h2", { style: { margin: '0.5rem 0 0', fontSize: '1.3rem', fontWeight: 600, fontFamily: 'monospace' }, children: groupName })] }), _jsx("h4", { style: { margin: '0 0 0.75rem', fontSize: '0.95rem', fontWeight: 600 }, children: "Log Streams" }), isLoading && _jsx("div", { style: { color: '#5f6b7a', fontSize: '0.9em' }, children: "Loading streams\u2026" }), error && _jsx("div", { style: { color: '#d13212', fontSize: '0.9em' }, children: error.message }), !isLoading && streams.length === 0 && (_jsx("p", { style: { color: '#5f6b7a', fontSize: '0.9em', fontStyle: 'italic' }, children: "No log streams in this group." })), streams.length > 0 && (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Stream name" }), _jsx("th", { style: th, children: "First event" }), _jsx("th", { style: th, children: "Last event" })] }) }), _jsx("tbody", { children: streams.map((s) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }, onClick: () => navigate(`/aws/logs/groups/${encodeURIComponent(groupName)}/streams/${encodeURIComponent(s.name)}`), onMouseEnter: (e) => (e.currentTarget.style.background = '#fafbfc'), onMouseLeave: (e) => (e.currentTarget.style.background = ''), children: [_jsx("td", { style: td, children: _jsx("span", { style: { color: '#0972d3', fontFamily: 'monospace', fontSize: '0.88em' }, children: s.name }) }), _jsx("td", { style: { ...td, color: '#5f6b7a' }, children: fmtTs(s.firstEventAt) }), _jsx("td", { style: { ...td, color: '#5f6b7a' }, children: fmtTs(s.lastEventAt) })] }, s.name))) })] }) }))] }));
}
const th = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' };
const td = { padding: '0.75rem 1rem' };
