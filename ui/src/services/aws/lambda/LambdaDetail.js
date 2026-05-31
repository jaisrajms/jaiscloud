import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Fragment, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { getFunction } from '../../../api/lambda';
import { listLogStreams, getLogEvents } from '../../../api/logs';
import { LambdaTest } from './LambdaTest';
export function LambdaDetail() {
    const { name: encodedName } = useParams();
    const name = encodedName ? decodeURIComponent(encodedName) : '';
    const [tab, setTab] = useState('test');
    const { data: fn, isLoading, error } = useQuery({
        queryKey: ['lambda', 'function', name],
        queryFn: () => getFunction(name),
        enabled: !!name,
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading\u2026" });
    if (error || !fn) {
        return (_jsxs("div", { children: [_jsx(Link, { to: "/aws/lambda", style: { color: '#0972d3', fontSize: '0.9em', textDecoration: 'none' }, children: "\u2190 Lambda" }), _jsx("p", { style: { color: '#d13212' }, children: error ? error.message : 'Function not found.' })] }));
    }
    const configRows = [
        ['ARN', fn.arn],
        ['Runtime', fn.runtime],
        ['Handler', fn.handler],
        ['Role ARN', fn.roleArn],
        ['Timeout', `${fn.timeout}s`],
        ['Memory', `${fn.memorySize} MB`],
        ['State', fn.state],
        ['Description', fn.description || '—'],
        ['Last modified', fn.lastModified ? new Date(fn.lastModified).toLocaleString() : '—'],
    ];
    return (_jsxs("div", { children: [_jsxs("div", { style: { marginBottom: '1.5rem' }, children: [_jsx(Link, { to: "/aws/lambda", style: { color: '#0972d3', fontSize: '0.85em', textDecoration: 'none' }, children: "\u2190 Lambda Functions" }), _jsxs("div", { style: { display: 'flex', alignItems: 'center', gap: '0.75rem', marginTop: '0.5rem', flexWrap: 'wrap' }, children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: fn.name }), _jsx("code", { style: { fontSize: '0.75em', color: '#8d9daa', background: '#f4f5f7', padding: '0.2em 0.5em', borderRadius: 3 }, children: fn.runtime }), _jsx("span", { style: {
                                    fontSize: '0.8em', fontWeight: 500,
                                    color: fn.state === 'Active' ? '#1d8102' : fn.state === 'Pending' ? '#e77600' : '#d13212',
                                }, children: fn.state })] })] }), _jsx("div", { style: { display: 'flex', borderBottom: '2px solid #e7e9ec', marginBottom: '1.5rem' }, children: [['test', 'Test'], ['logs', 'Logs'], ['configuration', 'Configuration']].map(([t, label]) => (_jsx("button", { onClick: () => setTab(t), style: {
                        background: 'none', border: 'none', padding: '0.6rem 1.25rem', cursor: 'pointer',
                        fontSize: '0.9em', fontWeight: tab === t ? 600 : 400,
                        color: tab === t ? '#e77600' : '#5f6b7a',
                        borderBottom: `2px solid ${tab === t ? '#e77600' : 'transparent'}`,
                        marginBottom: -2,
                    }, children: label }, t))) }), tab === 'test' && _jsx(LambdaTest, { name: name }), tab === 'logs' && _jsx(LambdaLogs, { name: name }), tab === 'configuration' && (_jsxs("div", { children: [_jsx("dl", { style: { display: 'grid', gridTemplateColumns: 'max-content 1fr', gap: '0 2rem', margin: '0 0 1.5rem', fontSize: '0.9em' }, children: configRows.map(([label, value]) => (_jsxs(Fragment, { children: [_jsx("dt", { style: { color: '#5f6b7a', fontWeight: 500, padding: '0.5rem 0', borderBottom: '1px solid #f4f5f7', whiteSpace: 'nowrap' }, children: label }), _jsx("dd", { style: { margin: 0, padding: '0.5rem 0', borderBottom: '1px solid #f4f5f7', wordBreak: 'break-all' }, children: value })] }, label))) }), fn.envVars && Object.keys(fn.envVars).length > 0 && (_jsxs("div", { style: { marginBottom: '1.5rem' }, children: [_jsx("h4", { style: { margin: '0 0 0.75rem', fontSize: '0.95rem', fontWeight: 600 }, children: "Environment variables" }), _jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 6, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.85em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: { padding: '0.5rem 1rem', textAlign: 'left', color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' }, children: "Key" }), _jsx("th", { style: { padding: '0.5rem 1rem', textAlign: 'left', color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' }, children: "Value" })] }) }), _jsx("tbody", { children: Object.entries(fn.envVars).map(([k, v]) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec' }, children: [_jsx("td", { style: { padding: '0.6rem 1rem' }, children: _jsx("code", { style: { background: '#f4f5f7', padding: '0.2em 0.4em', borderRadius: 3 }, children: k }) }), _jsx("td", { style: { padding: '0.6rem 1rem', fontFamily: 'monospace', fontSize: '0.9em' }, children: v })] }, k))) })] }) })] }))] }))] }));
}
function LambdaLogs({ name }) {
    const logGroupName = `/aws/lambda/${name}`;
    const [selectedStream, setSelectedStream] = useState(null);
    const { data: streamsData, isLoading: streamsLoading, refetch } = useQuery({
        queryKey: ['logs', 'lambda', name, 'streams'],
        queryFn: () => listLogStreams(logGroupName, { pageSize: 20 }),
        retry: false,
    });
    const streams = streamsData?.items ?? [];
    const activeStream = selectedStream ?? streams[0]?.name ?? null;
    const { data: eventsData, isLoading: eventsLoading } = useQuery({
        queryKey: ['logs', 'lambda', name, 'events', activeStream],
        queryFn: () => getLogEvents(logGroupName, activeStream, {
            limit: 200,
            startTime: Date.now() - 24 * 60 * 60 * 1000,
            endTime: Date.now(),
        }),
        enabled: !!activeStream,
    });
    function colorLine(line) {
        if (/^START /.test(line))
            return '#4caf78';
        if (/^END /.test(line))
            return '#79b8ff';
        if (/^REPORT /.test(line))
            return '#9da8b5';
        if (/^ERROR/.test(line))
            return '#ff6b6b';
        if (/^WARN/.test(line))
            return '#ffa94d';
        return '#e0e0e0';
    }
    if (streamsLoading)
        return _jsx("div", { style: { color: '#5f6b7a', fontSize: '0.9em' }, children: "Loading log streams\u2026" });
    if (streams.length === 0) {
        return (_jsxs("div", { style: { color: '#5f6b7a', fontSize: '0.9em' }, children: [_jsxs("p", { children: ["No log streams found for ", _jsxs("code", { children: ["/aws/lambda/", name] }), "."] }), _jsx("p", { style: { fontSize: '0.85em' }, children: "Invoke the function to generate logs." }), _jsx("button", { onClick: () => refetch(), style: btnSmall, children: "Refresh" })] }));
    }
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem' }, children: [_jsx("label", { style: { fontSize: '0.9em', fontWeight: 500 }, children: "Log stream" }), _jsx("select", { value: activeStream ?? '', onChange: (e) => setSelectedStream(e.target.value), style: { border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.3rem 0.5rem', fontSize: '0.85em', flex: 1, maxWidth: 480 }, children: streams.map((s) => (_jsx("option", { value: s.name, children: s.name }, s.name))) }), _jsx("button", { onClick: () => refetch(), style: btnSmall, children: "Refresh" })] }), _jsx("div", { style: { border: '1px solid #2d3748', borderRadius: 6, overflow: 'hidden' }, children: _jsx("pre", { style: { margin: 0, padding: '0.75rem 1rem', background: '#1e1e1e', overflow: 'auto', maxHeight: 500, fontSize: '0.8em', fontFamily: 'monospace', lineHeight: 1.6 }, children: eventsLoading ? (_jsx("span", { style: { color: '#5f6b7a' }, children: "Loading\u2026" })) : !eventsData || eventsData.events.length === 0 ? (_jsx("span", { style: { color: '#5f6b7a' }, children: "No events in this stream." })) : (eventsData.events.map((ev, i) => (_jsx("div", { style: { color: colorLine(ev.message) }, children: ev.message }, i)))) }) })] }));
}
const btnSmall = {
    background: 'none', border: '1px solid #c9cdd4', color: '#16191f',
    borderRadius: 4, padding: '0.3rem 0.75rem', cursor: 'pointer', fontSize: '0.82em',
};
