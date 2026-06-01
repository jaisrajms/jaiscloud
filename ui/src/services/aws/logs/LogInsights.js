import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState, useRef } from 'react';
import { startQuery, getQueryResults, stopQuery } from '../../../api/logs';
export function LogInsights() {
    const [queryString, setQueryString] = useState('fields @timestamp, @message\n| sort @timestamp desc\n| limit 20');
    const [logGroupName, setLogGroupName] = useState('');
    const [result, setResult] = useState(null);
    const [running, setRunning] = useState(false);
    const [error, setError] = useState('');
    const queryIdRef = useRef(null);
    async function run() {
        if (!queryString.trim())
            return;
        setRunning(true);
        setError('');
        setResult(null);
        try {
            const now = Date.now();
            const { queryId } = await startQuery({
                queryString,
                logGroupName: logGroupName || undefined,
                startTime: Math.floor((now - 3 * 60 * 60 * 1000) / 1000),
                endTime: Math.floor(now / 1000),
            });
            queryIdRef.current = queryId;
            // Poll for results
            let attempts = 0;
            const poll = async () => {
                const res = await getQueryResults(queryId);
                if (res.status === 'Complete' || res.status === 'Failed' || res.status === 'Cancelled' || attempts >= 20) {
                    setResult(res);
                    setRunning(false);
                }
                else {
                    attempts++;
                    setTimeout(poll, 500);
                }
            };
            await poll();
        }
        catch (e) {
            setError(e.message);
            setRunning(false);
        }
    }
    async function stop() {
        if (queryIdRef.current) {
            try {
                await stopQuery(queryIdRef.current);
            }
            catch { /* ignore */ }
        }
        setRunning(false);
    }
    const columns = result?.results[0]?.map(f => f.field) ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { marginBottom: '1.5rem' }, children: [_jsx("h2", { style: { margin: '0 0 0.25rem', fontSize: '1.4rem', fontWeight: 600 }, children: "Log Insights" }), _jsx("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: "Run CloudWatch Logs Insights queries" })] }), _jsx("div", { style: { display: 'flex', gap: '1rem', marginBottom: '0.75rem' }, children: _jsxs("label", { style: { flex: 1, ...labelStyle }, children: ["Log Group", _jsx("input", { type: "text", value: logGroupName, onChange: e => setLogGroupName(e.target.value), placeholder: "/aws/lambda/my-function (optional)", style: inputStyle })] }) }), _jsxs("label", { style: labelStyle, children: ["Query", _jsx("textarea", { value: queryString, onChange: e => setQueryString(e.target.value), rows: 5, style: { ...inputStyle, fontFamily: 'monospace', resize: 'vertical' } })] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', margin: '0.75rem 0 1.5rem' }, children: [_jsx("button", { onClick: run, disabled: running || !queryString.trim(), style: primaryBtnStyle, children: running ? 'Running…' : 'Run Query' }), running && (_jsx("button", { onClick: stop, style: cancelBtnStyle, children: "Stop" }))] }), error && _jsx("div", { style: { color: '#d13212', fontSize: '0.88em', marginBottom: '1rem' }, children: error }), result && (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '0.75rem' }, children: [_jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: ["Status: ", _jsx("strong", { style: { color: result.status === 'Complete' ? '#037f0c' : '#d13212' }, children: result.status })] }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [result.statistics.recordsScanned.toFixed(0), " records scanned, ", result.statistics.recordsMatched.toFixed(0), " matched"] })] }), result.results.length === 0 ? (_jsx("div", { style: { color: '#5f6b7a', fontSize: '0.88em' }, children: "No results matched the query." })) : (_jsx("div", { style: { overflowX: 'auto' }, children: _jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: columns.map(c => _jsx("th", { style: thStyle, children: c }, c)) }) }), _jsx("tbody", { children: result.results.map((row, i) => (_jsx("tr", { style: { borderBottom: '1px solid #2d3748' }, children: row.map((cell, j) => (_jsx("td", { style: { ...tdStyle, maxWidth: 400, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }, children: cell.value }, j))) }, i))) })] }) }))] }))] }));
}
const labelStyle = { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '0.75rem', color: '#c9cdd4', fontSize: '0.85em' };
const inputStyle = { background: '#0d1a26', border: '1px solid #2d3748', borderRadius: 4, color: '#e2e8f0', padding: '0.4rem 0.75rem', fontSize: '0.9em' };
const primaryBtnStyle = { background: '#0972d3', border: '1px solid #0972d3', borderRadius: 4, color: '#fff', cursor: 'pointer', padding: '0.4rem 1rem', fontSize: '0.85em', fontWeight: 500 };
const cancelBtnStyle = { background: 'none', border: '1px solid #2d3748', borderRadius: 4, color: '#c9cdd4', cursor: 'pointer', padding: '0.4rem 1rem', fontSize: '0.85em' };
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.85em' };
const thStyle = { textAlign: 'left', padding: '0.5rem 0.75rem', color: '#8892a4', fontWeight: 500, borderBottom: '1px solid #2d3748', fontSize: '0.8em', textTransform: 'uppercase', letterSpacing: '0.05em', whiteSpace: 'nowrap' };
const tdStyle = { padding: '0.6rem 0.75rem', color: '#c9cdd4', verticalAlign: 'top', fontFamily: 'monospace' };
