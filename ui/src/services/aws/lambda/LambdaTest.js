import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Fragment, useState } from 'react';
import { useMutation } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { invokeFunction } from '../../../api/lambda';
function decodeBase64(s) {
    try {
        return atob(s);
    }
    catch {
        return s;
    }
}
function tryPrettyJson(s) {
    if (!s)
        return '';
    try {
        return JSON.stringify(JSON.parse(s), null, 2);
    }
    catch {
        return s;
    }
}
function colorLogLine(line) {
    if (/^START /.test(line))
        return { color: '#4caf78' };
    if (/^END /.test(line))
        return { color: '#79b8ff' };
    if (/^REPORT /.test(line))
        return { color: '#9da8b5' };
    if (/^ERROR/.test(line))
        return { color: '#ff6b6b' };
    if (/^WARN/.test(line))
        return { color: '#ffa94d' };
    if (/^INFO/.test(line))
        return { color: '#74c7f0' };
    if (/^DEBUG/.test(line))
        return { color: '#b197fc' };
    return { color: '#e0e0e0' };
}
export function LambdaTest({ name }) {
    const [payload, setPayload] = useState('{}');
    const [invocationType, setInvocationType] = useState('RequestResponse');
    const [result, setResult] = useState(null);
    const [resultTab, setResultTab] = useState('response');
    const navigate = useNavigate();
    const invokeMut = useMutation({
        mutationFn: () => invokeFunction(name, { payload, invocationType }),
        onSuccess: (data) => {
            setResult(data);
            setResultTab('response');
        },
    });
    const logLines = result?.logResult ? decodeBase64(result.logResult).split('\n').filter(Boolean) : [];
    const hasError = !!result?.functionError;
    const logGroupPath = `/aws/logs/groups/%2Faws%2Flambda%2F${encodeURIComponent(name)}`;
    return (_jsxs("div", { children: [_jsxs("div", { style: { marginBottom: '1rem' }, children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.5rem' }, children: [_jsx("label", { style: { fontSize: '0.9em', fontWeight: 500 }, children: "Test event payload" }), _jsxs("select", { value: invocationType, onChange: (e) => setInvocationType(e.target.value), style: { marginLeft: 'auto', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.3rem 0.5rem', fontSize: '0.85em' }, children: [_jsx("option", { value: "RequestResponse", children: "RequestResponse" }), _jsx("option", { value: "Event", children: "Event (async)" }), _jsx("option", { value: "DryRun", children: "DryRun" })] })] }), _jsx("textarea", { value: payload, onChange: (e) => setPayload(e.target.value), rows: 8, spellCheck: false, style: {
                            width: '100%', boxSizing: 'border-box', fontFamily: 'monospace', fontSize: '0.85em',
                            border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.6rem 0.75rem',
                            resize: 'vertical', background: '#1e1e1e', color: '#d4d4d4', outline: 'none',
                        }, placeholder: "{}" })] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', marginBottom: '1.5rem' }, children: [_jsx("button", { onClick: () => invokeMut.mutate(), disabled: invokeMut.isPending, style: { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1.25rem', cursor: 'pointer', fontSize: '0.9em', fontWeight: 500, opacity: invokeMut.isPending ? 0.6 : 1 }, children: invokeMut.isPending ? 'Invoking…' : 'Invoke' }), invokeMut.error && (_jsx("span", { style: { color: '#d13212', fontSize: '0.85em', alignSelf: 'center' }, children: invokeMut.error.message }))] }), result && (_jsxs("div", { children: [_jsx("div", { style: { display: 'flex', borderBottom: '2px solid #e7e9ec', marginBottom: '1rem' }, children: ['response', 'logs', 'details'].map((t) => (_jsxs("button", { onClick: () => setResultTab(t), style: {
                                background: 'none', border: 'none', padding: '0.5rem 1rem', cursor: 'pointer',
                                fontSize: '0.85em', fontWeight: resultTab === t ? 600 : 400,
                                color: resultTab === t ? '#e77600' : '#5f6b7a',
                                borderBottom: `2px solid ${resultTab === t ? '#e77600' : 'transparent'}`,
                                marginBottom: -2,
                            }, children: [t.charAt(0).toUpperCase() + t.slice(1), t === 'response' && hasError && (_jsx("span", { style: { marginLeft: '0.4rem', background: '#d13212', color: '#fff', borderRadius: 10, padding: '0 0.4em', fontSize: '0.75em' }, children: "!" }))] }, t))) }), resultTab === 'response' && (_jsx("pre", { style: {
                            margin: 0, padding: '0.75rem', borderRadius: 6, overflow: 'auto',
                            fontSize: '0.82em', fontFamily: 'monospace', whiteSpace: 'pre-wrap', wordBreak: 'break-all',
                            background: hasError ? '#fff5f5' : '#f4f5f7',
                            border: `1px solid ${hasError ? '#f5c6cb' : '#e7e9ec'}`,
                            color: hasError ? '#d13212' : '#16191f',
                            maxHeight: 400,
                        }, children: tryPrettyJson(result.payload) || '(empty response)' })), resultTab === 'logs' && (_jsxs("div", { children: [_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 6, overflow: 'hidden', marginBottom: '0.75rem' }, children: _jsx("pre", { style: { margin: 0, padding: '0.75rem', background: '#1e1e1e', color: '#e0e0e0', overflow: 'auto', maxHeight: 400, fontSize: '0.8em', fontFamily: 'monospace' }, children: logLines.length === 0 ? (_jsx("span", { style: { color: '#5f6b7a' }, children: "No log output." })) : (logLines.map((line, i) => (_jsx("div", { style: colorLogLine(line), children: line }, i)))) }) }), result.requestId && (_jsx("button", { onClick: () => navigate(`${logGroupPath}?requestId=${encodeURIComponent(result.requestId)}`), style: { background: 'none', border: '1px solid #0972d3', color: '#0972d3', borderRadius: 4, padding: '0.35rem 0.8rem', cursor: 'pointer', fontSize: '0.85em' }, children: "View logs for this invocation \u2192" }))] })), resultTab === 'details' && (_jsx("dl", { style: { display: 'grid', gridTemplateColumns: 'max-content 1fr', gap: '0 2rem', fontSize: '0.9em', margin: 0 }, children: [
                            ['Status code', String(result.statusCode)],
                            ['Function error', result.functionError || '—'],
                            ['Executed version', result.executedVersion || '—'],
                            ['Billed duration', result.billedDurationMs ? `${result.billedDurationMs} ms` : '—'],
                            ['Request ID', result.requestId || '—'],
                        ].map(([label, value]) => (_jsxs(Fragment, { children: [_jsx("dt", { style: { color: '#5f6b7a', fontWeight: 500, padding: '0.4rem 0', borderBottom: '1px solid #f4f5f7' }, children: label }), _jsx("dd", { style: { margin: 0, padding: '0.4rem 0', borderBottom: '1px solid #f4f5f7', wordBreak: 'break-all' }, children: value })] }, label))) }))] }))] }));
}
