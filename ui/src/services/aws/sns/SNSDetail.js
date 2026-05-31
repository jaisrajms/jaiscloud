import { jsx as _jsx, jsxs as _jsxs, Fragment as _Fragment } from "react/jsx-runtime";
import { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getTopic, listSubscriptionsByTopic, subscribe, unsubscribe, publish } from '../../../api/sns';
export function SNSDetail() {
    const { topicArn: encodedArn } = useParams();
    const topicArn = decodeURIComponent(encodedArn ?? '');
    const navigate = useNavigate();
    const qc = useQueryClient();
    const [subOpen, setSubOpen] = useState(false);
    const [subProtocol, setSubProtocol] = useState('sqs');
    const [subEndpoint, setSubEndpoint] = useState('');
    const [publishOpen, setPublishOpen] = useState(false);
    const [publishMsg, setPublishMsg] = useState('');
    const [publishSubject, setPublishSubject] = useState('');
    const [publishResult, setPublishResult] = useState('');
    const { data: topic } = useQuery({
        queryKey: ['sns', 'topic', topicArn],
        queryFn: () => getTopic(topicArn),
    });
    const { data: subsData, isLoading: subsLoading } = useQuery({
        queryKey: ['sns', 'subscriptions', topicArn],
        queryFn: () => listSubscriptionsByTopic(topicArn),
    });
    const subscribeMut = useMutation({
        mutationFn: () => subscribe(topicArn, { protocol: subProtocol, endpoint: subEndpoint }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['sns', 'subscriptions', topicArn] });
            void qc.invalidateQueries({ queryKey: ['sns', 'topics'] });
            setSubOpen(false);
            setSubEndpoint('');
        },
    });
    const unsubscribeMut = useMutation({
        mutationFn: (subArn) => unsubscribe(subArn),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['sns', 'subscriptions', topicArn] });
        },
    });
    const publishMut = useMutation({
        mutationFn: () => publish(topicArn, { message: publishMsg, subject: publishSubject || undefined }),
        onSuccess: (res) => {
            setPublishResult(res.MessageId ?? 'sent');
            setPublishMsg('');
            setPublishSubject('');
        },
    });
    const subs = subsData?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '1.5rem' }, children: [_jsx("button", { onClick: () => navigate('/aws/sns'), style: btnBack, children: "\u2190 Topics" }), _jsxs("div", { style: { flex: 1 }, children: [_jsx("h2", { style: { margin: '0 0 0.15rem', fontSize: '1.4rem', fontWeight: 600 }, children: topic?.name ?? topicArn.split(':').pop() }), _jsx("span", { style: { fontSize: '0.78em', color: '#8d9daa', fontFamily: 'monospace' }, children: topicArn })] }), _jsx("button", { onClick: () => { setPublishOpen(true); setPublishMsg(''); setPublishSubject(''); setPublishResult(''); }, style: btnPrimary, children: "Publish" }), _jsx("button", { onClick: () => setSubOpen(true), style: btnSecondary, children: "Subscribe" })] }), _jsxs("h3", { style: { margin: '0 0 0.75rem', fontSize: '1rem', fontWeight: 600 }, children: ["Subscriptions (", subs.length, ")"] }), subsLoading && _jsx("div", { style: { color: '#5f6b7a' }, children: "Loading\u2026" }), !subsLoading && subs.length === 0 && (_jsxs("div", { style: { padding: '2.5rem', textAlign: 'center', color: '#8d9daa', border: '1px solid #e7e9ec', borderRadius: 8 }, children: ["No subscriptions yet.", ' ', _jsx("span", { style: { color: '#0972d3', cursor: 'pointer' }, onClick: () => setSubOpen(true), children: "Add one \u2192" })] })), subs.length > 0 && (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Protocol" }), _jsx("th", { style: th, children: "Endpoint" }), _jsx("th", { style: { ...th, width: 80 } })] }) }), _jsx("tbody", { children: subs.map((s) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec' }, children: [_jsx("td", { style: td, children: _jsx("span", { style: {
                                                display: 'inline-block', padding: '0.15em 0.55em', borderRadius: 3,
                                                fontSize: '0.8em', fontWeight: 500, background: '#f4f5f7', color: '#5f6b7a',
                                            }, children: s.protocol }) }), _jsx("td", { style: { ...td, fontFamily: 'monospace', fontSize: '0.85em', wordBreak: 'break-all' }, children: s.endpoint }), _jsx("td", { style: td, children: _jsx("button", { onClick: () => unsubscribeMut.mutate(s.subscriptionArn), disabled: unsubscribeMut.isPending, style: btnDelete, children: "Remove" }) })] }, s.subscriptionArn))) })] }) })), subOpen && (_jsx("div", { style: overlayStyle, onClick: () => setSubOpen(false), children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem' }, children: "Subscribe" }), _jsx("label", { style: labelStyle, children: "Protocol" }), _jsx("select", { value: subProtocol, onChange: (e) => setSubProtocol(e.target.value), style: { ...inputStyle, marginBottom: '0.75rem' }, children: ['sqs', 'lambda', 'http', 'https', 'email', 'sms'].map((p) => (_jsx("option", { value: p, children: p }, p))) }), _jsx("label", { style: labelStyle, children: "Endpoint" }), _jsx("input", { autoFocus: true, value: subEndpoint, onChange: (e) => setSubEndpoint(e.target.value), style: { ...inputStyle, marginBottom: '1rem' }, placeholder: subProtocol === 'sqs' ? 'arn:aws:sqs:us-east-1:000000000000:my-queue' : 'https://…' }), subscribeMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: subscribeMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => setSubOpen(false), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => subscribeMut.mutate(), disabled: !subEndpoint || subscribeMut.isPending, style: { ...btnPrimary, opacity: !subEndpoint || subscribeMut.isPending ? 0.6 : 1 }, children: subscribeMut.isPending ? 'Subscribing…' : 'Subscribe' })] })] }) })), publishOpen && (_jsx("div", { style: overlayStyle, onClick: () => setPublishOpen(false), children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 0.25rem' }, children: "Publish message" }), publishResult ? (_jsxs("div", { children: [_jsxs("p", { style: { color: '#1d6b2e', background: '#e0f9e0', padding: '0.75rem', borderRadius: 4, margin: '1rem 0' }, children: ["\u2713 Published \u2014 MessageId: ", publishResult] }), _jsx("div", { style: { display: 'flex', justifyContent: 'flex-end' }, children: _jsx("button", { onClick: () => { setPublishOpen(false); setPublishResult(''); }, style: btnPrimary, children: "Done" }) })] })) : (_jsxs(_Fragment, { children: [_jsxs("p", { style: { margin: '0 0 0.75rem', color: '#5f6b7a', fontSize: '0.85em' }, children: ["Topic: ", topic?.name] }), _jsx("label", { style: labelStyle, children: "Subject (optional)" }), _jsx("input", { value: publishSubject, onChange: (e) => setPublishSubject(e.target.value), style: { ...inputStyle, marginBottom: '0.75rem' }, placeholder: "Subject\u2026" }), _jsx("label", { style: labelStyle, children: "Message" }), _jsx("textarea", { autoFocus: true, value: publishMsg, onChange: (e) => setPublishMsg(e.target.value), style: { width: '100%', boxSizing: 'border-box', height: 120, padding: '0.5rem 0.75rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em', resize: 'vertical' }, placeholder: "Message body\u2026" }), publishMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0.5rem 0 0', fontSize: '0.85em' }, children: publishMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1rem' }, children: [_jsx("button", { onClick: () => setPublishOpen(false), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => publishMut.mutate(), disabled: !publishMsg || publishMut.isPending, style: { ...btnPrimary, opacity: !publishMsg || publishMut.isPending ? 0.6 : 1 }, children: publishMut.isPending ? 'Publishing…' : 'Publish' })] })] }))] }) }))] }));
}
const th = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' };
const td = { padding: '0.75rem 1rem' };
const btnBack = { background: 'none', border: 'none', color: '#0972d3', cursor: 'pointer', fontSize: '0.9em', padding: '0.25rem 0' };
const btnPrimary = { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.45rem 1.1rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em' };
const btnSecondary = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.45rem 0.9rem', cursor: 'pointer', fontSize: '0.88em' };
const btnDelete = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const dialogStyle = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 500, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' };
const labelStyle = { display: 'block', fontSize: '0.85em', fontWeight: 500, marginBottom: '0.4rem', color: '#3d4c5e' };
const inputStyle = { width: '100%', boxSizing: 'border-box', padding: '0.5rem 0.75rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em' };
