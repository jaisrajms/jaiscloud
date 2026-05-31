import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { listTopics, createTopic, deleteTopic, publish } from '../../../api/sns';
import { EmptyState } from '../../../components/EmptyState';
export function SNSList() {
    const [createOpen, setCreateOpen] = useState(false);
    const [newName, setNewName] = useState('');
    const [newFIFO, setNewFIFO] = useState(false);
    const [publishTopic, setPublishTopic] = useState(null);
    const [publishMsg, setPublishMsg] = useState('');
    const [publishSubject, setPublishSubject] = useState('');
    const [confirmDelete, setConfirmDelete] = useState(null);
    const qc = useQueryClient();
    const navigate = useNavigate();
    const { data, isLoading, error } = useQuery({
        queryKey: ['sns', 'topics'],
        queryFn: () => listTopics(),
    });
    const createMut = useMutation({
        mutationFn: () => createTopic({ name: newName, fifo: newFIFO }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['sns', 'topics'] });
            setCreateOpen(false);
            setNewName('');
            setNewFIFO(false);
        },
    });
    const deleteMut = useMutation({
        mutationFn: (arn) => deleteTopic(arn),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['sns', 'topics'] });
            setConfirmDelete(null);
        },
    });
    const publishMut = useMutation({
        mutationFn: ({ arn, msg, subj }) => publish(arn, { message: msg, subject: subj || undefined }),
        onSuccess: () => {
            setPublishTopic(null);
            setPublishMsg('');
            setPublishSubject('');
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading topics\u2026" });
    if (error)
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load: ", error.message] });
    const topics = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "SNS Topics" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [topics.length, " topic", topics.length !== 1 ? 's' : ''] })] }), _jsx("button", { onClick: () => setCreateOpen(true), style: btnPrimary, children: "Create topic" })] }), topics.length === 0 ? (_jsx(EmptyState, { title: "No topics", description: "SNS topics enable fan-out messaging to multiple subscribers.", cta: "Create Topic", onCta: () => setCreateOpen(true) })) : (_jsx("div", { style: { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' }, children: _jsxs("table", { style: { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' }, children: [_jsx("thead", { children: _jsxs("tr", { style: { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' }, children: [_jsx("th", { style: th, children: "Name" }), _jsx("th", { style: th, children: "Type" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Subscriptions" }), _jsx("th", { style: { ...th, width: 160 } })] }) }), _jsx("tbody", { children: topics.map((t) => (_jsxs("tr", { style: { borderBottom: '1px solid #e7e9ec', cursor: 'pointer' }, onClick: () => navigate(`/aws/sns/${encodeURIComponent(t.arn)}`), onMouseEnter: (e) => (e.currentTarget.style.background = '#fafbfc'), onMouseLeave: (e) => (e.currentTarget.style.background = ''), children: [_jsx("td", { style: td, children: _jsx("span", { style: { color: '#0972d3', fontWeight: 500 }, children: t.name }) }), _jsx("td", { style: td, children: _jsx("span", { style: {
                                                display: 'inline-block', padding: '0.15em 0.55em', borderRadius: 3,
                                                fontSize: '0.8em', fontWeight: 500,
                                                background: t.type === 'FIFO' ? '#e0f0ff' : '#f4f5f7',
                                                color: t.type === 'FIFO' ? '#0972d3' : '#5f6b7a',
                                            }, children: t.type }) }), _jsx("td", { style: { ...td, textAlign: 'right', fontVariantNumeric: 'tabular-nums' }, children: t.subscriptionCount }), _jsxs("td", { style: { ...td, display: 'flex', gap: '0.4rem' }, onClick: (e) => e.stopPropagation(), children: [_jsx("button", { onClick: () => { setPublishTopic(t); setPublishMsg(''); setPublishSubject(''); }, style: btnSmall, children: "Publish" }), _jsx("button", { onClick: () => setConfirmDelete(t), style: btnDelete, children: "Delete" })] })] }, t.arn))) })] }) })), createOpen && (_jsx("div", { style: overlayStyle, onClick: () => setCreateOpen(false), children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem' }, children: "Create topic" }), _jsx("label", { style: labelStyle, children: "Topic name" }), _jsx("input", { autoFocus: true, value: newName, onChange: (e) => setNewName(e.target.value), onKeyDown: (e) => e.key === 'Enter' && newName && createMut.mutate(), style: { ...inputStyle, marginBottom: '0.75rem' }, placeholder: "my-topic" }), _jsxs("label", { style: { ...labelStyle, display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }, children: [_jsx("input", { type: "checkbox", checked: newFIFO, onChange: (e) => setNewFIFO(e.target.checked) }), "FIFO topic"] }), createMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0.5rem 0 0', fontSize: '0.85em' }, children: createMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1.25rem' }, children: [_jsx("button", { onClick: () => setCreateOpen(false), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => createMut.mutate(), disabled: !newName || createMut.isPending, style: { ...btnPrimary, opacity: !newName || createMut.isPending ? 0.6 : 1 }, children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), publishTopic && (_jsx("div", { style: overlayStyle, onClick: () => setPublishTopic(null), children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 0.25rem' }, children: "Publish to topic" }), _jsx("p", { style: { margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.85em' }, children: publishTopic.name }), _jsx("label", { style: labelStyle, children: "Subject (optional)" }), _jsx("input", { value: publishSubject, onChange: (e) => setPublishSubject(e.target.value), style: { ...inputStyle, marginBottom: '0.75rem' }, placeholder: "My subject" }), _jsx("label", { style: labelStyle, children: "Message" }), _jsx("textarea", { autoFocus: true, value: publishMsg, onChange: (e) => setPublishMsg(e.target.value), style: { width: '100%', boxSizing: 'border-box', height: 120, padding: '0.5rem 0.75rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em', resize: 'vertical' }, placeholder: "Message body\u2026" }), publishMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0.5rem 0 0', fontSize: '0.85em' }, children: publishMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end', marginTop: '1rem' }, children: [_jsx("button", { onClick: () => setPublishTopic(null), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => publishMut.mutate({ arn: publishTopic.arn, msg: publishMsg, subj: publishSubject }), disabled: !publishMsg || publishMut.isPending, style: { ...btnPrimary, opacity: !publishMsg || publishMut.isPending ? 0.6 : 1 }, children: publishMut.isPending ? 'Publishing…' : 'Publish' })] })] }) })), confirmDelete && (_jsx("div", { style: overlayStyle, children: _jsxs("div", { style: dialogStyle, onClick: (e) => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 0.75rem', fontSize: '1.05rem' }, children: "Delete topic?" }), _jsxs("p", { style: { margin: '0 0 1.25rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Permanently delete ", _jsx("strong", { children: confirmDelete.name }), " and all its subscriptions?"] }), deleteMut.error && (_jsx("p", { style: { color: '#d13212', margin: '0 0 0.75rem', fontSize: '0.85em' }, children: deleteMut.error.message })), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { onClick: () => setConfirmDelete(null), style: btnSecondary, children: "Cancel" }), _jsx("button", { onClick: () => deleteMut.mutate(confirmDelete.arn), disabled: deleteMut.isPending, style: { ...btnDanger, opacity: deleteMut.isPending ? 0.6 : 1 }, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
const th = { padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, color: '#5f6b7a', fontSize: '0.82em', textTransform: 'uppercase', letterSpacing: '0.03em' };
const td = { padding: '0.75rem 1rem' };
const btnPrimary = { background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1.25rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em' };
const btnSecondary = { background: '#fff', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' };
const btnDanger = { background: '#d13212', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1rem', cursor: 'pointer', fontSize: '0.9em' };
const btnDelete = { background: 'none', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#5f6b7a' };
const btnSmall = { background: '#f4f5f7', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.25rem 0.6rem', cursor: 'pointer', fontSize: '0.8em', color: '#3d4c5e' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const dialogStyle = { background: '#fff', borderRadius: 8, padding: '1.5rem', minWidth: 380, maxWidth: 480, boxShadow: '0 4px 24px rgba(0,0,0,0.15)' };
const labelStyle = { display: 'block', fontSize: '0.85em', fontWeight: 500, marginBottom: '0.4rem', color: '#3d4c5e' };
const inputStyle = { width: '100%', boxSizing: 'border-box', padding: '0.5rem 0.75rem', border: '1px solid #c9cdd4', borderRadius: 4, fontSize: '0.9em' };
