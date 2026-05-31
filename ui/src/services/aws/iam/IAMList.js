import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listRoles, createRole, deleteRole, listUsers, createUser, deleteUser, listAccessKeys, createAccessKey, deleteAccessKey, listPolicies, createPolicy, deletePolicy, } from '../../../api/iam';
import { EmptyState } from '../../../components/EmptyState';
export function IAMList() {
    const [tab, setTab] = useState('roles');
    return (_jsxs("div", { children: [_jsx("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }, children: _jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "IAM" }) }), _jsx("div", { style: { display: 'flex', gap: 0, borderBottom: '2px solid #e7e9ec', marginBottom: '1.5rem' }, children: ['roles', 'users', 'policies'].map(t => (_jsx("button", { onClick: () => setTab(t), style: {
                        ...tabBtn,
                        borderBottom: tab === t ? '2px solid #e87600' : '2px solid transparent',
                        color: tab === t ? '#e87600' : '#5f6b7a',
                        fontWeight: tab === t ? 600 : 400,
                        marginBottom: -2,
                    }, children: t.charAt(0).toUpperCase() + t.slice(1) }, t))) }), tab === 'roles' && _jsx(RolesTab, {}), tab === 'users' && _jsx(UsersTab, {}), tab === 'policies' && _jsx(PoliciesTab, {})] }));
}
function RolesTab() {
    const [createOpen, setCreateOpen] = useState(false);
    const [newName, setNewName] = useState('');
    const [newDesc, setNewDesc] = useState('');
    const [newPolicy, setNewPolicy] = useState('');
    const [confirmDelete, setConfirmDelete] = useState(null);
    const qc = useQueryClient();
    const { data, isLoading, error } = useQuery({
        queryKey: ['iam', 'roles'],
        queryFn: () => listRoles(),
    });
    const createMut = useMutation({
        mutationFn: () => createRole({ roleName: newName, description: newDesc || undefined, assumeRolePolicyDocument: newPolicy || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['iam', 'roles'] });
            setCreateOpen(false);
            setNewName('');
            setNewDesc('');
            setNewPolicy('');
        },
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteRole(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['iam', 'roles'] });
            setConfirmDelete(null);
        },
    });
    if (isLoading)
        return _jsx("div", { style: loadingStyle, children: "Loading roles\u2026" });
    if (error)
        return _jsxs("div", { style: errorStyle, children: ["Failed to load: ", error.message] });
    const roles = data?.items ?? [];
    return (_jsxs("div", { children: [_jsx("div", { style: { display: 'flex', justifyContent: 'flex-end', marginBottom: '1rem' }, children: _jsx("button", { onClick: () => setCreateOpen(true), style: btnPrimary, children: "Create role" }) }), roles.length === 0 ? (_jsx(EmptyState, { title: "No roles", description: "IAM roles grant AWS service access permissions.", cta: "Create Role", onCta: () => setCreateOpen(true) })) : (_jsx("div", { style: tableWrap, children: _jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsxs("tr", { style: theadRow, children: [_jsx("th", { style: th, children: "Role name" }), _jsx("th", { style: th, children: "ARN" }), _jsx("th", { style: th, children: "Created" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Actions" })] }) }), _jsx("tbody", { children: roles.map((r, i) => (_jsxs("tr", { style: { borderBottom: i < roles.length - 1 ? '1px solid #e7e9ec' : 'none' }, children: [_jsx("td", { style: { ...td, fontWeight: 500, color: '#0972d3' }, children: r.roleName }), _jsx("td", { style: { ...td, fontFamily: 'monospace', fontSize: '0.82em', color: '#5f6b7a' }, children: r.arn }), _jsx("td", { style: td, children: r.createDate || '—' }), _jsx("td", { style: { ...td, textAlign: 'right' }, children: _jsx("button", { onClick: () => setConfirmDelete(r), style: { ...btnSmall, color: '#d13212', borderColor: '#d13212' }, children: "Delete" }) })] }, r.arn))) })] }) })), createOpen && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 1rem' }, children: "Create Role" }), _jsx("label", { style: labelStyle, children: "Role name *" }), _jsx("input", { style: inputStyle, value: newName, onChange: e => setNewName(e.target.value), placeholder: "my-lambda-role", autoFocus: true }), _jsx("label", { style: labelStyle, children: "Description (optional)" }), _jsx("input", { style: inputStyle, value: newDesc, onChange: e => setNewDesc(e.target.value), placeholder: "Role description" }), _jsx("label", { style: labelStyle, children: "Trust policy document (optional)" }), _jsx("textarea", { style: { ...inputStyle, height: 120, fontFamily: 'monospace', fontSize: '0.82em', resize: 'vertical' }, value: newPolicy, onChange: e => setNewPolicy(e.target.value), placeholder: '{\n  "Version": "2012-10-17",\n  "Statement": [...]\n}' }), _jsx("div", { style: { fontSize: '0.78em', color: '#5f6b7a', marginBottom: '0.5rem', marginTop: '-0.25rem' }, children: "Leave blank for default Lambda trust policy." }), createMut.error && _jsx("div", { style: mutError, children: createMut.error.message }), _jsxs("div", { style: dialogActions, children: [_jsx("button", { style: btnSecondary, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnPrimary, onClick: () => createMut.mutate(), disabled: !newName || createMut.isPending, children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), confirmDelete && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 0.75rem' }, children: "Delete role?" }), _jsxs("p", { style: { margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Delete role ", _jsx("strong", { children: confirmDelete.roleName }), "? This cannot be undone."] }), deleteMut.error && _jsx("div", { style: mutError, children: deleteMut.error.message }), _jsxs("div", { style: dialogActions, children: [_jsx("button", { style: btnSecondary, onClick: () => setConfirmDelete(null), children: "Cancel" }), _jsx("button", { style: { ...btnPrimary, background: '#d13212', borderColor: '#d13212' }, onClick: () => deleteMut.mutate(confirmDelete.roleName), disabled: deleteMut.isPending, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
function UsersTab() {
    const [createOpen, setCreateOpen] = useState(false);
    const [newName, setNewName] = useState('');
    const [confirmDelete, setConfirmDelete] = useState(null);
    const [keysUser, setKeysUser] = useState(null);
    const [newKey, setNewKey] = useState(null);
    const qc = useQueryClient();
    const { data, isLoading, error } = useQuery({
        queryKey: ['iam', 'users'],
        queryFn: () => listUsers(),
    });
    const { data: keysData } = useQuery({
        queryKey: ['iam', 'access-keys', keysUser?.userName],
        queryFn: () => listAccessKeys(keysUser.userName),
        enabled: !!keysUser,
    });
    const createMut = useMutation({
        mutationFn: () => createUser({ userName: newName }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['iam', 'users'] });
            setCreateOpen(false);
            setNewName('');
        },
    });
    const deleteMut = useMutation({
        mutationFn: (name) => deleteUser(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['iam', 'users'] });
            setConfirmDelete(null);
        },
    });
    const createKeyMut = useMutation({
        mutationFn: () => createAccessKey(keysUser.userName),
        onSuccess: (resp) => {
            void qc.invalidateQueries({ queryKey: ['iam', 'access-keys', keysUser?.userName] });
            setNewKey(resp.AccessKey);
        },
    });
    const deleteKeyMut = useMutation({
        mutationFn: ({ userName, keyId }) => deleteAccessKey(userName, keyId),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['iam', 'access-keys', keysUser?.userName] });
        },
    });
    if (isLoading)
        return _jsx("div", { style: loadingStyle, children: "Loading users\u2026" });
    if (error)
        return _jsxs("div", { style: errorStyle, children: ["Failed to load: ", error.message] });
    const users = data?.items ?? [];
    return (_jsxs("div", { children: [_jsx("div", { style: { display: 'flex', justifyContent: 'flex-end', marginBottom: '1rem' }, children: _jsx("button", { onClick: () => setCreateOpen(true), style: btnPrimary, children: "Create user" }) }), users.length === 0 ? (_jsx(EmptyState, { title: "No users", description: "IAM users allow programmatic access to AWS services.", cta: "Create User", onCta: () => setCreateOpen(true) })) : (_jsx("div", { style: tableWrap, children: _jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsxs("tr", { style: theadRow, children: [_jsx("th", { style: th, children: "User name" }), _jsx("th", { style: th, children: "ARN" }), _jsx("th", { style: th, children: "Created" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Actions" })] }) }), _jsx("tbody", { children: users.map((u, i) => (_jsxs("tr", { style: { borderBottom: i < users.length - 1 ? '1px solid #e7e9ec' : 'none' }, children: [_jsx("td", { style: { ...td, fontWeight: 500, color: '#0972d3' }, children: u.userName }), _jsx("td", { style: { ...td, fontFamily: 'monospace', fontSize: '0.82em', color: '#5f6b7a' }, children: u.arn }), _jsx("td", { style: td, children: u.createDate || '—' }), _jsxs("td", { style: { ...td, textAlign: 'right', whiteSpace: 'nowrap' }, children: [_jsx("button", { onClick: () => setKeysUser(u), style: btnSmall, children: "Access keys" }), _jsx("button", { onClick: () => setConfirmDelete(u), style: { ...btnSmall, marginLeft: 6, color: '#d13212', borderColor: '#d13212' }, children: "Delete" })] })] }, u.arn))) })] }) })), createOpen && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 1rem' }, children: "Create User" }), _jsx("label", { style: labelStyle, children: "User name *" }), _jsx("input", { style: inputStyle, value: newName, onChange: e => setNewName(e.target.value), placeholder: "my-service-user", autoFocus: true }), createMut.error && _jsx("div", { style: mutError, children: createMut.error.message }), _jsxs("div", { style: dialogActions, children: [_jsx("button", { style: btnSecondary, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnPrimary, onClick: () => createMut.mutate(), disabled: !newName || createMut.isPending, children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), confirmDelete && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 0.75rem' }, children: "Delete user?" }), _jsxs("p", { style: { margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Delete user ", _jsx("strong", { children: confirmDelete.userName }), "? This cannot be undone."] }), deleteMut.error && _jsx("div", { style: mutError, children: deleteMut.error.message }), _jsxs("div", { style: dialogActions, children: [_jsx("button", { style: btnSecondary, onClick: () => setConfirmDelete(null), children: "Cancel" }), _jsx("button", { style: { ...btnPrimary, background: '#d13212', borderColor: '#d13212' }, onClick: () => deleteMut.mutate(confirmDelete.userName), disabled: deleteMut.isPending, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) })), keysUser && (_jsx("div", { style: overlay, children: _jsxs("div", { style: { ...dialog, minWidth: 560, maxWidth: 680 }, children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }, children: [_jsxs("h3", { style: { margin: 0 }, children: ["Access keys \u2014 ", keysUser.userName] }), _jsx("button", { style: btnPrimary, onClick: () => createKeyMut.mutate(), disabled: createKeyMut.isPending, children: createKeyMut.isPending ? 'Creating…' : 'Create key' })] }), newKey && (_jsxs("div", { style: { background: '#f0f9f0', border: '1px solid #1d8102', borderRadius: 6, padding: '0.75rem', marginBottom: '1rem', fontSize: '0.85em' }, children: [_jsx("div", { style: { fontWeight: 600, marginBottom: 4, color: '#1d8102' }, children: "Key created \u2014 save the secret now, it won't be shown again" }), _jsxs("div", { children: [_jsx("strong", { children: "Access Key ID:" }), " ", _jsx("code", { children: newKey.accessKeyId })] }), _jsxs("div", { children: [_jsx("strong", { children: "Secret Access Key:" }), " ", _jsx("code", { children: newKey.secretAccessKey })] }), _jsx("button", { style: { ...btnSmall, marginTop: 8 }, onClick: () => setNewKey(null), children: "Dismiss" })] })), (keysData?.items ?? []).length === 0 ? (_jsx("div", { style: { color: '#8d9daa', fontSize: '0.9em', marginBottom: '1rem' }, children: "No access keys." })) : (_jsx("div", { style: { ...tableWrap, marginBottom: '1rem' }, children: _jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsxs("tr", { style: theadRow, children: [_jsx("th", { style: th, children: "Access Key ID" }), _jsx("th", { style: th, children: "Status" }), _jsx("th", { style: th, children: "Created" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Actions" })] }) }), _jsx("tbody", { children: (keysData?.items ?? []).map((k, i) => (_jsxs("tr", { style: { borderBottom: i < (keysData?.items ?? []).length - 1 ? '1px solid #e7e9ec' : 'none' }, children: [_jsx("td", { style: { ...td, fontFamily: 'monospace', fontSize: '0.85em' }, children: k.accessKeyId }), _jsx("td", { style: td, children: _jsx("span", { style: {
                                                            display: 'inline-block', padding: '1px 8px', borderRadius: 10, fontSize: '0.8em',
                                                            background: k.status === 'Active' ? '#1d810222' : '#8d9daa22',
                                                            color: k.status === 'Active' ? '#1d8102' : '#5f6b7a',
                                                        }, children: k.status }) }), _jsx("td", { style: td, children: k.createDate || '—' }), _jsx("td", { style: { ...td, textAlign: 'right' }, children: _jsx("button", { onClick: () => deleteKeyMut.mutate({ userName: keysUser.userName, keyId: k.accessKeyId }), style: { ...btnSmall, color: '#d13212', borderColor: '#d13212' }, disabled: deleteKeyMut.isPending, children: "Delete" }) })] }, k.accessKeyId))) })] }) })), _jsx("div", { style: { display: 'flex', justifyContent: 'flex-end' }, children: _jsx("button", { style: btnSecondary, onClick: () => { setKeysUser(null); setNewKey(null); }, children: "Close" }) })] }) }))] }));
}
function PoliciesTab() {
    const [createOpen, setCreateOpen] = useState(false);
    const [newName, setNewName] = useState('');
    const [newDoc, setNewDoc] = useState('');
    const [newDesc, setNewDesc] = useState('');
    const [confirmDelete, setConfirmDelete] = useState(null);
    const qc = useQueryClient();
    const { data, isLoading, error } = useQuery({
        queryKey: ['iam', 'policies'],
        queryFn: () => listPolicies({ scope: 'Local' }),
    });
    const createMut = useMutation({
        mutationFn: () => createPolicy({ policyName: newName, policyDocument: newDoc, description: newDesc || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['iam', 'policies'] });
            setCreateOpen(false);
            setNewName('');
            setNewDoc('');
            setNewDesc('');
        },
    });
    const deleteMut = useMutation({
        mutationFn: (arn) => deletePolicy(arn),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['iam', 'policies'] });
            setConfirmDelete(null);
        },
    });
    if (isLoading)
        return _jsx("div", { style: loadingStyle, children: "Loading policies\u2026" });
    if (error)
        return _jsxs("div", { style: errorStyle, children: ["Failed to load: ", error.message] });
    const policies = data?.items ?? [];
    return (_jsxs("div", { children: [_jsx("div", { style: { display: 'flex', justifyContent: 'flex-end', marginBottom: '1rem' }, children: _jsx("button", { onClick: () => setCreateOpen(true), style: btnPrimary, children: "Create policy" }) }), policies.length === 0 ? (_jsx(EmptyState, { title: "No policies", description: "IAM policies define permissions that can be attached to roles and users.", cta: "Create Policy", onCta: () => setCreateOpen(true) })) : (_jsx("div", { style: tableWrap, children: _jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsxs("tr", { style: theadRow, children: [_jsx("th", { style: th, children: "Policy name" }), _jsx("th", { style: th, children: "ARN" }), _jsx("th", { style: th, children: "Attachments" }), _jsx("th", { style: th, children: "Created" }), _jsx("th", { style: { ...th, textAlign: 'right' }, children: "Actions" })] }) }), _jsx("tbody", { children: policies.map((p, i) => (_jsxs("tr", { style: { borderBottom: i < policies.length - 1 ? '1px solid #e7e9ec' : 'none' }, children: [_jsx("td", { style: { ...td, fontWeight: 500, color: '#0972d3' }, children: p.policyName }), _jsx("td", { style: { ...td, fontFamily: 'monospace', fontSize: '0.82em', color: '#5f6b7a' }, children: p.arn }), _jsx("td", { style: td, children: p.attachmentCount }), _jsx("td", { style: td, children: p.createDate || '—' }), _jsx("td", { style: { ...td, textAlign: 'right' }, children: _jsx("button", { onClick: () => setConfirmDelete(p), style: { ...btnSmall, color: '#d13212', borderColor: '#d13212' }, children: "Delete" }) })] }, p.arn))) })] }) })), createOpen && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 1rem' }, children: "Create Policy" }), _jsx("label", { style: labelStyle, children: "Policy name *" }), _jsx("input", { style: inputStyle, value: newName, onChange: e => setNewName(e.target.value), placeholder: "my-policy", autoFocus: true }), _jsx("label", { style: labelStyle, children: "Policy document *" }), _jsx("textarea", { style: { ...inputStyle, height: 160, fontFamily: 'monospace', fontSize: '0.82em', resize: 'vertical' }, value: newDoc, onChange: e => setNewDoc(e.target.value), placeholder: '{\n  "Version": "2012-10-17",\n  "Statement": [\n    {\n      "Effect": "Allow",\n      "Action": "*",\n      "Resource": "*"\n    }\n  ]\n}' }), _jsx("label", { style: labelStyle, children: "Description (optional)" }), _jsx("input", { style: inputStyle, value: newDesc, onChange: e => setNewDesc(e.target.value), placeholder: "Policy description" }), createMut.error && _jsx("div", { style: mutError, children: createMut.error.message }), _jsxs("div", { style: dialogActions, children: [_jsx("button", { style: btnSecondary, onClick: () => setCreateOpen(false), children: "Cancel" }), _jsx("button", { style: btnPrimary, onClick: () => createMut.mutate(), disabled: !newName || !newDoc || createMut.isPending, children: createMut.isPending ? 'Creating…' : 'Create' })] })] }) })), confirmDelete && (_jsx("div", { style: overlay, children: _jsxs("div", { style: dialog, children: [_jsx("h3", { style: { margin: '0 0 0.75rem' }, children: "Delete policy?" }), _jsxs("p", { style: { margin: '0 0 1rem', color: '#5f6b7a', fontSize: '0.9em' }, children: ["Delete policy ", _jsx("strong", { children: confirmDelete.policyName }), "? This cannot be undone."] }), deleteMut.error && _jsx("div", { style: mutError, children: deleteMut.error.message }), _jsxs("div", { style: dialogActions, children: [_jsx("button", { style: btnSecondary, onClick: () => setConfirmDelete(null), children: "Cancel" }), _jsx("button", { style: { ...btnPrimary, background: '#d13212', borderColor: '#d13212' }, onClick: () => deleteMut.mutate(confirmDelete.arn), disabled: deleteMut.isPending, children: deleteMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
const tabBtn = {
    background: 'none', border: 'none', padding: '0.5rem 1rem', cursor: 'pointer',
    fontSize: '0.9em', borderRadius: '4px 4px 0 0',
};
const btnPrimary = {
    background: '#e87600', color: '#fff', border: '1px solid #e87600',
    borderRadius: 6, padding: '0.4rem 1rem', cursor: 'pointer', fontWeight: 500, fontSize: '0.9em',
};
const btnSecondary = {
    background: 'transparent', color: '#5f6b7a', border: '1px solid #ccc',
    borderRadius: 6, padding: '0.4rem 1rem', cursor: 'pointer', fontSize: '0.9em',
};
const btnSmall = {
    background: 'transparent', color: '#0972d3', border: '1px solid #0972d3',
    borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: '0.8em',
};
const tableWrap = { border: '1px solid #e7e9ec', borderRadius: 8, overflow: 'hidden' };
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9em' };
const theadRow = { background: '#f4f5f7', borderBottom: '2px solid #e7e9ec' };
const th = {
    padding: '0.6rem 1rem', textAlign: 'left', fontWeight: 600, fontSize: '0.82em',
    color: '#5f6b7a', textTransform: 'uppercase', letterSpacing: '0.05em',
};
const td = { padding: '0.7rem 1rem', verticalAlign: 'middle' };
const overlay = {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.4)', display: 'flex',
    alignItems: 'center', justifyContent: 'center', zIndex: 1000,
};
const dialog = {
    background: '#fff', borderRadius: 10, padding: '1.5rem', minWidth: 440, maxWidth: 560,
    boxShadow: '0 8px 32px rgba(0,0,0,0.18)',
};
const labelStyle = { display: 'block', fontSize: '0.82em', fontWeight: 600, marginBottom: 4, color: '#2d3748' };
const inputStyle = {
    width: '100%', padding: '0.4rem 0.6rem', border: '1px solid #ccc', borderRadius: 6,
    marginBottom: '0.75rem', fontSize: '0.9em', boxSizing: 'border-box',
};
const dialogActions = { display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: '0.5rem' };
const mutError = { color: '#d13212', marginBottom: '0.5rem', fontSize: '0.85em' };
const loadingStyle = { padding: '2rem', color: '#5f6b7a' };
const errorStyle = { padding: '2rem', color: '#d13212' };
