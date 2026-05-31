import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listDatabases, createDatabase, deleteDatabase, listTables, createTable, deleteTable, } from '../../../api/glue';
import { EmptyState } from '../../../components/EmptyState';
const tableStyle = { width: '100%', borderCollapse: 'collapse', fontSize: '0.9rem' };
const thStyle = { textAlign: 'left', padding: '0.6rem 1rem', borderBottom: '2px solid #2d3748', color: '#b0bec5', fontWeight: 600, fontSize: '0.78rem', textTransform: 'uppercase' };
const tdStyle = { padding: '0.6rem 1rem', verticalAlign: 'middle' };
const btnStyle = { padding: '0.4rem 1rem', borderRadius: 4, border: 'none', cursor: 'pointer', fontSize: '0.85rem', background: '#0073bb', color: '#fff' };
const inputStyle = { padding: '0.4rem 0.75rem', borderRadius: 4, border: '1px solid #2d3748', background: '#1a2332', color: '#e8eaf0', fontSize: '0.85rem', width: '100%', boxSizing: 'border-box' };
const overlayStyle = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const modalStyle = { background: '#1a2332', borderRadius: 8, padding: '2rem', minWidth: 420, maxWidth: 560 };
export function GlueDatabases() {
    const qc = useQueryClient();
    const [selectedDB, setSelectedDB] = useState(null);
    const [createDBOpen, setCreateDBOpen] = useState(false);
    const [deleteDBTarget, setDeleteDBTarget] = useState(null);
    const [createTableOpen, setCreateTableOpen] = useState(false);
    const [deleteTableTarget, setDeleteTableTarget] = useState(null);
    const [dbForm, setDbForm] = useState({ name: '', description: '', locationUri: '' });
    const [tableForm, setTableForm] = useState({ name: '', description: '', location: '', storageType: '' });
    const { data: dbData, isLoading } = useQuery({
        queryKey: ['glue', 'databases'],
        queryFn: () => listDatabases(),
    });
    const { data: tableData } = useQuery({
        queryKey: ['glue', 'tables', selectedDB?.name],
        queryFn: () => listTables(selectedDB.name),
        enabled: !!selectedDB,
    });
    const createDBMut = useMutation({
        mutationFn: () => createDatabase({ name: dbForm.name, description: dbForm.description || undefined, locationUri: dbForm.locationUri || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['glue', 'databases'] });
            setCreateDBOpen(false);
            setDbForm({ name: '', description: '', locationUri: '' });
        },
    });
    const deleteDBMut = useMutation({
        mutationFn: (name) => deleteDatabase(name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['glue', 'databases'] });
            if (deleteDBTarget?.name === selectedDB?.name)
                setSelectedDB(null);
            setDeleteDBTarget(null);
        },
    });
    const createTableMut = useMutation({
        mutationFn: () => createTable(selectedDB.name, { name: tableForm.name, description: tableForm.description || undefined, location: tableForm.location || undefined, storageType: tableForm.storageType || undefined }),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['glue', 'tables', selectedDB?.name] });
            setCreateTableOpen(false);
            setTableForm({ name: '', description: '', location: '', storageType: '' });
        },
    });
    const deleteTableMut = useMutation({
        mutationFn: ({ db, name }) => deleteTable(db, name),
        onSuccess: () => {
            void qc.invalidateQueries({ queryKey: ['glue', 'tables', selectedDB?.name] });
            setDeleteTableTarget(null);
        },
    });
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading databases\u2026" });
    const databases = dbData?.items ?? [];
    const tables = tableData?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "Glue Databases" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [databases.length, " database", databases.length !== 1 ? 's' : ''] })] }), _jsx("button", { style: btnStyle, onClick: () => setCreateDBOpen(true), children: "Create Database" })] }), _jsxs("div", { style: { display: 'grid', gridTemplateColumns: selectedDB ? '1fr 1fr' : '1fr', gap: '1.5rem' }, children: [_jsx("div", { children: databases.length === 0 ? (_jsx(EmptyState, { title: "No databases. Create one to store your Glue tables." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Name', 'Description', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: databases.map(db => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748', cursor: 'pointer', background: selectedDB?.name === db.name ? '#1e2d3d' : 'transparent' }, onClick: () => setSelectedDB(db), children: [_jsx("td", { style: { ...tdStyle, fontWeight: 600 }, children: db.name }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.85rem' }, children: db.description || '—' }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, onClick: e => e.stopPropagation(), children: _jsx("button", { style: { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setDeleteDBTarget(db), children: "Delete" }) })] }, db.name))) })] })) }), selectedDB && (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }, children: [_jsxs("h3", { style: { margin: 0, fontWeight: 600, fontSize: '1rem' }, children: ["Tables in ", _jsx("em", { children: selectedDB.name }), " (", tables.length, ")"] }), _jsxs("div", { style: { display: 'flex', gap: '0.5rem' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0', padding: '0.3rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setSelectedDB(null), children: "\u2715" }), _jsx("button", { style: { ...btnStyle, fontSize: '0.8rem', padding: '0.3rem 0.75rem' }, onClick: () => setCreateTableOpen(true), children: "Create Table" })] })] }), tables.length === 0 ? (_jsx(EmptyState, { title: "No tables in this database." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Name', 'Location', ''].map(h => _jsx("th", { style: thStyle, children: h }, h)) }) }), _jsx("tbody", { children: tables.map(t => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: { ...tdStyle, fontWeight: 600 }, children: t.name }), _jsx("td", { style: { ...tdStyle, color: '#b0bec5', fontSize: '0.82rem', fontFamily: 'monospace' }, children: t.location || '—' }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, children: _jsx("button", { style: { ...btnStyle, background: 'transparent', color: '#d13212', border: '1px solid #d13212', padding: '0.25rem 0.6rem', fontSize: '0.8rem' }, onClick: () => setDeleteTableTarget(t), children: "Delete" }) })] }, t.name))) })] }))] }))] }), createDBOpen && (_jsx("div", { style: overlayStyle, onClick: () => setCreateDBOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: "Create Database" }), [
                            { key: 'name', label: 'Name *', placeholder: 'my_database' },
                            { key: 'description', label: 'Description', placeholder: '' },
                            { key: 'locationUri', label: 'Location URI', placeholder: 's3://bucket/prefix' },
                        ].map(f => (_jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: f.label }), _jsx("input", { style: inputStyle, placeholder: f.placeholder, value: dbForm[f.key], onChange: e => setDbForm(p => ({ ...p, [f.key]: e.target.value })) })] }, f.key))), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setCreateDBOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !dbForm.name || createDBMut.isPending, onClick: () => createDBMut.mutate(), children: createDBMut.isPending ? 'Creating…' : 'Create' })] })] }) })), deleteDBTarget && (_jsx("div", { style: overlayStyle, onClick: () => setDeleteDBTarget(null), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem', fontWeight: 600 }, children: "Delete Database?" }), _jsxs("p", { style: { color: '#b0bec5', marginBottom: '1.5rem' }, children: ["Delete database ", _jsx("strong", { children: deleteDBTarget.name }), "?"] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setDeleteDBTarget(null), children: "Cancel" }), _jsx("button", { style: { ...btnStyle, background: '#d13212' }, disabled: deleteDBMut.isPending, onClick: () => deleteDBMut.mutate(deleteDBTarget.name), children: deleteDBMut.isPending ? 'Deleting…' : 'Delete' })] })] }) })), createTableOpen && selectedDB && (_jsx("div", { style: overlayStyle, onClick: () => setCreateTableOpen(false), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsxs("h3", { style: { margin: '0 0 1.5rem', fontWeight: 600 }, children: ["Create Table in ", selectedDB.name] }), [
                            { key: 'name', label: 'Name *', placeholder: 'my_table' },
                            { key: 'description', label: 'Description', placeholder: '' },
                            { key: 'location', label: 'S3 Location', placeholder: 's3://bucket/prefix/' },
                            { key: 'storageType', label: 'Input Format', placeholder: 'org.apache.hadoop.mapred.TextInputFormat' },
                        ].map(f => (_jsxs("div", { style: { display: 'flex', flexDirection: 'column', gap: '0.3rem', marginBottom: '1rem' }, children: [_jsx("label", { style: { fontSize: '0.8rem', color: '#b0bec5' }, children: f.label }), _jsx("input", { style: inputStyle, placeholder: f.placeholder, value: tableForm[f.key], onChange: e => setTableForm(p => ({ ...p, [f.key]: e.target.value })) })] }, f.key))), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setCreateTableOpen(false), children: "Cancel" }), _jsx("button", { style: btnStyle, disabled: !tableForm.name || createTableMut.isPending, onClick: () => createTableMut.mutate(), children: createTableMut.isPending ? 'Creating…' : 'Create' })] })] }) })), deleteTableTarget && (_jsx("div", { style: overlayStyle, onClick: () => setDeleteTableTarget(null), children: _jsxs("div", { style: modalStyle, onClick: e => e.stopPropagation(), children: [_jsx("h3", { style: { margin: '0 0 1rem', fontWeight: 600 }, children: "Delete Table?" }), _jsxs("p", { style: { color: '#b0bec5', marginBottom: '1.5rem' }, children: ["Delete table ", _jsx("strong", { children: deleteTableTarget.name }), "?"] }), _jsxs("div", { style: { display: 'flex', gap: '0.75rem', justifyContent: 'flex-end' }, children: [_jsx("button", { style: { ...btnStyle, background: '#2d3748', color: '#e8eaf0' }, onClick: () => setDeleteTableTarget(null), children: "Cancel" }), _jsx("button", { style: { ...btnStyle, background: '#d13212' }, disabled: deleteTableMut.isPending, onClick: () => deleteTableMut.mutate({ db: deleteTableTarget.databaseName, name: deleteTableTarget.name }), children: deleteTableMut.isPending ? 'Deleting…' : 'Delete' })] })] }) }))] }));
}
