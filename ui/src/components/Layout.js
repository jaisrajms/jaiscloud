import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { TopBar } from './TopBar';
import { Sidebar } from './Sidebar';
import { AccountProvider } from '../context/AccountContext';
function Shell({ children }) {
    const [sidebarOpen, setSidebarOpen] = useState(true);
    return (_jsxs("div", { style: { display: 'flex', flexDirection: 'column', height: '100vh', overflow: 'hidden' }, children: [_jsx(TopBar, { sidebarOpen: sidebarOpen, onToggleSidebar: () => setSidebarOpen((o) => !o) }), _jsxs("div", { style: { display: 'flex', flex: 1, overflow: 'hidden' }, children: [_jsx(Sidebar, { open: sidebarOpen }), _jsx("main", { style: mainStyle, children: children })] })] }));
}
export function Layout({ children }) {
    return (_jsx(AccountProvider, { children: _jsx(Shell, { children: children }) }));
}
const mainStyle = {
    flex: 1,
    overflowY: 'auto',
    padding: '1.5rem 2rem',
    background: '#f8f9fa',
    boxSizing: 'border-box',
};
