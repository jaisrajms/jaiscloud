import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { NavLink, useLocation } from 'react-router-dom';
const navTree = [
    {
        id: 'sqs',
        label: 'SQS',
        basePath: '/aws/sqs',
        rootPath: '/aws/sqs',
        children: [
            { label: 'Queues', path: '/aws/sqs' },
        ],
    },
    {
        id: 'lambda',
        label: 'Lambda',
        basePath: '/aws/lambda',
        rootPath: '/aws/lambda',
        children: [
            { label: 'Functions', path: '/aws/lambda' },
        ],
    },
    {
        id: 'logs',
        label: 'CloudWatch Logs',
        basePath: '/aws/logs',
        rootPath: '/aws/logs/groups',
        children: [
            { label: 'Log Groups', path: '/aws/logs/groups' },
        ],
    },
];
export function Sidebar({ open }) {
    const { pathname } = useLocation();
    return (_jsx("aside", { style: {
            ...sidebarStyle,
            width: open ? SIDEBAR_WIDTH : 0,
            minWidth: open ? SIDEBAR_WIDTH : 0,
            overflowY: open ? 'auto' : 'hidden',
            overflowX: 'hidden',
            position: 'relative',
            zIndex: 10,
        }, children: _jsx("nav", { style: { width: SIDEBAR_WIDTH, paddingTop: '0.5rem' }, children: navTree.map((section) => {
                const isActive = pathname.startsWith(section.basePath);
                return (_jsxs("div", { style: { marginBottom: '0.15rem' }, children: [_jsxs(NavLink, { to: section.rootPath, style: {
                                ...sectionHeaderStyle,
                                background: isActive ? 'rgba(232,118,0,0.12)' : 'none',
                                color: isActive ? '#e87600' : '#c9cdd4',
                                textDecoration: 'none',
                                display: 'flex',
                            }, children: [_jsx("span", { style: { fontSize: '0.6em', opacity: 0.7, width: 16, textAlign: 'center', flexShrink: 0, paddingTop: 1 }, children: isActive ? '▼' : '▶' }), _jsx("span", { style: { fontWeight: isActive ? 600 : 400, fontSize: '0.83em' }, children: section.label })] }), isActive && (_jsx("div", { children: section.children.map((child) => (_jsx(NavLink, { to: child.path, end: true, style: ({ isActive: childActive }) => ({
                                    ...childLinkStyle,
                                    background: childActive ? 'rgba(9,114,211,0.12)' : 'none',
                                    color: childActive ? '#0972d3' : '#8d9daa',
                                    fontWeight: childActive ? 500 : 400,
                                }), children: child.label }, child.path))) }))] }, section.id));
            }) }) }));
}
const SIDEBAR_WIDTH = 210;
const sidebarStyle = {
    background: '#1b2530',
    borderRight: '1px solid #2d3748',
    flexShrink: 0,
    transition: 'width 0.18s ease, min-width 0.18s ease',
};
const sectionHeaderStyle = {
    alignItems: 'center',
    gap: '0.5rem',
    padding: '0.55rem 1rem',
    cursor: 'pointer',
    userSelect: 'none',
};
const childLinkStyle = {
    display: 'block',
    padding: '0.4rem 1rem 0.4rem 2.25rem',
    textDecoration: 'none',
    fontSize: '0.82em',
    transition: 'background 0.1s',
};
