import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { GlueDatabases } from './GlueDatabases';
import { GlueJobs } from './GlueJobs';
import { GlueCrawlers } from './GlueCrawlers';
export function GlueRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(Navigate, { to: "databases", replace: true }) }), _jsx(Route, { path: "databases", element: _jsx(GlueDatabases, {}) }), _jsx(Route, { path: "jobs", element: _jsx(GlueJobs, {}) }), _jsx(Route, { path: "crawlers", element: _jsx(GlueCrawlers, {}) })] }));
}
