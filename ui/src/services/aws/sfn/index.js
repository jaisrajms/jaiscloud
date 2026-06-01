import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { SFNStateMachines } from './SFNStateMachines';
import { SFNExecutions } from './SFNExecutions';
export function SFNRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(Navigate, { to: "state-machines", replace: true }) }), _jsx(Route, { path: "state-machines", element: _jsx(SFNStateMachines, {}) }), _jsx(Route, { path: "executions", element: _jsx(SFNExecutions, {}) })] }));
}
