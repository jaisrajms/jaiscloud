import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { EventBridgeRules } from './EventBridgeRules';
import { EventBridgeBuses } from './EventBridgeBuses';
export function EventBridgeRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(Navigate, { to: "rules", replace: true }) }), _jsx(Route, { path: "rules", element: _jsx(EventBridgeRules, {}) }), _jsx(Route, { path: "buses", element: _jsx(EventBridgeBuses, {}) })] }));
}
