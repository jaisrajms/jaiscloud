import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { SESIdentities } from './SESIdentities';
export function SESRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(Navigate, { to: "identities", replace: true }) }), _jsx(Route, { path: "identities", element: _jsx(SESIdentities, {}) }), _jsx(Route, { path: "*", element: _jsx(Navigate, { to: "identities", replace: true }) })] }));
}
