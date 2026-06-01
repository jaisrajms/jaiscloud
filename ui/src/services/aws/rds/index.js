import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { RDSInstances } from './RDSInstances';
export function RDSRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(RDSInstances, {}) }), _jsx(Route, { path: "instances", element: _jsx(RDSInstances, {}) })] }));
}
