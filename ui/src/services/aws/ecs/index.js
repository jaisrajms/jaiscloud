import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { ECSClusters } from './ECSClusters';
export function ECSRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(ECSClusters, {}) }), _jsx(Route, { path: "clusters", element: _jsx(ECSClusters, {}) })] }));
}
