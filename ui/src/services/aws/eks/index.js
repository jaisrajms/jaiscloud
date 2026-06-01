import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { EKSClusters } from './EKSClusters';
export function EKSRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(EKSClusters, {}) }), _jsx(Route, { path: "clusters", element: _jsx(EKSClusters, {}) })] }));
}
