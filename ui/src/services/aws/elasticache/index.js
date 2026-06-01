import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { ElastiCacheClusters } from './ElastiCacheClusters';
export function ElastiCacheRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(ElastiCacheClusters, {}) }), _jsx(Route, { path: "clusters", element: _jsx(ElastiCacheClusters, {}) })] }));
}
