import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { EMRList } from './EMRList';
import { EMRDetail } from './EMRDetail';
export function EMRRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(Navigate, { to: "clusters", replace: true }) }), _jsx(Route, { path: "clusters", element: _jsx(EMRList, {}) }), _jsx(Route, { path: ":id", element: _jsx(EMRDetail, {}) })] }));
}
