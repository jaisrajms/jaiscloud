import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { EMRContainersList } from './EMRContainersList';
import { EMRContainersDetail } from './EMRContainersDetail';
export function EMRContainersRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(Navigate, { to: "clusters", replace: true }) }), _jsx(Route, { path: "clusters", element: _jsx(EMRContainersList, {}) }), _jsx(Route, { path: ":id", element: _jsx(EMRContainersDetail, {}) })] }));
}
