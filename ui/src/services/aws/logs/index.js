import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { LogGroupList } from './LogGroupList';
import { LogGroupDetail } from './LogGroupDetail';
import { LogStreamView } from './LogStreamView';
import { LogInsights } from './LogInsights';
export function LogsRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(Navigate, { to: "groups", replace: true }) }), _jsx(Route, { path: "groups", element: _jsx(LogGroupList, {}) }), _jsx(Route, { path: "groups/:name", element: _jsx(LogGroupDetail, {}) }), _jsx(Route, { path: "groups/:name/streams/:stream", element: _jsx(LogStreamView, {}) }), _jsx(Route, { path: "insights", element: _jsx(LogInsights, {}) })] }));
}
