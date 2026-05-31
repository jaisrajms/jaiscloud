import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { CloudWatchMetrics } from './CloudWatchMetrics';
import { CloudWatchAlarms } from './CloudWatchAlarms';
import { CloudWatchDashboards } from './CloudWatchDashboards';
export function CloudWatchRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(Navigate, { to: "metrics", replace: true }) }), _jsx(Route, { path: "metrics", element: _jsx(CloudWatchMetrics, {}) }), _jsx(Route, { path: "alarms", element: _jsx(CloudWatchAlarms, {}) }), _jsx(Route, { path: "dashboards", element: _jsx(CloudWatchDashboards, {}) })] }));
}
