import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { FirehoseStreams } from './FirehoseStreams';
export function FirehoseRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(Navigate, { to: "streams", replace: true }) }), _jsx(Route, { path: "streams", element: _jsx(FirehoseStreams, {}) }), _jsx(Route, { path: "*", element: _jsx(Navigate, { to: "streams", replace: true }) })] }));
}
