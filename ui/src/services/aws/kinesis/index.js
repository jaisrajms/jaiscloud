import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { KinesisStreams } from './KinesisStreams';
export function KinesisRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(Navigate, { to: "streams", replace: true }) }), _jsx(Route, { path: "streams", element: _jsx(KinesisStreams, {}) }), _jsx(Route, { path: "*", element: _jsx(Navigate, { to: "streams", replace: true }) })] }));
}
