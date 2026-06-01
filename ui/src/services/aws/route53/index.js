import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { Route53Zones } from './Route53Zones';
export function Route53Routes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(Route53Zones, {}) }), _jsx(Route, { path: "zones", element: _jsx(Route53Zones, {}) })] }));
}
