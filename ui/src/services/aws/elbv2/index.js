import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { ELBv2LoadBalancers } from './ELBv2LoadBalancers';
export function ELBv2Routes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(Navigate, { to: "load-balancers", replace: true }) }), _jsx(Route, { path: "load-balancers", element: _jsx(ELBv2LoadBalancers, {}) }), _jsx(Route, { path: "*", element: _jsx(Navigate, { to: "load-balancers", replace: true }) })] }));
}
