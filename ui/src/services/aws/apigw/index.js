import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { APIGatewayAPIs } from './APIGatewayAPIs';
export function APIGatewayRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(Navigate, { to: "apis", replace: true }) }), _jsx(Route, { path: "apis", element: _jsx(APIGatewayAPIs, {}) })] }));
}
