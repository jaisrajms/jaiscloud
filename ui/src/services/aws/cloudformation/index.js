import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { CFNStacks } from './CFNStacks';
export function CloudFormationRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(CFNStacks, {}) }), _jsx(Route, { path: "stacks", element: _jsx(CFNStacks, {}) })] }));
}
