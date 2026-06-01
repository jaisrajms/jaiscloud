import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { EC2Instances } from './EC2Instances';
export function EC2Routes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(EC2Instances, {}) }), _jsx(Route, { path: "instances", element: _jsx(EC2Instances, {}) })] }));
}
