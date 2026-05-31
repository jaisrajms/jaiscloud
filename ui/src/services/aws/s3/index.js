import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { S3List } from './S3List';
import { S3Detail } from './S3Detail';
export function S3Routes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(S3List, {}) }), _jsx(Route, { path: ":bucket", element: _jsx(S3Detail, {}) })] }));
}
