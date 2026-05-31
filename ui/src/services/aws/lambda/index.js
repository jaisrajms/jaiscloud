import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { LambdaList } from './LambdaList';
import { LambdaDetail } from './LambdaDetail';
export function LambdaRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(LambdaList, {}) }), _jsx(Route, { path: ":name", element: _jsx(LambdaDetail, {}) })] }));
}
