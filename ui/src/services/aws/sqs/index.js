import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { SQSList } from './SQSList';
import { SQSDetail } from './SQSDetail';
export function SQSRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(SQSList, {}) }), _jsx(Route, { path: ":queueUrl", element: _jsx(SQSDetail, {}) })] }));
}
