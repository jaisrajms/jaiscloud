import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { SNSList } from './SNSList';
import { SNSDetail } from './SNSDetail';
export function SNSRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(SNSList, {}) }), _jsx(Route, { path: ":topicArn", element: _jsx(SNSDetail, {}) })] }));
}
