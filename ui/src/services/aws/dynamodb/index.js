import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { DynamoDBList } from './DynamoDBList';
import { DynamoDBDetail } from './DynamoDBDetail';
export function DynamoDBRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(DynamoDBList, {}) }), _jsx(Route, { path: ":table", element: _jsx(DynamoDBDetail, {}) })] }));
}
