import { jsx as _jsx } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { KMSList } from './KMSList';
export function KMSRoutes() {
    return (_jsx(Routes, { children: _jsx(Route, { index: true, element: _jsx(KMSList, {}) }) }));
}
