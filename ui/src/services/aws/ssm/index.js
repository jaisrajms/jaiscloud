import { jsx as _jsx } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { SSMList } from './SSMList';
export function SSMRoutes() {
    return (_jsx(Routes, { children: _jsx(Route, { index: true, element: _jsx(SSMList, {}) }) }));
}
