import { jsx as _jsx } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { IAMList } from './IAMList';
export function IAMRoutes() {
    return (_jsx(Routes, { children: _jsx(Route, { index: true, element: _jsx(IAMList, {}) }) }));
}
