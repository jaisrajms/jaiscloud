import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route } from 'react-router-dom';
import { SecretsList } from './SecretsList';
import { SecretsDetail } from './SecretsDetail';
export function SecretsManagerRoutes() {
    return (_jsxs(Routes, { children: [_jsx(Route, { index: true, element: _jsx(SecretsList, {}) }), _jsx(Route, { path: ":name", element: _jsx(SecretsDetail, {}) })] }));
}
