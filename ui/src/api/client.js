/**
 * Base fetch wrapper. Reads session token from cookie and sets
 * credentials: 'include' so the session cookie is sent cross-origin
 * in dev mode (Vite dev server → 4567).
 */
const BASE = '';
let _currentAccount = '';
/** Called by AccountContext whenever the selected account changes. */
export function setCurrentAccount(id) {
    _currentAccount = id;
}
function getCookie(name) {
    const match = document.cookie
        .split('; ')
        .find((row) => row.startsWith(`${name}=`));
    return match?.split('=')[1];
}
export class APIError extends Error {
    status;
    code;
    constructor(status, code, message) {
        super(message);
        this.status = status;
        this.code = code;
        this.name = 'APIError';
    }
}
async function request(method, path, body, params) {
    const url = new URL(`${BASE}${path}`, window.location.origin);
    if (_currentAccount) {
        url.searchParams.set('account', _currentAccount);
    }
    if (params) {
        Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, String(v)));
    }
    const token = getCookie('session');
    const headers = {};
    if (token) {
        headers['Authorization'] = `Bearer ${token}`;
    }
    if (body !== undefined) {
        headers['Content-Type'] = 'application/json';
    }
    const res = await fetch(url.toString(), {
        method,
        headers,
        credentials: 'include',
        body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    if (!res.ok) {
        let code = 'UnknownError';
        let message = res.statusText;
        try {
            const err = await res.json();
            code = err.code ?? code;
            message = err.message ?? message;
        }
        catch {
            // ignore JSON parse errors
        }
        throw new APIError(res.status, code, message);
    }
    if (res.status === 204) {
        return undefined;
    }
    return res.json();
}
export const api = {
    get: (path, params) => request('GET', path, undefined, params),
    post: (path, body, params) => request('POST', path, body, params),
    put: (path, body) => request('PUT', path, body),
    patch: (path, body) => request('PATCH', path, body),
    delete: (path, params) => request('DELETE', path, undefined, params),
};
