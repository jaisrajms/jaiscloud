import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
export function EmptyState({ icon, title, description, cta, onCta }) {
    return (_jsxs("div", { style: { textAlign: 'center', padding: '4rem 2rem', color: '#5f6b7a' }, children: [_jsx("div", { style: { fontSize: '2.5rem', marginBottom: '1rem', opacity: 0.5 }, children: icon ?? '☁' }), _jsx("h3", { style: { margin: '0 0 0.5rem', color: '#16191f', fontSize: '1.1rem', fontWeight: 600 }, children: title }), description && (_jsx("p", { style: { margin: '0 0 1.5rem', fontSize: '0.9em', maxWidth: 380, marginLeft: 'auto', marginRight: 'auto' }, children: description })), cta && onCta && (_jsx("button", { onClick: onCta, style: {
                    background: '#e77600',
                    color: '#fff',
                    border: 'none',
                    borderRadius: 4,
                    padding: '0.5rem 1.25rem',
                    fontSize: '0.9em',
                    cursor: 'pointer',
                    fontWeight: 500,
                }, children: cta }))] }));
}
