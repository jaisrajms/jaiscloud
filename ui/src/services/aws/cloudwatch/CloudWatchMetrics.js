import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { listMetrics, getMetricStatistics } from '../../../api/cloudwatch';
import { EmptyState } from '../../../components/EmptyState';
export function CloudWatchMetrics() {
    const [nsFilter, setNsFilter] = useState('');
    const [selected, setSelected] = useState(null);
    const [statsData, setStatsData] = useState(null);
    const [statsLabel, setStatsLabel] = useState('');
    const [loadingStats, setLoadingStats] = useState(false);
    const { data, isLoading, error } = useQuery({
        queryKey: ['cloudwatch', 'metrics', nsFilter],
        queryFn: () => listMetrics(nsFilter ? { namespace: nsFilter } : undefined),
    });
    async function viewStats(metric) {
        setSelected(metric);
        setLoadingStats(true);
        setStatsData(null);
        try {
            const now = new Date();
            const start = new Date(now.getTime() - 3 * 60 * 60 * 1000);
            const resp = await getMetricStatistics({
                namespace: metric.namespace,
                metricName: metric.metricName,
                startTime: start.toISOString(),
                endTime: now.toISOString(),
                period: 300,
                statistics: ['Sum', 'Average', 'Maximum', 'SampleCount'],
            });
            setStatsLabel(resp.label);
            setStatsData(resp.datapoints);
        }
        catch {
            setStatsData([]);
        }
        finally {
            setLoadingStats(false);
        }
    }
    if (isLoading)
        return _jsx("div", { style: { padding: '2rem', color: '#5f6b7a' }, children: "Loading metrics\u2026" });
    if (error)
        return _jsxs("div", { style: { padding: '2rem', color: '#d13212' }, children: ["Failed to load: ", error.message] });
    const metrics = data?.items ?? [];
    return (_jsxs("div", { children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1.5rem' }, children: [_jsxs("div", { children: [_jsx("h2", { style: { margin: 0, fontSize: '1.4rem', fontWeight: 600 }, children: "CloudWatch Metrics" }), _jsxs("span", { style: { fontSize: '0.85em', color: '#5f6b7a' }, children: [metrics.length, " metric", metrics.length !== 1 ? 's' : ''] })] }), _jsx("input", { type: "text", placeholder: "Filter by namespace\u2026", value: nsFilter, onChange: e => setNsFilter(e.target.value), style: inputStyle })] }), metrics.length === 0 ? (_jsx(EmptyState, { title: "No metrics found. Publish metric data via PutMetricData to see metrics here." })) : (_jsxs("table", { style: tableStyle, children: [_jsx("thead", { children: _jsx("tr", { children: ['Namespace', 'Metric Name', ''].map(h => (_jsx("th", { style: thStyle, children: h }, h))) }) }), _jsx("tbody", { children: metrics.map((m, i) => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: tdStyle, children: m.namespace }), _jsx("td", { style: tdStyle, children: m.metricName }), _jsx("td", { style: { ...tdStyle, textAlign: 'right' }, children: _jsx("button", { onClick: () => viewStats(m), style: actionBtnStyle, children: "View Stats" }) })] }, i))) })] })), selected && (_jsxs("div", { style: drawerStyle, children: [_jsxs("div", { style: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '1rem' }, children: [_jsxs("strong", { style: { color: '#e2e8f0' }, children: [statsLabel || selected.metricName, " \u2014 last 3 hours (5m periods)"] }), _jsx("button", { onClick: () => { setSelected(null); setStatsData(null); }, style: closeBtnStyle, children: "\u2715" })] }), loadingStats ? (_jsx("div", { style: { color: '#5f6b7a' }, children: "Loading statistics\u2026" })) : !statsData || statsData.length === 0 ? (_jsx("div", { style: { color: '#5f6b7a' }, children: "No datapoints in the last 3 hours." })) : (_jsxs("table", { style: { ...tableStyle, marginTop: 0 }, children: [_jsx("thead", { children: _jsx("tr", { children: ['Timestamp', 'Sum', 'Average', 'Maximum', 'Samples'].map(h => (_jsx("th", { style: thStyle, children: h }, h))) }) }), _jsx("tbody", { children: statsData.map((dp, i) => (_jsxs("tr", { style: { borderBottom: '1px solid #2d3748' }, children: [_jsx("td", { style: tdStyle, children: dp.timestamp }), _jsx("td", { style: tdStyle, children: dp.sum?.toFixed(2) ?? '—' }), _jsx("td", { style: tdStyle, children: dp.average?.toFixed(2) ?? '—' }), _jsx("td", { style: tdStyle, children: dp.maximum?.toFixed(2) ?? '—' }), _jsx("td", { style: tdStyle, children: dp.sampleCount ?? '—' })] }, i))) })] }))] }))] }));
}
const inputStyle = {
    background: '#1b2a3b',
    border: '1px solid #2d3748',
    borderRadius: 4,
    color: '#e2e8f0',
    padding: '0.4rem 0.75rem',
    fontSize: '0.85em',
    width: 220,
};
const tableStyle = {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: '0.88em',
    marginTop: '1rem',
};
const thStyle = {
    textAlign: 'left',
    padding: '0.5rem 0.75rem',
    color: '#8892a4',
    fontWeight: 500,
    borderBottom: '1px solid #2d3748',
    fontSize: '0.8em',
    textTransform: 'uppercase',
    letterSpacing: '0.05em',
};
const tdStyle = {
    padding: '0.6rem 0.75rem',
    color: '#c9cdd4',
    verticalAlign: 'middle',
};
const actionBtnStyle = {
    background: 'none',
    border: '1px solid #2d3748',
    borderRadius: 4,
    color: '#0972d3',
    cursor: 'pointer',
    padding: '0.25rem 0.75rem',
    fontSize: '0.82em',
};
const drawerStyle = {
    marginTop: '2rem',
    padding: '1.25rem',
    background: '#1b2a3b',
    border: '1px solid #2d3748',
    borderRadius: 6,
};
const closeBtnStyle = {
    background: 'none',
    border: 'none',
    color: '#8892a4',
    cursor: 'pointer',
    fontSize: '1rem',
};
