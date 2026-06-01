import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { useMeta } from './hooks/useMeta';
import { Layout } from './components/Layout';
import { SQSRoutes } from './services/aws/sqs';
import { LambdaRoutes } from './services/aws/lambda';
import { LogsRoutes } from './services/aws/logs';
import { S3Routes } from './services/aws/s3';
import { DynamoDBRoutes } from './services/aws/dynamodb';
import { SNSRoutes } from './services/aws/sns';
import { KMSRoutes } from './services/aws/kms';
import { SecretsManagerRoutes } from './services/aws/secretsmanager';
import { SSMRoutes } from './services/aws/ssm';
import { IAMRoutes } from './services/aws/iam';
import { CloudWatchRoutes } from './services/aws/cloudwatch';
import { EMRRoutes } from './services/aws/emr';
import { EMRContainersRoutes } from './services/aws/emroneks';
import { GlueRoutes } from './services/aws/glue';
import { EventBridgeRoutes } from './services/aws/eventbridge';
import { APIGatewayRoutes } from './services/aws/apigw';
import { SFNRoutes } from './services/aws/sfn';
import { AdminPanel } from './admin/AdminPanel';
export default function App() {
    const { data: meta, isLoading } = useMeta();
    if (isLoading) {
        return (_jsx("div", { style: { padding: '2rem', fontFamily: 'monospace' }, children: "Connecting to JaisCloud\u2026" }));
    }
    if (!meta) {
        return (_jsx("div", { style: { padding: '2rem', fontFamily: 'monospace', color: '#e53' }, children: "Could not connect to JaisCloud. Is the server running on port 4567?" }));
    }
    return (_jsx(Layout, { children: _jsxs(Routes, { children: [_jsx(Route, { path: "/", element: _jsx(Navigate, { to: "/aws/sqs", replace: true }) }), _jsx(Route, { path: "/aws/sqs/*", element: _jsx(SQSRoutes, {}) }), _jsx(Route, { path: "/aws/lambda/*", element: _jsx(LambdaRoutes, {}) }), _jsx(Route, { path: "/aws/logs/*", element: _jsx(LogsRoutes, {}) }), _jsx(Route, { path: "/aws/s3/*", element: _jsx(S3Routes, {}) }), _jsx(Route, { path: "/aws/dynamodb/*", element: _jsx(DynamoDBRoutes, {}) }), _jsx(Route, { path: "/aws/sns/*", element: _jsx(SNSRoutes, {}) }), _jsx(Route, { path: "/aws/kms/*", element: _jsx(KMSRoutes, {}) }), _jsx(Route, { path: "/aws/secretsmanager/*", element: _jsx(SecretsManagerRoutes, {}) }), _jsx(Route, { path: "/aws/ssm/*", element: _jsx(SSMRoutes, {}) }), _jsx(Route, { path: "/aws/iam/*", element: _jsx(IAMRoutes, {}) }), _jsx(Route, { path: "/aws/cloudwatch/*", element: _jsx(CloudWatchRoutes, {}) }), _jsx(Route, { path: "/aws/emr/*", element: _jsx(EMRRoutes, {}) }), _jsx(Route, { path: "/aws/emr-containers/*", element: _jsx(EMRContainersRoutes, {}) }), _jsx(Route, { path: "/aws/glue/*", element: _jsx(GlueRoutes, {}) }), _jsx(Route, { path: "/aws/eventbridge/*", element: _jsx(EventBridgeRoutes, {}) }), _jsx(Route, { path: "/aws/apigateway/*", element: _jsx(APIGatewayRoutes, {}) }), _jsx(Route, { path: "/aws/sfn/*", element: _jsx(SFNRoutes, {}) }), _jsx(Route, { path: "/admin", element: _jsx(AdminPanel, {}) }), _jsx(Route, { path: "*", element: _jsx(Navigate, { to: "/", replace: true }) })] }) }));
}
