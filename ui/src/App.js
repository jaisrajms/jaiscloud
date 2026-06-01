import { jsx as _jsx, jsxs as _jsxs } from "react/jsx-runtime";
import { Routes, Route, Navigate } from 'react-router-dom';
import { useMeta } from './hooks/useMeta';
import { Layout } from './components/Layout';
import { AzureRoutes } from './services/azure';
import { GCPRoutes } from './services/gcp';
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
import { EC2Routes } from './services/aws/ec2';
import { ECSRoutes } from './services/aws/ecs';
import { EKSRoutes } from './services/aws/eks';
import { RDSRoutes } from './services/aws/rds';
import { ElastiCacheRoutes } from './services/aws/elasticache';
import { Route53Routes } from './services/aws/route53';
import { CloudFormationRoutes } from './services/aws/cloudformation';
import { KinesisRoutes } from './services/aws/kinesis';
import { FirehoseRoutes } from './services/aws/firehose';
import { SESRoutes } from './services/aws/ses';
import { ELBv2Routes } from './services/aws/elbv2';
import { AdminPanel } from './admin/AdminPanel';
export default function App() {
    const { data: meta, isLoading } = useMeta();
    if (isLoading) {
        return (_jsx("div", { style: { padding: '2rem', fontFamily: 'monospace' }, children: "Connecting to JaisCloud\u2026" }));
    }
    if (!meta) {
        return (_jsx("div", { style: { padding: '2rem', fontFamily: 'monospace', color: '#e53' }, children: "Could not connect to JaisCloud. Is the server running on port 4567?" }));
    }
    if (meta.cloud === 'azure') {
        return (_jsx(Layout, { children: _jsxs(Routes, { children: [_jsx(Route, { path: "/", element: _jsx(Navigate, { to: "/azure", replace: true }) }), _jsx(Route, { path: "/azure/*", element: _jsx(AzureRoutes, {}) }), _jsx(Route, { path: "/admin", element: _jsx(AdminPanel, {}) }), _jsx(Route, { path: "*", element: _jsx(Navigate, { to: "/azure", replace: true }) })] }) }));
    }
    if (meta.cloud === 'gcp') {
        return (_jsx(Layout, { children: _jsxs(Routes, { children: [_jsx(Route, { path: "/", element: _jsx(Navigate, { to: "/gcp", replace: true }) }), _jsx(Route, { path: "/gcp/*", element: _jsx(GCPRoutes, {}) }), _jsx(Route, { path: "/admin", element: _jsx(AdminPanel, {}) }), _jsx(Route, { path: "*", element: _jsx(Navigate, { to: "/gcp", replace: true }) })] }) }));
    }
    return (_jsx(Layout, { children: _jsxs(Routes, { children: [_jsx(Route, { path: "/", element: _jsx(Navigate, { to: "/aws/sqs", replace: true }) }), _jsx(Route, { path: "/aws/sqs/*", element: _jsx(SQSRoutes, {}) }), _jsx(Route, { path: "/aws/lambda/*", element: _jsx(LambdaRoutes, {}) }), _jsx(Route, { path: "/aws/logs/*", element: _jsx(LogsRoutes, {}) }), _jsx(Route, { path: "/aws/s3/*", element: _jsx(S3Routes, {}) }), _jsx(Route, { path: "/aws/dynamodb/*", element: _jsx(DynamoDBRoutes, {}) }), _jsx(Route, { path: "/aws/sns/*", element: _jsx(SNSRoutes, {}) }), _jsx(Route, { path: "/aws/kms/*", element: _jsx(KMSRoutes, {}) }), _jsx(Route, { path: "/aws/secretsmanager/*", element: _jsx(SecretsManagerRoutes, {}) }), _jsx(Route, { path: "/aws/ssm/*", element: _jsx(SSMRoutes, {}) }), _jsx(Route, { path: "/aws/iam/*", element: _jsx(IAMRoutes, {}) }), _jsx(Route, { path: "/aws/cloudwatch/*", element: _jsx(CloudWatchRoutes, {}) }), _jsx(Route, { path: "/aws/emr/*", element: _jsx(EMRRoutes, {}) }), _jsx(Route, { path: "/aws/emr-containers/*", element: _jsx(EMRContainersRoutes, {}) }), _jsx(Route, { path: "/aws/glue/*", element: _jsx(GlueRoutes, {}) }), _jsx(Route, { path: "/aws/eventbridge/*", element: _jsx(EventBridgeRoutes, {}) }), _jsx(Route, { path: "/aws/apigateway/*", element: _jsx(APIGatewayRoutes, {}) }), _jsx(Route, { path: "/aws/sfn/*", element: _jsx(SFNRoutes, {}) }), _jsx(Route, { path: "/aws/ec2/*", element: _jsx(EC2Routes, {}) }), _jsx(Route, { path: "/aws/ecs/*", element: _jsx(ECSRoutes, {}) }), _jsx(Route, { path: "/aws/eks/*", element: _jsx(EKSRoutes, {}) }), _jsx(Route, { path: "/aws/rds/*", element: _jsx(RDSRoutes, {}) }), _jsx(Route, { path: "/aws/elasticache/*", element: _jsx(ElastiCacheRoutes, {}) }), _jsx(Route, { path: "/aws/route53/*", element: _jsx(Route53Routes, {}) }), _jsx(Route, { path: "/aws/cloudformation/*", element: _jsx(CloudFormationRoutes, {}) }), _jsx(Route, { path: "/aws/kinesis/*", element: _jsx(KinesisRoutes, {}) }), _jsx(Route, { path: "/aws/firehose/*", element: _jsx(FirehoseRoutes, {}) }), _jsx(Route, { path: "/aws/ses/*", element: _jsx(SESRoutes, {}) }), _jsx(Route, { path: "/aws/elbv2/*", element: _jsx(ELBv2Routes, {}) }), _jsx(Route, { path: "/admin", element: _jsx(AdminPanel, {}) }), _jsx(Route, { path: "*", element: _jsx(Navigate, { to: "/", replace: true }) })] }) }));
}
