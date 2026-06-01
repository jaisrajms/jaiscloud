import { NavLink, useLocation } from 'react-router-dom'

interface NavChild {
  label: string
  path: string
}

interface NavSection {
  id: string
  label: string
  basePath: string
  rootPath: string  // navigate here when section header is clicked
  children: NavChild[]
}

const navTree: NavSection[] = [
  {
    id: 's3',
    label: 'S3',
    basePath: '/aws/s3',
    rootPath: '/aws/s3',
    children: [
      { label: 'Buckets', path: '/aws/s3' },
    ],
  },
  {
    id: 'dynamodb',
    label: 'DynamoDB',
    basePath: '/aws/dynamodb',
    rootPath: '/aws/dynamodb',
    children: [
      { label: 'Tables', path: '/aws/dynamodb' },
    ],
  },
  {
    id: 'sqs',
    label: 'SQS',
    basePath: '/aws/sqs',
    rootPath: '/aws/sqs',
    children: [
      { label: 'Queues', path: '/aws/sqs' },
    ],
  },
  {
    id: 'sns',
    label: 'SNS',
    basePath: '/aws/sns',
    rootPath: '/aws/sns',
    children: [
      { label: 'Topics', path: '/aws/sns' },
    ],
  },
  {
    id: 'lambda',
    label: 'Lambda',
    basePath: '/aws/lambda',
    rootPath: '/aws/lambda',
    children: [
      { label: 'Functions', path: '/aws/lambda' },
    ],
  },
  {
    id: 'logs',
    label: 'CloudWatch Logs',
    basePath: '/aws/logs',
    rootPath: '/aws/logs/groups',
    children: [
      { label: 'Log Groups', path: '/aws/logs/groups' },
      { label: 'Insights', path: '/aws/logs/insights' },
    ],
  },
  {
    id: 'cloudwatch',
    label: 'CloudWatch',
    basePath: '/aws/cloudwatch',
    rootPath: '/aws/cloudwatch/metrics',
    children: [
      { label: 'Metrics', path: '/aws/cloudwatch/metrics' },
      { label: 'Alarms', path: '/aws/cloudwatch/alarms' },
      { label: 'Dashboards', path: '/aws/cloudwatch/dashboards' },
    ],
  },
  {
    id: 'kms',
    label: 'KMS',
    basePath: '/aws/kms',
    rootPath: '/aws/kms',
    children: [
      { label: 'Keys', path: '/aws/kms' },
    ],
  },
  {
    id: 'secretsmanager',
    label: 'Secrets Manager',
    basePath: '/aws/secretsmanager',
    rootPath: '/aws/secretsmanager',
    children: [
      { label: 'Secrets', path: '/aws/secretsmanager' },
    ],
  },
  {
    id: 'ssm',
    label: 'SSM',
    basePath: '/aws/ssm',
    rootPath: '/aws/ssm',
    children: [
      { label: 'Parameters', path: '/aws/ssm' },
    ],
  },
  {
    id: 'iam',
    label: 'IAM',
    basePath: '/aws/iam',
    rootPath: '/aws/iam',
    children: [
      { label: 'Roles', path: '/aws/iam' },
    ],
  },
  {
    id: 'emr',
    label: 'EMR',
    basePath: '/aws/emr',
    rootPath: '/aws/emr/clusters',
    children: [
      { label: 'Clusters', path: '/aws/emr/clusters' },
    ],
  },
  {
    id: 'emr-containers',
    label: 'EMR on EKS',
    basePath: '/aws/emr-containers',
    rootPath: '/aws/emr-containers/clusters',
    children: [
      { label: 'Virtual Clusters', path: '/aws/emr-containers/clusters' },
    ],
  },
  {
    id: 'glue',
    label: 'Glue',
    basePath: '/aws/glue',
    rootPath: '/aws/glue/databases',
    children: [
      { label: 'Databases', path: '/aws/glue/databases' },
      { label: 'Jobs', path: '/aws/glue/jobs' },
      { label: 'Crawlers', path: '/aws/glue/crawlers' },
    ],
  },
  {
    id: 'eventbridge',
    label: 'EventBridge',
    basePath: '/aws/eventbridge',
    rootPath: '/aws/eventbridge/rules',
    children: [
      { label: 'Rules', path: '/aws/eventbridge/rules' },
      { label: 'Event Buses', path: '/aws/eventbridge/buses' },
    ],
  },
  {
    id: 'apigateway',
    label: 'API Gateway',
    basePath: '/aws/apigateway',
    rootPath: '/aws/apigateway/apis',
    children: [
      { label: 'REST APIs', path: '/aws/apigateway/apis' },
    ],
  },
  {
    id: 'sfn',
    label: 'Step Functions',
    basePath: '/aws/sfn',
    rootPath: '/aws/sfn/state-machines',
    children: [
      { label: 'State Machines', path: '/aws/sfn/state-machines' },
    ],
  },
  {
    id: 'ec2',
    label: 'EC2',
    basePath: '/aws/ec2',
    rootPath: '/aws/ec2/instances',
    children: [
      { label: 'Instances', path: '/aws/ec2/instances' },
    ],
  },
  {
    id: 'ecs',
    label: 'ECS',
    basePath: '/aws/ecs',
    rootPath: '/aws/ecs/clusters',
    children: [
      { label: 'Clusters', path: '/aws/ecs/clusters' },
    ],
  },
  {
    id: 'eks',
    label: 'EKS',
    basePath: '/aws/eks',
    rootPath: '/aws/eks/clusters',
    children: [
      { label: 'Clusters', path: '/aws/eks/clusters' },
    ],
  },
  {
    id: 'rds',
    label: 'RDS',
    basePath: '/aws/rds',
    rootPath: '/aws/rds/instances',
    children: [
      { label: 'Instances', path: '/aws/rds/instances' },
    ],
  },
  {
    id: 'elasticache',
    label: 'ElastiCache',
    basePath: '/aws/elasticache',
    rootPath: '/aws/elasticache/clusters',
    children: [
      { label: 'Clusters', path: '/aws/elasticache/clusters' },
    ],
  },
  {
    id: 'route53',
    label: 'Route 53',
    basePath: '/aws/route53',
    rootPath: '/aws/route53/zones',
    children: [
      { label: 'Hosted Zones', path: '/aws/route53/zones' },
    ],
  },
  {
    id: 'cloudformation',
    label: 'CloudFormation',
    basePath: '/aws/cloudformation',
    rootPath: '/aws/cloudformation/stacks',
    children: [
      { label: 'Stacks', path: '/aws/cloudformation/stacks' },
    ],
  },
]

interface Props {
  open: boolean
}

export function Sidebar({ open }: Props) {
  const { pathname } = useLocation()

  return (
    <aside style={{
      ...sidebarStyle,
      width: open ? SIDEBAR_WIDTH : 0,
      minWidth: open ? SIDEBAR_WIDTH : 0,
      overflowY: open ? 'auto' : 'hidden',
      overflowX: 'hidden',
      position: 'relative',
      zIndex: 10,
    }}>
      <nav style={{ width: SIDEBAR_WIDTH, paddingTop: '0.5rem', display: 'flex', flexDirection: 'column', height: '100%' }}>
        <div style={{ flex: 1 }}>
          {navTree.map((section) => {
            const isActive = pathname.startsWith(section.basePath)
            return (
              <div key={section.id} style={{ marginBottom: '0.15rem' }}>
                {/* Section header — NavLink to the service root */}
                <NavLink
                  to={section.rootPath}
                  style={{
                    ...sectionHeaderStyle,
                    background: isActive ? 'rgba(232,118,0,0.12)' : 'none',
                    color: isActive ? '#e87600' : '#c9cdd4',
                    textDecoration: 'none',
                    display: 'flex',
                  }}
                >
                  <span style={{ fontSize: '0.6em', opacity: 0.7, width: 16, textAlign: 'center', flexShrink: 0, paddingTop: 1 }}>
                    {isActive ? '▼' : '▶'}
                  </span>
                  <span style={{ fontWeight: isActive ? 600 : 400, fontSize: '0.83em' }}>
                    {section.label}
                  </span>
                </NavLink>

                {/* Children — shown when this service is active */}
                {isActive && (
                  <div>
                    {section.children.map((child) => (
                      <NavLink
                        key={child.path}
                        to={child.path}
                        end
                        style={({ isActive: childActive }) => ({
                          ...childLinkStyle,
                          background: childActive ? 'rgba(9,114,211,0.12)' : 'none',
                          color: childActive ? '#0972d3' : '#8d9daa',
                          fontWeight: childActive ? 500 : 400,
                        })}
                      >
                        {child.label}
                      </NavLink>
                    ))}
                  </div>
                )}
              </div>
            )
          })}
        </div>

        {/* Admin link pinned at bottom */}
        <div style={{ borderTop: '1px solid #2d3748', padding: '0.5rem 0' }}>
          <NavLink
            to="/admin"
            style={({ isActive: adminActive }) => ({
              ...sectionHeaderStyle,
              display: 'flex',
              textDecoration: 'none',
              background: adminActive ? 'rgba(232,118,0,0.12)' : 'none',
              color: adminActive ? '#e87600' : '#8d9daa',
            })}
          >
            <span style={{ fontSize: '0.6em', opacity: 0.7, width: 16, textAlign: 'center', flexShrink: 0, paddingTop: 1 }}>⚙</span>
            <span style={{ fontWeight: pathname === '/admin' ? 600 : 400, fontSize: '0.83em' }}>Admin</span>
          </NavLink>
        </div>
      </nav>
    </aside>
  )
}

const SIDEBAR_WIDTH = 210

const sidebarStyle: React.CSSProperties = {
  background: '#1b2530',
  borderRight: '1px solid #2d3748',
  flexShrink: 0,
  transition: 'width 0.18s ease, min-width 0.18s ease',
}

const sectionHeaderStyle: React.CSSProperties = {
  alignItems: 'center',
  gap: '0.5rem',
  padding: '0.55rem 1rem',
  cursor: 'pointer',
  userSelect: 'none',
}

const childLinkStyle: React.CSSProperties = {
  display: 'block',
  padding: '0.4rem 1rem 0.4rem 2.25rem',
  textDecoration: 'none',
  fontSize: '0.82em',
  transition: 'background 0.1s',
}
