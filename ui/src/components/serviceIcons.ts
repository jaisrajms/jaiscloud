import type { IconProps } from '@cloudscape-design/components'

type IconName = NonNullable<IconProps['name']>

/**
 * Per-service sidebar icons. Cloudscape ships generic UI glyphs rather than AWS
 * service logos, so these are the closest built-ins; swap for official AWS
 * pictograms later if desired. Unknown services fall back to a generic icon.
 */
const SERVICE_ICONS: Record<string, IconName> = {
  ec2: 'grid-view',
  lambda: 'script',
  ecs: 'multiscreen',
  eks: 'group',
  s3: 'folder',
  dynamodb: 'insert-row',
  rds: 'file',
  elasticache: 'refresh',
  apigateway: 'external',
  route53: 'globe',
  elbv2: 'resize-area',
  emr: 'convert-code',
  'emr-containers': 'group-active',
  glue: 'treeview-expand',
  kinesis: 'forward-10-seconds',
  firehose: 'upload-download',
  sqs: 'list-view',
  sns: 'notification',
  eventbridge: 'send',
  sfn: 'share',
  cloudwatch: 'status-info',
  logs: 'transcript',
  cloudformation: 'file-open',
  ssm: 'settings',
  iam: 'user-profile',
  kms: 'key',
  secretsmanager: 'lock-private',
  ses: 'envelope',
}

const FALLBACK_ICON: IconName = 'globe'

/** Icon name for a service descriptor id (falls back for unknown services). */
export function serviceIconName(id: string): IconName {
  return SERVICE_ICONS[id] ?? FALLBACK_ICON
}
