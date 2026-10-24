export interface CloudDocsLink {
  href: string
  label: string
}

const AWS_DOCS: CloudDocsLink = { href: 'https://docs.aws.amazon.com/', label: 'AWS documentation' }

const DOCS_BY_CLOUD: Record<string, CloudDocsLink> = {
  aws: AWS_DOCS,
  gcp: { href: 'https://cloud.google.com/docs', label: 'Google Cloud documentation' },
  azure: { href: 'https://learn.microsoft.com/azure/', label: 'Azure documentation' },
}

/** Documentation link for the active cloud (unknown clouds fall back to AWS). */
export function docsLink(cloud: string | undefined): CloudDocsLink {
  return DOCS_BY_CLOUD[cloud ?? ''] ?? AWS_DOCS
}
