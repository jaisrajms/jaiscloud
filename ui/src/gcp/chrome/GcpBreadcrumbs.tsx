import { Breadcrumbs, Link, Typography } from '@mui/material'
import { Link as RouterLink, useLocation } from 'react-router-dom'
import type { ServiceDescriptor } from '../../api/services'
import { breadcrumbsFor } from './navModel'

/** Breadcrumb trail for the current GCP route (hidden on the console home). */
export function GcpBreadcrumbs({ services }: { services: ServiceDescriptor[] }) {
  const { pathname } = useLocation()
  const crumbs = breadcrumbsFor(services, pathname)
  if (crumbs.length <= 1) return null

  return (
    <Breadcrumbs aria-label="Breadcrumbs" sx={{ mb: 2 }}>
      {crumbs.map((crumb, index) =>
        crumb.to && index < crumbs.length - 1 ? (
          <Link
            key={crumb.label}
            component={RouterLink}
            to={crumb.to}
            underline="hover"
            color="inherit"
            variant="body2"
          >
            {crumb.label}
          </Link>
        ) : (
          <Typography key={crumb.label} variant="body2" color="text.primary">
            {crumb.label}
          </Typography>
        ),
      )}
    </Breadcrumbs>
  )
}
