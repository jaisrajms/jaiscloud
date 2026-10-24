import { useQuery } from '@tanstack/react-query'
import {
  Alert,
  Box,
  CircularProgress,
  Divider,
  IconButton,
  Stack,
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import { Link as RouterLink, useParams } from 'react-router-dom'
import { getWorkflowTemplate } from '../../api/gcp/dataproc'
import { useAccount } from '../../context/AccountContext'
import { Detail, JsonBlock } from './common'
import { shortDate } from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** A single Dataproc workflow template: metadata plus the full definition. */
export function WorkflowTemplateDetailPage() {
  const { region = '', template: templateID = '' } = useParams()
  const { accountId } = useAccount()

  const detail = useQuery({
    queryKey: ['gcp', 'dataproc', 'workflow-template', region, templateID, accountId],
    queryFn: () => getWorkflowTemplate(region, templateID),
    enabled: Boolean(region && templateID),
  })

  const template = detail.data

  return (
    <Box>
      <Stack direction="row" spacing={1} sx={{ alignItems: 'center', mb: 2 }}>
        <IconButton
          component={RouterLink}
          to="/gcp/dataproc/workflow-templates"
          aria-label="Back to workflow templates"
        >
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ minWidth: 0 }}>
          <GcpPageTitle id="dataproc">{templateID}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Workflow template · {region || '—'}
          </Typography>
        </Box>
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the workflow template.</Alert>}
      {detail.isLoading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
          <CircularProgress />
        </Box>
      )}

      {template && (
        <>
          <Box
            sx={{
              display: 'grid',
              gridTemplateColumns: { xs: '1fr', sm: 'repeat(2, 1fr)', md: 'repeat(4, 1fr)' },
              gap: 2,
              mb: 3,
            }}
          >
            <Detail label="Region">{template.region}</Detail>
            <Detail label="Version">{String(template.version)}</Detail>
            <Detail label="Created">{shortDate(template.createTime)}</Detail>
            <Detail label="Updated">{shortDate(template.updateTime)}</Detail>
          </Box>

          <Divider sx={{ mb: 2 }} />
          <Typography variant="subtitle1" sx={{ mb: 1 }}>
            Definition
          </Typography>
          <JsonBlock value={template.definition} />
        </>
      )}
    </Box>
  )
}
