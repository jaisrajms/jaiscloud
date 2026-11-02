import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  Divider,
  IconButton,
  Stack,
  Typography,
} from '@mui/material'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'
import EditOutlinedIcon from '@mui/icons-material/EditOutlined'
import PlayArrowIcon from '@mui/icons-material/PlayArrow'
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom'
import { deleteWorkflowTemplate, getWorkflowTemplate } from '../../api/gcp/dataproc'
import { InstantiateTemplateDialog, WorkflowTemplateDialog } from './WorkflowTemplateDialog'
import { useAccount } from '../../context/AccountContext'
import { Detail, JsonBlock } from './common'
import { shortDate } from './util'
import { GcpPageTitle } from '../common/PageTitle'

/** A single Dataproc workflow template: metadata plus the full definition. */
export function WorkflowTemplateDetailPage() {
  const { region = '', template: templateID = '' } = useParams()
  const { accountId } = useAccount()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [editing, setEditing] = useState(false)
  const [running, setRunning] = useState(false)

  const detail = useQuery({
    queryKey: ['gcp', 'dataproc', 'workflow-template', region, templateID, accountId],
    queryFn: () => getWorkflowTemplate(region, templateID),
    enabled: Boolean(region && templateID),
  })

  const template = detail.data

  const remove = useMutation({
    mutationFn: () => deleteWorkflowTemplate(region, templateID, template?.version),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ['gcp', 'dataproc'] })
      navigate('/gcp/dataproc/workflow-templates')
    },
  })

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
        {template && (
          <>
            <Button startIcon={<EditOutlinedIcon />} onClick={() => setEditing(true)}>
              Edit
            </Button>
            <Button startIcon={<PlayArrowIcon />} onClick={() => setRunning(true)}>
              Run
            </Button>
            <Button
              color="error"
              startIcon={<DeleteOutlineIcon />}
              disabled={remove.isPending}
              onClick={() => remove.mutate()}
            >
              Delete
            </Button>
          </>
        )}
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the workflow template.</Alert>}
      {detail.isLoading && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
          <CircularProgress />
        </Box>
      )}

      {remove.isError && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {(remove.error as Error).message}
        </Alert>
      )}

      {template && (
        <>
          <WorkflowTemplateDialog
            open={editing}
            onClose={() => setEditing(false)}
            template={template}
          />
          <InstantiateTemplateDialog
            open={running}
            onClose={() => setRunning(false)}
            region={region}
            templateID={templateID}
            version={template.version}
          />
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
