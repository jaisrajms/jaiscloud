import { useEffect, useMemo, useState } from 'react'
import {
  AppBar,
  Box,
  Button,
  CssBaseline,
  Drawer,
  IconButton,
  ThemeProvider,
  Toolbar,
  Typography,
  useMediaQuery,
} from '@mui/material'
import { useTheme } from '@mui/material/styles'
import MenuIcon from '@mui/icons-material/Menu'
import MenuOpenIcon from '@mui/icons-material/MenuOpen'
import KeyboardArrowDownIcon from '@mui/icons-material/KeyboardArrowDown'
import CloudQueueIcon from '@mui/icons-material/CloudQueue'
import { Link as RouterLink, Navigate, Route, Routes, useLocation } from 'react-router-dom'
import '@fontsource/roboto/400.css'
import '@fontsource/roboto/500.css'
import '@fontsource/roboto/700.css'
import { createGcpTheme } from './theme'
import { useGcpAppearance, type GcpAppearance } from './appearance'
import { GcpHome } from './GcpHome'
import { GcpAdminPage } from './admin/GcpAdminPage'
import { GcpGlobalSearch } from './chrome/GcpGlobalSearch'
import { ProjectPickerDialog } from './chrome/ProjectPicker'
import { AccountMenu } from './chrome/AccountMenu'
import { ChromeActions } from './chrome/ChromeActions'
import { GcpBreadcrumbs } from './chrome/GcpBreadcrumbs'
import { GcpNav } from './chrome/GcpNav'
import { pageTitleFor } from './chrome/navModel'
import { BucketsPage } from './storage/BucketsPage'
import { ObjectsPage } from './storage/ObjectsPage'
import { BucketSettingsPage } from './storage/BucketSettingsPage'
import { TopicsPage } from './pubsub/TopicsPage'
import { TopicDetailPage } from './pubsub/TopicDetailPage'
import { SubscriptionsPage } from './pubsub/SubscriptionsPage'
import { SubscriptionDetailPage } from './pubsub/SubscriptionDetailPage'
import { CollectionsPage } from './firestore/CollectionsPage'
import { DocumentsPage } from './firestore/DocumentsPage'
import { DocumentDetailPage } from './firestore/DocumentDetailPage'
import { QueryPage as FirestoreQueryPage } from './firestore/QueryPage'
import { IndexesPage as FirestoreIndexesPage } from './firestore/IndexesPage'
import { KindsPage as DatastoreKindsPage } from './datastore/KindsPage'
import { EntitiesPage as DatastoreEntitiesPage } from './datastore/EntitiesPage'
import { EntityDetailPage as DatastoreEntityDetailPage } from './datastore/EntityDetailPage'
import { QueryPage as DatastoreQueryPage } from './datastore/QueryPage'
import { InstancesPage } from './compute/InstancesPage'
import { InstanceDetailPage } from './compute/InstanceDetailPage'
import { ServicesPage as RunServicesPage } from './run/ServicesPage'
import { ServiceDetailPage as RunServiceDetailPage } from './run/ServiceDetailPage'
import { RevisionDetailPage as RunRevisionDetailPage } from './run/RevisionDetailPage'
import { JobsPage as SchedulerJobsPage } from './scheduler/JobsPage'
import { JobDetailPage as SchedulerJobDetailPage } from './scheduler/JobDetailPage'
import { QueuesPage as TasksQueuesPage } from './tasks/QueuesPage'
import { QueueDetailPage as TasksQueueDetailPage } from './tasks/QueueDetailPage'
import { TriggersPage as EventarcTriggersPage } from './eventarc/TriggersPage'
import { TriggerDetailPage as EventarcTriggerDetailPage } from './eventarc/TriggerDetailPage'
import { ChannelsPage as EventarcChannelsPage } from './eventarc/ChannelsPage'
import { ChannelDetailPage as EventarcChannelDetailPage } from './eventarc/ChannelDetailPage'
import { FunctionsPage } from './functions/FunctionsPage'
import { FunctionDetailPage } from './functions/FunctionDetailPage'
import { WorkflowsPage } from './workflows/WorkflowsPage'
import { WorkflowDetailPage } from './workflows/WorkflowDetailPage'
import { ExecutionDetailPage as WorkflowExecutionDetailPage } from './workflows/ExecutionDetailPage'
import { DatasetsPage } from './bigquery/DatasetsPage'
import { DatasetDetailPage } from './bigquery/DatasetDetailPage'
import { TableDetailPage } from './bigquery/TableDetailPage'
import { JobsPage } from './bigquery/JobsPage'
import { JobDetailPage } from './bigquery/JobDetailPage'
import { QueryPage as BigQueryQueryPage } from './bigquery/QueryPage'
import { ClustersPage as DataprocClustersPage } from './dataproc/ClustersPage'
import { ClusterDetailPage as DataprocClusterDetailPage } from './dataproc/ClusterDetailPage'
import { JobsPage as DataprocJobsPage } from './dataproc/JobsPage'
import { JobDetailPage as DataprocJobDetailPage } from './dataproc/JobDetailPage'
import { WorkflowTemplatesPage as DataprocWorkflowTemplatesPage } from './dataproc/WorkflowTemplatesPage'
import { WorkflowTemplateDetailPage as DataprocWorkflowTemplateDetailPage } from './dataproc/WorkflowTemplateDetailPage'
import { ClustersPage as ManagedKafkaClustersPage } from './managedkafka/ClustersPage'
import { ClusterDetailPage as ManagedKafkaClusterDetailPage } from './managedkafka/ClusterDetailPage'
import { TopicsPage as ManagedKafkaTopicsPage } from './managedkafka/TopicsPage'
import { TopicDetailPage as ManagedKafkaTopicDetailPage } from './managedkafka/TopicDetailPage'
import { ServiceAccountsPage } from './iam/ServiceAccountsPage'
import { ServiceAccountDetailPage } from './iam/ServiceAccountDetailPage'
import { KeyRingsPage } from './kms/KeyRingsPage'
import { KeyRingDetailPage } from './kms/KeyRingDetailPage'
import { CryptoKeyDetailPage } from './kms/CryptoKeyDetailPage'
import { SecretsPage } from './secretmanager/SecretsPage'
import { SecretDetailPage } from './secretmanager/SecretDetailPage'
import { LogsExplorer } from './logging/LogsExplorer'
import { MetricsPage as LoggingMetricsPage } from './logging/MetricsPage'
import { SinksPage } from './logging/SinksPage'
import { ExclusionsPage } from './logging/ExclusionsPage'
import { MetricsExplorer } from './monitoring/MetricsExplorer'
import { AlertingPage } from './monitoring/AlertingPage'
import { ChannelsPage } from './monitoring/ChannelsPage'
import { ProjectsPage as ResourceManagerProjectsPage } from './resourcemanager/ProjectsPage'
import { AccountProvider, useAccount } from '../context/AccountContext'
import { useEventStream } from '../hooks/useEventStream'
import { useMeta } from '../hooks/useMeta'
import { useServices } from '../hooks/useServices'
import { GcpSnackbarProvider } from './common/SnackbarProvider'

const DRAWER_WIDTH = 256
/** Icons-only rail width; the desktop default. */
const MINI_WIDTH = 72
const OVERLAY_WIDTH = 360

/** Persisted choice: whether the desktop rail is expanded (labelled) or mini. */
const NAV_EXPANDED_KEY = 'jaiscloud-nav-expanded'

function readRailExpanded(): boolean {
  try {
    // Default to expanded so labels are visible; mini is an opt-in collapse.
    return window.localStorage.getItem(NAV_EXPANDED_KEY) !== 'false'
  } catch {
    return true
  }
}

/** Material shell approximating the Google Cloud Console chrome. */
function GcpShell({ appearance }: { appearance: GcpAppearance }) {
  const theme = useTheme()
  const isDesktop = useMediaQuery(theme.breakpoints.up('md'))
  const location = useLocation()
  const { data: meta } = useMeta()
  const { accountId } = useAccount()
  const { data: servicesData } = useServices()
  const services = useMemo(() => servicesData?.services ?? [], [servicesData])
  const { connected } = useEventStream()

  // One nav, two surfaces: on desktop the hamburger toggles the rail between
  // mini (icons) and expanded (labelled, persisted); on mobile it opens the
  // overlay. The rail is always present on desktop, never hidden.
  const [railExpanded, setRailExpandedState] = useState<boolean>(readRailExpanded)
  const [overlayOpen, setOverlayOpen] = useState(false)
  const [projectOpen, setProjectOpen] = useState(false)

  const navExpanded = isDesktop ? railExpanded : overlayOpen

  const toggleNav = () => {
    if (!isDesktop) {
      setOverlayOpen(true)
      return
    }
    setRailExpandedState((value) => {
      const next = !value
      try {
        window.localStorage.setItem(NAV_EXPANDED_KEY, String(next))
      } catch {
        /* ignore storage errors */
      }
      return next
    })
  }

  const closeOverlay = () => setOverlayOpen(false)

  useEffect(() => {
    document.title = pageTitleFor(services, location.pathname)
  }, [services, location.pathname])

  return (
    <Box sx={{ display: 'flex' }}>
      <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
        <Toolbar sx={{ gap: 1 }}>
          <IconButton
            edge="start"
            color="inherit"
            aria-label={navExpanded ? 'Collapse navigation menu' : 'Expand navigation menu'}
            aria-expanded={navExpanded}
            aria-controls={isDesktop ? 'gcp-nav-rail' : 'gcp-nav-overlay'}
            onClick={toggleNav}
            sx={{ color: 'text.primary' }}
          >
            {/* MenuOpen when the nav is showing (desktop expanded rail or open
                mobile overlay), plain Menu when it is collapsed/closed. */}
            {navExpanded ? <MenuOpenIcon /> : <MenuIcon />}
          </IconButton>
          <Box
            component={RouterLink}
            to="/gcp"
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 1,
              mr: 1,
              minWidth: 0,
              textDecoration: 'none',
              color: 'text.primary',
            }}
          >
            <CloudQueueIcon sx={{ color: 'primary.main' }} />
            <Box sx={{ minWidth: 0, display: { xs: 'none', sm: 'block' } }}>
              <Typography variant="subtitle1" noWrap sx={{ fontWeight: 500, lineHeight: 1.1 }}>
                JaisCloud
              </Typography>
              <Typography variant="caption" color="text.secondary" noWrap>
                Cloud console
              </Typography>
            </Box>
          </Box>
          <Box sx={{ flexGrow: 1, display: 'flex', justifyContent: 'center', minWidth: 0 }}>
            <GcpGlobalSearch />
          </Box>
          <Button
            onClick={() => setProjectOpen(true)}
            endIcon={<KeyboardArrowDownIcon />}
            sx={{
              color: 'text.primary',
              textTransform: 'none',
              minWidth: 0,
              maxWidth: { xs: 140, sm: 260 },
              flexShrink: 1,
              display: { xs: 'none', sm: 'inline-flex' },
            }}
          >
            <Typography variant="body2" noWrap component="span">
              {accountId || meta?.accountId || 'Select a project'}
            </Typography>
          </Button>
          <ChromeActions connected={connected} appearance={appearance} />
          <AccountMenu onSelectProject={() => setProjectOpen(true)} />
        </Toolbar>
      </AppBar>

      <Box
        component="nav"
        id="gcp-nav-rail"
        aria-label="Service navigation"
        sx={{
          display: { xs: 'none', md: 'block' },
          width: railExpanded ? DRAWER_WIDTH : MINI_WIDTH,
          flexShrink: 0,
          overflow: 'hidden',
          transition: (theme) =>
            theme.transitions.create('width', {
              easing: theme.transitions.easing.sharp,
              duration: theme.transitions.duration.enteringScreen,
            }),
        }}
      >
        <Drawer
          variant="permanent"
          open
          sx={{
            '& .MuiDrawer-paper': {
              width: railExpanded ? DRAWER_WIDTH : MINI_WIDTH,
              boxSizing: 'border-box',
              overflowX: 'hidden',
              borderRightWidth: 1,
              transition: (theme) =>
                theme.transitions.create('width', {
                  easing: theme.transitions.easing.sharp,
                  duration: theme.transitions.duration.enteringScreen,
                }),
            },
          }}
        >
          <GcpNav
            services={services}
            title="Console"
            variant={railExpanded ? 'full' : 'mini'}
          />
        </Drawer>
      </Box>

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          minWidth: 0,
          p: 3,
          bgcolor: 'background.default',
          minHeight: '100vh',
        }}
      >
        <Toolbar />
        <GcpBreadcrumbs services={services} />
        <Routes>
          <Route path="/gcp" element={<GcpHome />} />
          <Route path="/gcp/storage/buckets" element={<BucketsPage />} />
          <Route path="/gcp/storage/buckets/:bucket" element={<ObjectsPage />} />
          <Route path="/gcp/storage/buckets/:bucket/settings" element={<BucketSettingsPage />} />
          <Route path="/gcp/pubsub/topics" element={<TopicsPage />} />
          <Route path="/gcp/pubsub/topics/:topic" element={<TopicDetailPage />} />
          <Route path="/gcp/pubsub/subscriptions" element={<SubscriptionsPage />} />
          <Route path="/gcp/pubsub/subscriptions/:subscription" element={<SubscriptionDetailPage />} />
          <Route path="/gcp/firestore/collections" element={<CollectionsPage />} />
          <Route path="/gcp/firestore/query" element={<FirestoreQueryPage />} />
          <Route path="/gcp/firestore/indexes" element={<FirestoreIndexesPage />} />
          <Route path="/gcp/firestore/collections/:collection" element={<DocumentsPage />} />
          <Route
            path="/gcp/firestore/collections/:collection/documents/:document"
            element={<DocumentDetailPage />}
          />
          <Route path="/gcp/datastore/kinds" element={<DatastoreKindsPage />} />
          <Route path="/gcp/datastore/kinds/:kind" element={<DatastoreEntitiesPage />} />
          <Route path="/gcp/datastore/entity" element={<DatastoreEntityDetailPage />} />
          <Route path="/gcp/datastore/query" element={<DatastoreQueryPage />} />
          <Route path="/gcp/compute/instances" element={<InstancesPage />} />
          <Route path="/gcp/compute/instances/:zone/:instance" element={<InstanceDetailPage />} />
          <Route path="/gcp/run/services" element={<RunServicesPage />} />
          <Route path="/gcp/run/services/:region/:service" element={<RunServiceDetailPage />} />
          <Route
            path="/gcp/run/services/:region/:service/revisions/:revision"
            element={<RunRevisionDetailPage />}
          />
          <Route path="/gcp/scheduler/jobs" element={<SchedulerJobsPage />} />
          <Route
            path="/gcp/scheduler/jobs/:location/:job"
            element={<SchedulerJobDetailPage />}
          />
          <Route path="/gcp/tasks/queues" element={<TasksQueuesPage />} />
          <Route
            path="/gcp/tasks/queues/:location/:queue"
            element={<TasksQueueDetailPage />}
          />
          <Route path="/gcp/eventarc/triggers" element={<EventarcTriggersPage />} />
          <Route
            path="/gcp/eventarc/triggers/:location/:trigger"
            element={<EventarcTriggerDetailPage />}
          />
          <Route path="/gcp/eventarc/channels" element={<EventarcChannelsPage />} />
          <Route
            path="/gcp/eventarc/channels/:location/:channel"
            element={<EventarcChannelDetailPage />}
          />
          <Route path="/gcp/functions" element={<FunctionsPage />} />
          <Route path="/gcp/functions/:location/:function" element={<FunctionDetailPage />} />
          <Route path="/gcp/workflows" element={<WorkflowsPage />} />
          <Route path="/gcp/workflows/:location/:workflow" element={<WorkflowDetailPage />} />
          <Route
            path="/gcp/workflows/:location/:workflow/executions/:execution"
            element={<WorkflowExecutionDetailPage />}
          />
          <Route path="/gcp/bigquery/datasets" element={<DatasetsPage />} />
          <Route path="/gcp/bigquery/datasets/:dataset" element={<DatasetDetailPage />} />
          <Route
            path="/gcp/bigquery/datasets/:dataset/tables/:table"
            element={<TableDetailPage />}
          />
          <Route path="/gcp/bigquery/jobs" element={<JobsPage />} />
          <Route path="/gcp/bigquery/jobs/:job" element={<JobDetailPage />} />
          <Route path="/gcp/bigquery/query" element={<BigQueryQueryPage />} />
          <Route path="/gcp/dataproc/clusters" element={<DataprocClustersPage />} />
          <Route
            path="/gcp/dataproc/clusters/:region/:cluster"
            element={<DataprocClusterDetailPage />}
          />
          <Route path="/gcp/dataproc/jobs" element={<DataprocJobsPage />} />
          <Route path="/gcp/dataproc/jobs/:region/:job" element={<DataprocJobDetailPage />} />
          <Route path="/gcp/dataproc/workflow-templates" element={<DataprocWorkflowTemplatesPage />} />
          <Route
            path="/gcp/dataproc/workflow-templates/:region/:template"
            element={<DataprocWorkflowTemplateDetailPage />}
          />
          <Route path="/gcp/managedkafka/clusters" element={<ManagedKafkaClustersPage />} />
          <Route
            path="/gcp/managedkafka/clusters/:location/:cluster"
            element={<ManagedKafkaClusterDetailPage />}
          />
          <Route path="/gcp/managedkafka/topics" element={<ManagedKafkaTopicsPage />} />
          <Route
            path="/gcp/managedkafka/clusters/:location/:cluster/topics/:topic"
            element={<ManagedKafkaTopicDetailPage />}
          />
          <Route path="/gcp/iam/service-accounts" element={<ServiceAccountsPage />} />
          <Route path="/gcp/iam/service-accounts/:email" element={<ServiceAccountDetailPage />} />
          <Route path="/gcp/kms/keyrings" element={<KeyRingsPage />} />
          <Route path="/gcp/kms/keyrings/:location/:keyRing" element={<KeyRingDetailPage />} />
          <Route
            path="/gcp/kms/keyrings/:location/:keyRing/keys/:key"
            element={<CryptoKeyDetailPage />}
          />
          <Route path="/gcp/secretmanager/secrets" element={<SecretsPage />} />
          <Route path="/gcp/secretmanager/secrets/:secret" element={<SecretDetailPage />} />
          <Route path="/gcp/logging/entries" element={<LogsExplorer />} />
          <Route path="/gcp/logging/metrics" element={<LoggingMetricsPage />} />
          <Route path="/gcp/logging/sinks" element={<SinksPage />} />
          <Route path="/gcp/logging/exclusions" element={<ExclusionsPage />} />
          <Route path="/gcp/monitoring/metrics" element={<MetricsExplorer />} />
          <Route path="/gcp/monitoring/alerting" element={<AlertingPage />} />
          <Route path="/gcp/monitoring/channels" element={<ChannelsPage />} />
          <Route path="/gcp/resourcemanager/projects" element={<ResourceManagerProjectsPage />} />
          <Route path="/gcp/admin" element={<GcpAdminPage />} />
          <Route path="*" element={<Navigate to="/gcp" replace />} />
        </Routes>
      </Box>

      {/*
        Mobile overlay: the same GcpNav in a temporary Drawer. It is only
        mounted below the md breakpoint so desktop shows exactly one surface
        (the rail); a tap navigates and closes it.
      */}
      {!isDesktop && (
        <Drawer
          id="gcp-nav-overlay"
          anchor="left"
          open={overlayOpen}
          onClose={closeOverlay}
          sx={{
            '& .MuiDrawer-paper': {
              width: OVERLAY_WIDTH,
              maxWidth: '100vw',
              boxSizing: 'border-box',
            },
          }}
        >
          <GcpNav
            services={services}
            open={overlayOpen}
            showSearch
            onClose={closeOverlay}
            title="Navigation menu"
          />
        </Drawer>
      )}
      <ProjectPickerDialog open={projectOpen} onClose={() => setProjectOpen(false)} />
    </Box>
  )
}

/** GCP console: Material Design, rendered only when meta.cloud === 'gcp'. */
export function GcpApp() {
  const appearance = useGcpAppearance()
  const theme = useMemo(
    () => createGcpTheme(appearance.mode, appearance.density),
    [appearance.mode, appearance.density],
  )

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <GcpSnackbarProvider>
        <AccountProvider>
          <GcpShell appearance={appearance} />
        </AccountProvider>
      </GcpSnackbarProvider>
    </ThemeProvider>
  )
}
