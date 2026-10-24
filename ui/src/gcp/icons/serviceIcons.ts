import type { ElementType } from 'react'
import StorageOutlinedIcon from '@mui/icons-material/StorageOutlined'
import CampaignOutlinedIcon from '@mui/icons-material/CampaignOutlined'
import LocalFireDepartmentOutlinedIcon from '@mui/icons-material/LocalFireDepartmentOutlined'
import DatasetOutlinedIcon from '@mui/icons-material/DatasetOutlined'
import ComputerOutlinedIcon from '@mui/icons-material/ComputerOutlined'
import RocketLaunchOutlinedIcon from '@mui/icons-material/RocketLaunchOutlined'
import BoltOutlinedIcon from '@mui/icons-material/BoltOutlined'
import ScheduleOutlinedIcon from '@mui/icons-material/ScheduleOutlined'
import ChecklistOutlinedIcon from '@mui/icons-material/ChecklistOutlined'
import AccountTreeOutlinedIcon from '@mui/icons-material/AccountTreeOutlined'
import ApartmentOutlinedIcon from '@mui/icons-material/ApartmentOutlined'
import DeviceHubOutlinedIcon from '@mui/icons-material/DeviceHubOutlined'
import AnalyticsOutlinedIcon from '@mui/icons-material/AnalyticsOutlined'
import HubOutlinedIcon from '@mui/icons-material/HubOutlined'
import StreamOutlinedIcon from '@mui/icons-material/StreamOutlined'
import AdminPanelSettingsOutlinedIcon from '@mui/icons-material/AdminPanelSettingsOutlined'
import VpnKeyOutlinedIcon from '@mui/icons-material/VpnKeyOutlined'
import LockOutlinedIcon from '@mui/icons-material/LockOutlined'
import ReceiptLongOutlinedIcon from '@mui/icons-material/ReceiptLongOutlined'
import MonitorHeartOutlinedIcon from '@mui/icons-material/MonitorHeartOutlined'
import SettingsOutlinedIcon from '@mui/icons-material/SettingsOutlined'
import CloudOutlinedIcon from '@mui/icons-material/CloudOutlined'

/** An MUI icon component that accepts the standard SvgIcon props. */
export type GcpIconComponent = ElementType

/**
 * Per-service product glyphs for the GCP console. These are Apache-2.0
 * Material Icons (bundled with `@mui/icons-material`, no CDN/runtime fetch),
 * not Google Cloud's brand pictograms — the closest distinct glyph per service.
 * Unknown services fall back to a generic cloud.
 */
export const SERVICE_ICONS: Record<string, GcpIconComponent> = {
  storage: StorageOutlinedIcon,
  pubsub: CampaignOutlinedIcon,
  firestore: LocalFireDepartmentOutlinedIcon,
  datastore: DatasetOutlinedIcon,
  compute: ComputerOutlinedIcon,
  run: RocketLaunchOutlinedIcon,
  functions: BoltOutlinedIcon,
  scheduler: ScheduleOutlinedIcon,
  tasks: ChecklistOutlinedIcon,
  workflows: AccountTreeOutlinedIcon,
  eventarc: DeviceHubOutlinedIcon,
  bigquery: AnalyticsOutlinedIcon,
  dataproc: HubOutlinedIcon,
  managedkafka: StreamOutlinedIcon,
  iam: AdminPanelSettingsOutlinedIcon,
  kms: VpnKeyOutlinedIcon,
  secretmanager: LockOutlinedIcon,
  logging: ReceiptLongOutlinedIcon,
  monitoring: MonitorHeartOutlinedIcon,
  resourcemanager: ApartmentOutlinedIcon,
  admin: SettingsOutlinedIcon,
}

/** Google-palette accent per service, used for card badges and the page header. */
export const SERVICE_ACCENTS: Record<string, string> = {
  storage: '#1a73e8',
  pubsub: '#1a73e8',
  firestore: '#f9ab00',
  datastore: '#1a73e8',
  compute: '#1a73e8',
  run: '#1a73e8',
  functions: '#1a73e8',
  scheduler: '#1a73e8',
  tasks: '#1a73e8',
  workflows: '#34a853',
  eventarc: '#ea4335',
  bigquery: '#4285f4',
  dataproc: '#4285f4',
  managedkafka: '#4285f4',
  iam: '#ea4335',
  kms: '#34a853',
  secretmanager: '#ea4335',
  logging: '#34a853',
  monitoring: '#4285f4',
  resourcemanager: '#4285f4',
  admin: '#5f6368',
}

const FALLBACK_ICON: GcpIconComponent = CloudOutlinedIcon
const FALLBACK_ACCENT = '#5f6368'

/** Icon component for a service descriptor id (falls back for unknown ids). */
export function serviceIconComponent(id: string): GcpIconComponent {
  return SERVICE_ICONS[id] ?? FALLBACK_ICON
}

/** True when the id has a dedicated product glyph (i.e. not the fallback). */
export function hasServiceIcon(id: string): boolean {
  return id in SERVICE_ICONS
}

/** Accent color for a service descriptor id (falls back for unknown ids). */
export function serviceAccent(id: string): string {
  return SERVICE_ACCENTS[id] ?? FALLBACK_ACCENT
}
