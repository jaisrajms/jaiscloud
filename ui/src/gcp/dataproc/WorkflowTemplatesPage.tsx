import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, Stack } from '@mui/material'
import { Link as RouterLink } from 'react-router-dom'
import {
  listWorkflowTemplates,
  type DataprocWorkflowTemplate,
} from '../../api/gcp/dataproc'
import { useAccount } from '../../context/AccountContext'
import { shortDate } from './util'
import { GcpDataTable, type GcpColumn } from '../common/GcpDataTable'
import { GcpPageHeader } from '../common/GcpPageHeader'
import { GcpRowDetail } from '../common/GcpRowDetail'
import { GcpToolbar } from '../common/GcpToolbar'
import { filterRows } from '../common/pagination'

/** Dataproc workflow templates across every region. */
export function WorkflowTemplatesPage() {
  const { accountId } = useAccount()
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])

  const templates = useQuery({
    queryKey: ['gcp', 'dataproc', 'workflow-templates', accountId],
    queryFn: listWorkflowTemplates,
  })

  const rows = filterRows(templates.data?.templates ?? [], filter, (template) =>
    [template.id, template.region, String(template.version)].join(' '),
  )

  const columns: GcpColumn<DataprocWorkflowTemplate>[] = [
    {
      key: 'id',
      header: 'Template',
      sortable: true,
      sortValue: (template) => template.id,
      render: (template) => (
        <Link
          component={RouterLink}
          to={`/gcp/dataproc/workflow-templates/${encodeURIComponent(template.region)}/${encodeURIComponent(template.id)}`}
        >
          {template.id}
        </Link>
      ),
    },
    {
      key: 'region',
      header: 'Region',
      sortable: true,
      sortValue: (template) => template.region,
      render: (template) => template.region || '—',
    },
    {
      key: 'version',
      header: 'Version',
      sortable: true,
      sortValue: (template) => template.version,
      render: (template) => template.version || '—',
    },
    {
      key: 'created',
      header: 'Created',
      sortable: true,
      sortValue: (template) => template.createTime ?? '',
      render: (template) => shortDate(template.createTime),
    },
    {
      key: 'updated',
      header: 'Updated',
      sortable: true,
      sortValue: (template) => template.updateTime ?? '',
      render: (template) => shortDate(template.updateTime),
    },
  ]

  return (
    <Stack>
      <GcpPageHeader
        id="dataproc"
        title="Dataproc"
        subtitle={`Workflow templates · project ${accountId || '—'}`}
      />

      <GcpToolbar
        filter={filter}
        onFilterChange={setFilter}
        filterPlaceholder="Filter workflow templates"
        onRefresh={() => void templates.refetch()}
        refreshing={templates.isFetching}
      />

      <GcpDataTable
        aria-label="Dataproc workflow templates"
        columns={columns}
        rows={rows}
        getRowKey={(template) => `${template.region}/${template.id}`}
        loading={templates.isLoading}
        error={templates.isError ? 'Failed to load workflow templates.' : null}
        emptyMessage={
          filter ? 'No workflow templates match the filter.' : 'No workflow templates in this project.'
        }
        selectable
        selectedKeys={selected}
        onSelectionChange={setSelected}
        renderDetail={(template) => <GcpRowDetail row={template} />}
        detailTitle={(template) => template.id}
      />
    </Stack>
  )
}
