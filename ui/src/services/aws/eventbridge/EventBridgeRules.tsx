import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Box,
  Button,
  ButtonDropdown,
  Container,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Modal,
  Select,
  SpaceBetween,
  StatusIndicator,
  Table,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import type { TableProps } from '@cloudscape-design/components'
import {
  listRules,
  putRule,
  deleteRule,
  enableRule,
  disableRule,
  listTargets,
  putTargets,
  removeTarget,
  type Rule,
  type Target,
} from '../../../api/eventbridge'
import { resourceStatus } from '../../../lib/status'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

const EMPTY_FORM = {
  name: '',
  eventPattern: '',
  scheduleExpression: '',
  state: 'ENABLED',
  description: '',
}
const EMPTY_TARGET_FORM = { id: '', arn: '' }

export function EventBridgeRules() {
  const qc = useQueryClient()
  const { notify } = useNotifications()
  const [selectedRule, setSelectedRule] = useState<Rule | null>(null)
  const [createOpen, setCreateOpen] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<Rule | null>(null)
  const [addTargetOpen, setAddTargetOpen] = useState(false)
  const [form, setForm] = useState(EMPTY_FORM)
  const [targetForm, setTargetForm] = useState(EMPTY_TARGET_FORM)

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['eventbridge', 'rules'],
    queryFn: () => listRules(),
  })

  const { data: targetsData } = useQuery({
    queryKey: ['eventbridge', 'targets', selectedRule?.name],
    queryFn: () => listTargets(selectedRule!.name),
    enabled: !!selectedRule,
  })

  const createMut = useMutation({
    mutationFn: () =>
      putRule({
        name: form.name,
        eventPattern: form.eventPattern || undefined,
        scheduleExpression: form.scheduleExpression || undefined,
        state: form.state,
        description: form.description || undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['eventbridge', 'rules'] })
      notify({ type: 'success', header: 'Rule created', content: form.name })
      setCreateOpen(false)
      setForm(EMPTY_FORM)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Create failed', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteRule(name),
    onSuccess: (_result, name) => {
      void qc.invalidateQueries({ queryKey: ['eventbridge', 'rules'] })
      notify({ type: 'success', header: 'Rule deleted', content: name })
      if (deleteTarget?.name === selectedRule?.name) setSelectedRule(null)
      setDeleteTarget(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const toggleMut = useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      enabled ? disableRule(name) : enableRule(name),
    onSuccess: (_result, { name, enabled }) => {
      void qc.invalidateQueries({ queryKey: ['eventbridge', 'rules'] })
      notify({ type: 'success', header: `${enabled ? 'Disabled' : 'Enabled'} rule`, content: name })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Update failed', content: (err as Error).message }),
  })

  const addTargetMut = useMutation({
    mutationFn: () =>
      putTargets(selectedRule!.name, [{ id: targetForm.id, arn: targetForm.arn }]),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['eventbridge', 'targets', selectedRule?.name] })
      notify({ type: 'success', header: 'Target added', content: targetForm.id })
      setAddTargetOpen(false)
      setTargetForm(EMPTY_TARGET_FORM)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Failed to add target', content: (err as Error).message }),
  })

  const removeTargetMut = useMutation({
    mutationFn: ({ rule, id }: { rule: string; id: string }) => removeTarget(rule, id),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['eventbridge', 'targets', selectedRule?.name] })
      notify({ type: 'success', header: 'Target removed' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Failed to remove target', content: (err as Error).message }),
  })

  const rules = data?.items ?? []
  const targets: Target[] = targetsData?.items ?? []

  const columns: ResourceColumn<Rule>[] = [
    {
      id: 'name',
      header: 'Name',
      filterLabel: 'Name',
      filterValue: (r) => r.name,
      cell: (r) => <Box fontWeight="bold">{r.name}</Box>,
    },
    {
      id: 'state',
      header: 'State',
      filterLabel: 'State',
      filterValue: (r) => r.state ?? '',
      cell: (r) => <StatusIndicator type={resourceStatus(r.state)}>{r.state || '—'}</StatusIndicator>,
    },
    {
      id: 'pattern',
      header: 'Pattern / schedule',
      cell: (r) => (
        <Box variant="code">{r.scheduleExpression || r.eventPattern || '—'}</Box>
      ),
    },
  ]

  const targetColumns: TableProps.ColumnDefinition<Target>[] = [
    { id: 'id', header: 'ID', cell: (t) => t.id },
    { id: 'arn', header: 'ARN', cell: (t) => <Box variant="code">{t.arn}</Box> },
    {
      id: 'actions',
      header: '',
      cell: (t) => (
        <Button
          onClick={() =>
            selectedRule && removeTargetMut.mutate({ rule: selectedRule.name, id: t.id })
          }
        >
          Remove
        </Button>
      ),
    },
  ]

  return (
    <ContentLayout header={<Header variant="h1">EventBridge rules</Header>}>
      <SpaceBetween size="l">
        {error ? (
          <ErrorState header="Failed to load rules" message={(error as Error).message} onRetry={() => void refetch()} />
        ) : (
          <ResourceTable
            favoriteService="eventbridge"
            favorite={(r) => ({ id: r.name, label: r.name, href: '/aws/eventbridge/rules', type: 'rule' })}
            items={rules}
            columns={columns}
            trackBy={(r) => r.name}
            title="Rules"
            loading={isLoading}
            onRowClick={(r) => setSelectedRule(r)}
            selectionType="single"
            selectedItems={selectedRule ? [selectedRule] : []}
            onSelectionChange={(items) => setSelectedRule(items[0] ?? null)}
            actions={
              <SpaceBetween direction="horizontal" size="xs">
                <ButtonDropdown
                  items={[
                    {
                      id: 'enable',
                      text: 'Enable',
                      disabled: !selectedRule || selectedRule.state === 'ENABLED',
                    },
                    {
                      id: 'disable',
                      text: 'Disable',
                      disabled: !selectedRule || selectedRule.state !== 'ENABLED',
                    },
                    { id: 'delete', text: 'Delete', disabled: !selectedRule },
                  ]}
                  onItemClick={({ detail }) => {
                    if (!selectedRule) return
                    if (detail.id === 'enable')
                      toggleMut.mutate({ name: selectedRule.name, enabled: false })
                    else if (detail.id === 'disable')
                      toggleMut.mutate({ name: selectedRule.name, enabled: true })
                    else if (detail.id === 'delete') setDeleteTarget(selectedRule)
                  }}
                  disabled={!selectedRule}
                >
                  Actions
                </ButtonDropdown>
                <Button variant="primary" onClick={() => setCreateOpen(true)}>
                  Create rule
                </Button>
              </SpaceBetween>
            }
            emptyTitle="No rules"
            emptyBody="Create a rule to route events to targets."
          />
        )}

        {selectedRule && (
          <Container
            header={
              <Header
                variant="h2"
                counter={`(${targets.length})`}
                actions={
                  <SpaceBetween direction="horizontal" size="xs">
                    <Button
                      iconName="close"
                      variant="icon"
                      ariaLabel="Close targets"
                      onClick={() => setSelectedRule(null)}
                    />
                    <Button variant="primary" onClick={() => setAddTargetOpen(true)}>
                      Add target
                    </Button>
                  </SpaceBetween>
                }
              >
                Targets for {selectedRule.name}
              </Header>
            }
          >
            <Table
              variant="embedded"
              items={targets}
              trackBy={(t) => t.id}
              columnDefinitions={targetColumns}
              empty={
                <Box textAlign="center" color="inherit">
                  <b>No targets</b>
                  <Box variant="p" color="inherit">
                    Add a target to route matching events.
                  </Box>
                </Box>
              }
            />
          </Container>
        )}
      </SpaceBetween>

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create rule"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!form.name.trim()}
                onClick={() => createMut.mutate()}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <FormField label="Name" constraintText="Required">
              <Input
                value={form.name}
                placeholder="my-rule"
                onChange={({ detail }) => setForm({ ...form, name: detail.value })}
              />
            </FormField>
            <FormField label="Event pattern (JSON)">
              <Input
                value={form.eventPattern}
                placeholder={'{"source":["aws.ec2"]}'}
                onChange={({ detail }) => setForm({ ...form, eventPattern: detail.value })}
              />
            </FormField>
            <FormField label="Schedule expression">
              <Input
                value={form.scheduleExpression}
                placeholder="rate(5 minutes)"
                onChange={({ detail }) => setForm({ ...form, scheduleExpression: detail.value })}
              />
            </FormField>
            <FormField label="Description">
              <Input
                value={form.description}
                onChange={({ detail }) => setForm({ ...form, description: detail.value })}
              />
            </FormField>
            <FormField label="State">
              <Select
                selectedOption={{ value: form.state, label: form.state }}
                onChange={({ detail }) =>
                  setForm({ ...form, state: detail.selectedOption.value ?? 'ENABLED' })
                }
                options={[
                  { value: 'ENABLED', label: 'ENABLED' },
                  { value: 'DISABLED', label: 'DISABLED' },
                ]}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={!!deleteTarget}
        onDismiss={() => setDeleteTarget(null)}
        header="Delete rule"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setDeleteTarget(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => deleteTarget && deleteMut.mutate(deleteTarget.name)}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Delete rule <b>{deleteTarget?.name}</b>? This action cannot be undone.
      </Modal>

      <Modal
        visible={addTargetOpen && !!selectedRule}
        onDismiss={() => setAddTargetOpen(false)}
        header={selectedRule ? `Add target to ${selectedRule.name}` : 'Add target'}
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setAddTargetOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={addTargetMut.isPending}
                disabled={!targetForm.id.trim() || !targetForm.arn.trim()}
                onClick={() => addTargetMut.mutate()}
              >
                Add
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="m">
            <FormField label="Target ID" constraintText="Required">
              <Input
                value={targetForm.id}
                placeholder="target-1"
                onChange={({ detail }) => setTargetForm({ ...targetForm, id: detail.value })}
              />
            </FormField>
            <FormField label="ARN" constraintText="Required">
              <Input
                value={targetForm.arn}
                placeholder="arn:aws:sqs:us-east-1:000000000000:my-queue"
                onChange={({ detail }) => setTargetForm({ ...targetForm, arn: detail.value })}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>
    </ContentLayout>
  )
}
