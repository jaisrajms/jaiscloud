import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Badge,
  Box,
  Button,
  ContentLayout,
  Form,
  FormField,
  Header,
  Input,
  Modal,
  SpaceBetween,
  Table,
  Tabs,
  Textarea,
} from '@cloudscape-design/components'
import { CopyText } from '../../../components/CopyText'
import { ErrorState } from '../../../components/ErrorState'
import {
  listRoles,
  createRole,
  deleteRole,
  listUsers,
  createUser,
  deleteUser,
  listAccessKeys,
  createAccessKey,
  deleteAccessKey,
  listPolicies,
  createPolicy,
  deletePolicy,
  type IAMRole,
  type IAMUser,
  type IAMPolicy,
  type AccessKey,
} from '../../../api/iam'
import { formatDate } from '../../../lib/date'
import { ResourceTable, type ResourceColumn } from '../../../components/ResourceTable'
import { useNotifications } from '../../../components/notifications'

type Tab = 'roles' | 'users' | 'policies'

export function IAMList() {
  const [tab, setTab] = useState<Tab>('roles')

  return (
    <ContentLayout header={<Header variant="h1">IAM</Header>}>
      <SpaceBetween size="l">
        <Tabs
          tabs={[
            { id: 'roles', label: 'Roles' },
            { id: 'users', label: 'Users' },
            { id: 'policies', label: 'Policies' },
          ]}
          activeTabId={tab}
          onChange={({ detail }) => setTab(detail.activeTabId as Tab)}
        />
        {tab === 'roles' && <RolesTab />}
        {tab === 'users' && <UsersTab />}
        {tab === 'policies' && <PoliciesTab />}
      </SpaceBetween>
    </ContentLayout>
  )
}

function RolesTab() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDesc, setNewDesc] = useState('')
  const [newPolicy, setNewPolicy] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<IAMRole | null>(null)
  const qc = useQueryClient()
  const { notify } = useNotifications()

  const { data, isLoading, error } = useQuery({
    queryKey: ['iam', 'roles'],
    queryFn: () => listRoles(),
  })

  const createMut = useMutation({
    mutationFn: () =>
      createRole({
        roleName: newName,
        description: newDesc || undefined,
        assumeRolePolicyDocument: newPolicy || undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'roles'] })
      notify({ type: 'success', header: 'Role created', content: newName })
      setCreateOpen(false)
      setNewName('')
      setNewDesc('')
      setNewPolicy('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create role', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteRole(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'roles'] })
      notify({ type: 'success', header: 'Role deleted' })
      setConfirmDelete(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const roles = data?.items ?? []

  const columns: ResourceColumn<IAMRole>[] = [
    {
      id: 'roleName',
      header: 'Role name',
      filterLabel: 'Role name',
      filterValue: (r) => r.roleName,
      cell: (r) => r.roleName,
    },
    {
      id: 'arn',
      header: 'ARN',
      cell: (r) => <CopyText value={r.arn} label="role ARN" />,
    },
    {
      id: 'created',
      header: 'Created',
      cell: (r) => formatDate(r.createDate),
    },
    {
      id: 'actions',
      header: 'Actions',
      cell: (r) => (
        <Button variant="link" onClick={() => setConfirmDelete(r)}>
          Delete
        </Button>
      ),
    },
  ]

  return (
    <SpaceBetween size="m">
      {error ? (
        <ErrorState header="Failed to load roles" message={(error as Error).message} />
      ) : (
        <ResourceTable
          favoriteService="iam"
          favorite={(r) => ({ id: r.arn, label: r.roleName, href: '/aws/iam', type: 'role' })}
          items={roles}
          columns={columns}
          trackBy={(r) => r.arn}
          title="Roles"
          loading={isLoading}
          actions={
            <Button variant="primary" onClick={() => setCreateOpen(true)}>
              Create role
            </Button>
          }
          emptyTitle="No roles"
          emptyBody="IAM roles grant AWS service access permissions."
        />
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create role"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!newName}
                onClick={() => createMut.mutate()}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="l">
            <FormField label="Role name">
              <Input
                autoFocus
                value={newName}
                onChange={({ detail }) => setNewName(detail.value)}
                placeholder="my-lambda-role"
              />
            </FormField>
            <FormField label="Description" description="Optional">
              <Input
                value={newDesc}
                onChange={({ detail }) => setNewDesc(detail.value)}
                placeholder="Role description"
              />
            </FormField>
            <FormField
              label="Trust policy document"
              description="Optional. Leave blank for the default Lambda trust policy."
            >
              <Textarea
                value={newPolicy}
                onChange={({ detail }) => setNewPolicy(detail.value)}
                placeholder={'{\n  "Version": "2012-10-17",\n  "Statement": [...]\n}'}
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete !== null}
        onDismiss={() => setConfirmDelete(null)}
        header="Delete role"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => deleteMut.mutate(confirmDelete?.roleName ?? '')}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Delete role <b>{confirmDelete?.roleName}</b>? This cannot be undone.
      </Modal>
    </SpaceBetween>
  )
}

function UsersTab() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<IAMUser | null>(null)
  const [keysUser, setKeysUser] = useState<IAMUser | null>(null)
  const [newKey, setNewKey] = useState<AccessKey | null>(null)
  const qc = useQueryClient()
  const { notify } = useNotifications()

  const { data, isLoading, error } = useQuery({
    queryKey: ['iam', 'users'],
    queryFn: () => listUsers(),
  })

  const { data: keysData } = useQuery({
    queryKey: ['iam', 'access-keys', keysUser?.userName],
    queryFn: () => listAccessKeys(keysUser!.userName),
    enabled: !!keysUser,
  })

  const createMut = useMutation({
    mutationFn: () => createUser({ userName: newName }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'users'] })
      notify({ type: 'success', header: 'User created', content: newName })
      setCreateOpen(false)
      setNewName('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create user', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: (name: string) => deleteUser(name),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'users'] })
      notify({ type: 'success', header: 'User deleted' })
      setConfirmDelete(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const createKeyMut = useMutation({
    mutationFn: () => createAccessKey(keysUser!.userName),
    onSuccess: (resp) => {
      void qc.invalidateQueries({ queryKey: ['iam', 'access-keys', keysUser?.userName] })
      notify({ type: 'success', header: 'Access key created' })
      setNewKey(resp.AccessKey)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create access key', content: (err as Error).message }),
  })

  const deleteKeyMut = useMutation({
    mutationFn: ({ userName, keyId }: { userName: string; keyId: string }) =>
      deleteAccessKey(userName, keyId),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'access-keys', keysUser?.userName] })
      notify({ type: 'success', header: 'Access key deleted' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const users = data?.items ?? []
  const accessKeys = keysData?.items ?? []

  const columns: ResourceColumn<IAMUser>[] = [
    {
      id: 'userName',
      header: 'User name',
      filterLabel: 'User name',
      filterValue: (u) => u.userName,
      cell: (u) => u.userName,
    },
    {
      id: 'arn',
      header: 'ARN',
      cell: (u) => <CopyText value={u.arn} label="user ARN" />,
    },
    {
      id: 'created',
      header: 'Created',
      cell: (u) => formatDate(u.createDate),
    },
    {
      id: 'actions',
      header: 'Actions',
      cell: (u) => (
        <div onClick={(event) => event.stopPropagation()}>
          <SpaceBetween direction="horizontal" size="xs">
            <Button variant="link" onClick={() => setKeysUser(u)}>
              Access keys
            </Button>
            <Button variant="link" onClick={() => setConfirmDelete(u)}>
              Delete
            </Button>
          </SpaceBetween>
        </div>
      ),
    },
  ]

  return (
    <SpaceBetween size="m">
      {error ? (
        <ErrorState header="Failed to load users" message={(error as Error).message} />
      ) : (
        <ResourceTable
          favoriteService="iam"
          favorite={(u) => ({ id: u.arn, label: u.userName, href: '/aws/iam', type: 'user' })}
          items={users}
          columns={columns}
          trackBy={(u) => u.arn}
          title="Users"
          loading={isLoading}
          actions={
            <Button variant="primary" onClick={() => setCreateOpen(true)}>
              Create user
            </Button>
          }
          emptyTitle="No users"
          emptyBody="IAM users allow programmatic access to AWS services."
        />
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create user"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!newName}
                onClick={() => createMut.mutate()}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <FormField label="User name">
            <Input
              autoFocus
              value={newName}
              onChange={({ detail }) => setNewName(detail.value)}
              placeholder="my-service-user"
            />
          </FormField>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete !== null}
        onDismiss={() => setConfirmDelete(null)}
        header="Delete user"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => deleteMut.mutate(confirmDelete?.userName ?? '')}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Delete user <b>{confirmDelete?.userName}</b>? This cannot be undone.
      </Modal>

      <Modal
        visible={keysUser !== null}
        onDismiss={() => {
          setKeysUser(null)
          setNewKey(null)
        }}
        size="large"
        header={`Access keys — ${keysUser?.userName ?? ''}`}
        footer={
          <Box float="right">
            <Button
              onClick={() => {
                setKeysUser(null)
                setNewKey(null)
              }}
            >
              Close
            </Button>
          </Box>
        }
      >
        <SpaceBetween size="m">
          <Box>
            <Button
              variant="primary"
              loading={createKeyMut.isPending}
              onClick={() => createKeyMut.mutate()}
            >
              Create key
            </Button>
          </Box>

          {newKey && (
            <Alert
              type="success"
              header="Access key created"
              dismissible
              onDismiss={() => setNewKey(null)}
            >
              <SpaceBetween size="xs">
                <Box>
                  Save the secret access key now — it will not be shown again.
                </Box>
                <div>
                  <b>Access Key ID:</b> <Box variant="code">{newKey.accessKeyId}</Box>
                </div>
                <div>
                  <b>Secret Access Key:</b> <Box variant="code">{newKey.secretAccessKey}</Box>
                </div>
              </SpaceBetween>
            </Alert>
          )}

          <Table
            items={accessKeys}
            columnDefinitions={[
              {
                id: 'accessKeyId',
                header: 'Access Key ID',
                cell: (k: AccessKey) => <Box variant="code">{k.accessKeyId}</Box>,
              },
              {
                id: 'status',
                header: 'Status',
                cell: (k: AccessKey) => (
                  <Badge color={k.status === 'Active' ? 'green' : 'grey'}>{k.status}</Badge>
                ),
              },
              {
                id: 'created',
                header: 'Created',
                cell: (k: AccessKey) => formatDate(k.createDate),
              },
              {
                id: 'actions',
                header: '',
                cell: (k: AccessKey) => (
                  <Button
                    variant="link"
                    loading={deleteKeyMut.isPending}
                    onClick={() =>
                      deleteKeyMut.mutate({
                        userName: keysUser?.userName ?? '',
                        keyId: k.accessKeyId,
                      })
                    }
                  >
                    Delete
                  </Button>
                ),
              },
            ]}
            trackBy={(k) => k.accessKeyId}
            empty={
              <Box textAlign="center" color="inherit">
                <b>No access keys</b>
              </Box>
            }
          />
        </SpaceBetween>
      </Modal>
    </SpaceBetween>
  )
}

function PoliciesTab() {
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDoc, setNewDoc] = useState('')
  const [newDesc, setNewDesc] = useState('')
  const [confirmDelete, setConfirmDelete] = useState<IAMPolicy | null>(null)
  const qc = useQueryClient()
  const { notify } = useNotifications()

  const { data, isLoading, error } = useQuery({
    queryKey: ['iam', 'policies'],
    queryFn: () => listPolicies({ scope: 'Local' }),
  })

  const createMut = useMutation({
    mutationFn: () =>
      createPolicy({
        policyName: newName,
        policyDocument: newDoc,
        description: newDesc || undefined,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'policies'] })
      notify({ type: 'success', header: 'Policy created', content: newName })
      setCreateOpen(false)
      setNewName('')
      setNewDoc('')
      setNewDesc('')
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Could not create policy', content: (err as Error).message }),
  })

  const deleteMut = useMutation({
    mutationFn: (arn: string) => deletePolicy(arn),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['iam', 'policies'] })
      notify({ type: 'success', header: 'Policy deleted' })
      setConfirmDelete(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const policies = data?.items ?? []

  const columns: ResourceColumn<IAMPolicy>[] = [
    {
      id: 'policyName',
      header: 'Policy name',
      filterLabel: 'Policy name',
      filterValue: (p) => p.policyName,
      cell: (p) => p.policyName,
    },
    {
      id: 'arn',
      header: 'ARN',
      cell: (p) => <CopyText value={p.arn} label="policy ARN" />,
    },
    {
      id: 'attachmentCount',
      header: 'Attachments',
      cell: (p) => p.attachmentCount,
    },
    {
      id: 'created',
      header: 'Created',
      cell: (p) => formatDate(p.createDate),
    },
    {
      id: 'actions',
      header: 'Actions',
      cell: (p) => (
        <Button variant="link" onClick={() => setConfirmDelete(p)}>
          Delete
        </Button>
      ),
    },
  ]

  return (
    <SpaceBetween size="m">
      {error ? (
        <ErrorState header="Failed to load policies" message={(error as Error).message} />
      ) : (
        <ResourceTable
          favoriteService="iam"
          favorite={(p) => ({ id: p.arn, label: p.policyName, href: '/aws/iam', type: 'policy' })}
          items={policies}
          columns={columns}
          trackBy={(p) => p.arn}
          title="Policies"
          loading={isLoading}
          actions={
            <Button variant="primary" onClick={() => setCreateOpen(true)}>
              Create policy
            </Button>
          }
          emptyTitle="No policies"
          emptyBody="IAM policies define permissions that can be attached to roles and users."
        />
      )}

      <Modal
        visible={createOpen}
        onDismiss={() => setCreateOpen(false)}
        header="Create policy"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setCreateOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={createMut.isPending}
                disabled={!newName || !newDoc}
                onClick={() => createMut.mutate()}
              >
                Create
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <Form>
          <SpaceBetween size="l">
            <FormField label="Policy name">
              <Input
                autoFocus
                value={newName}
                onChange={({ detail }) => setNewName(detail.value)}
                placeholder="my-policy"
              />
            </FormField>
            <FormField label="Policy document">
              <Textarea
                value={newDoc}
                onChange={({ detail }) => setNewDoc(detail.value)}
                placeholder={
                  '{\n  "Version": "2012-10-17",\n  "Statement": [\n    {\n      "Effect": "Allow",\n      "Action": "*",\n      "Resource": "*"\n    }\n  ]\n}'
                }
              />
            </FormField>
            <FormField label="Description" description="Optional">
              <Input
                value={newDesc}
                onChange={({ detail }) => setNewDesc(detail.value)}
                placeholder="Policy description"
              />
            </FormField>
          </SpaceBetween>
        </Form>
      </Modal>

      <Modal
        visible={confirmDelete !== null}
        onDismiss={() => setConfirmDelete(null)}
        header="Delete policy"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => deleteMut.mutate(confirmDelete?.arn ?? '')}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        Delete policy <b>{confirmDelete?.policyName}</b>? This cannot be undone.
      </Modal>
    </SpaceBetween>
  )
}
