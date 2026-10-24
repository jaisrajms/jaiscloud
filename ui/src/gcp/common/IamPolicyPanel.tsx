import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  IconButton,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableRow,
  TextField,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined'

/** Structural IAM policy shape shared by the GCP service API clients. */
export interface IamBindingLike {
  role: string
  members: string[]
  condition?: Record<string, unknown>
}

export interface IamPolicyLike {
  bindings?: IamBindingLike[]
  etag?: string
  version?: number
}

export interface IamPolicyPanelProps {
  title: string
  /** React Query key for the policy. */
  queryKey: unknown[]
  load: () => Promise<IamPolicyLike>
  save: (policy: IamPolicyLike) => Promise<IamPolicyLike>
  /** Role prefilled in the add-binding field. */
  defaultRole?: string
}

/**
 * Editable IAM policy for a GCP resource. Shared by the Pub/Sub, IAM, Cloud KMS
 * and Secret Manager console pages so each speaks one policy UI. A stale etag
 * fails the save (the provider enforces optimistic concurrency).
 */
export function IamPolicyPanel({
  title,
  queryKey,
  load,
  save,
  defaultRole = '',
}: IamPolicyPanelProps) {
  const queryClient = useQueryClient()
  const query = useQuery({ queryKey, queryFn: load })
  const [bindings, setBindings] = useState<IamBindingLike[]>([])
  const [role, setRole] = useState(defaultRole)
  const [members, setMembers] = useState('')

  useEffect(() => {
    setBindings(query.data?.bindings ?? [])
  }, [query.data])

  const saveMutation = useMutation({
    mutationFn: (policy: IamPolicyLike) => save(policy),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey }),
  })

  const add = () => {
    if (!role || !members.trim()) return
    setBindings((prev) => [
      ...prev.filter((b) => b.role !== role),
      { role, members: members.split(',').map((m) => m.trim()).filter(Boolean) },
    ])
    setMembers('')
  }

  return (
    <Box sx={{ maxWidth: 720 }}>
      <Typography variant="h6" sx={{ mb: 1 }}>
        {title}
      </Typography>
      {query.isError && <Alert severity="error">Failed to load IAM policy.</Alert>}
      {saveMutation.isError && (
        <Alert severity="error">Failed to save (etag mismatch?).</Alert>
      )}
      {saveMutation.isSuccess && <Alert severity="success">Policy saved.</Alert>}

      <Table size="small">
        <TableHead>
          <TableRow>
            <TableCell>Role</TableCell>
            <TableCell>Members</TableCell>
            <TableCell align="right">Actions</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {bindings.map((b) => (
            <TableRow key={b.role}>
              <TableCell>{b.role}</TableCell>
              <TableCell>{b.members.join(', ')}</TableCell>
              <TableCell align="right">
                <IconButton
                  size="small"
                  onClick={() => setBindings((prev) => prev.filter((x) => x.role !== b.role))}
                >
                  <DeleteOutlineIcon fontSize="small" />
                </IconButton>
              </TableCell>
            </TableRow>
          ))}
          {bindings.length === 0 && (
            <TableRow>
              <TableCell colSpan={3} sx={{ color: 'text.secondary' }}>
                No bindings.
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>

      <Stack direction="row" spacing={1} sx={{ mt: 2 }}>
        <TextField
          label="Role"
          value={role}
          onChange={(e) => setRole(e.target.value)}
          size="small"
          sx={{ minWidth: 260 }}
        />
        <TextField
          label="Members (comma separated)"
          value={members}
          onChange={(e) => setMembers(e.target.value)}
          size="small"
          fullWidth
        />
        <Button onClick={add} startIcon={<AddIcon />}>
          Add
        </Button>
      </Stack>
      <Button
        variant="contained"
        sx={{ mt: 2 }}
        disabled={saveMutation.isPending}
        onClick={() =>
          saveMutation.mutate({
            bindings,
            etag: query.data?.etag,
            version: query.data?.version ?? 1,
          })
        }
      >
        Save policy
      </Button>
    </Box>
  )
}
