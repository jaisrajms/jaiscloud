import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  IconButton,
  Link,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material'
import AddIcon from '@mui/icons-material/Add'
import ArrowBackIcon from '@mui/icons-material/ArrowBack'
import { Link as RouterLink, useParams } from 'react-router-dom'
import { getKeyRing, getKeyRingIam, listCryptoKeys, putKeyRingIam } from '../../api/gcp/kms'
import { useAccount } from '../../context/AccountContext'
import { IamPolicyPanel } from '../common/IamPolicyPanel'
import { CreateCryptoKeyDialog } from './CreateCryptoKeyDialog'
import { GcpPageTitle } from '../common/PageTitle'

/** One KMS key ring: its crypto keys and IAM policy. */
export function KeyRingDetailPage() {
  const { location = '', keyRing = '' } = useParams()
  const { accountId } = useAccount()
  const [createOpen, setCreateOpen] = useState(false)

  const detail = useQuery({
    queryKey: ['gcp', 'kms', 'keyRing', location, keyRing, accountId],
    queryFn: () => getKeyRing(location, keyRing),
    enabled: Boolean(location && keyRing),
  })

  const keys = useQuery({
    queryKey: ['gcp', 'kms', 'cryptoKeys', location, keyRing, accountId],
    queryFn: () => listCryptoKeys(location, keyRing),
    enabled: Boolean(location && keyRing),
  })

  return (
    <Box>
      <Stack
        direction="row"
        spacing={1}
        sx={{ alignItems: 'center', mb: 2, flexWrap: 'wrap', rowGap: 1 }}
      >
        <IconButton component={RouterLink} to="/gcp/kms/keyrings" aria-label="Back to key rings">
          <ArrowBackIcon />
        </IconButton>
        <Box sx={{ flexGrow: 1, minWidth: 0 }}>
          <GcpPageTitle id="kms">{keyRing}</GcpPageTitle>
          <Typography variant="body2" color="text.secondary">
            Key ring · {location} · project {accountId || '—'}
          </Typography>
        </Box>
        <Button variant="contained" startIcon={<AddIcon />} onClick={() => setCreateOpen(true)}>
          Create crypto key
        </Button>
      </Stack>

      {detail.isError && <Alert severity="error">Failed to load the key ring.</Alert>}
      {detail.data && (
        <Box sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2, p: 2, mb: 3 }}>
          <Typography variant="body2" color="text.secondary">
            Created: {detail.data.createTime || '—'}
          </Typography>
        </Box>
      )}

      <TableContainer sx={{ border: '1px solid', borderColor: 'divider', borderRadius: 2 }}>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell>Crypto key</TableCell>
              <TableCell>Purpose</TableCell>
              <TableCell>Algorithm</TableCell>
              <TableCell>Primary</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {keys.isLoading && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4 }}>
                  <CircularProgress size={24} />
                </TableCell>
              </TableRow>
            )}
            {!keys.isLoading && (keys.data?.cryptoKeys.length ?? 0) === 0 && (
              <TableRow>
                <TableCell colSpan={4} align="center" sx={{ py: 4, color: 'text.secondary' }}>
                  No crypto keys in this key ring.
                </TableCell>
              </TableRow>
            )}
            {keys.data?.cryptoKeys.map((key) => (
              <TableRow key={key.name} hover>
                <TableCell>
                  <Link
                    component={RouterLink}
                    to={`/gcp/kms/keyrings/${encodeURIComponent(location)}/${encodeURIComponent(keyRing)}/keys/${encodeURIComponent(key.cryptoKeyId || '')}`}
                  >
                    {key.cryptoKeyId}
                  </Link>
                </TableCell>
                <TableCell>{key.purpose || '—'}</TableCell>
                <TableCell>{key.algorithm || '—'}</TableCell>
                <TableCell>
                  <Chip
                    size="small"
                    label={`v${key.primaryVersion || '?'} · ${key.primaryState || 'ENABLED'}`}
                    color={key.primaryState === 'ENABLED' ? 'success' : 'default'}
                  />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>

      <Box sx={{ mt: 4 }}>
        <IamPolicyPanel
          title="Key ring IAM policy"
          queryKey={['gcp', 'kms', 'keyRing', location, keyRing, 'iam']}
          load={() => getKeyRingIam(location, keyRing)}
          save={(policy) => putKeyRingIam(location, keyRing, policy)}
          defaultRole="roles/cloudkms.cryptoKeyEncrypterDecrypter"
        />
      </Box>

      <CreateCryptoKeyDialog
        open={createOpen}
        location={location}
        keyRing={keyRing}
        onClose={() => setCreateOpen(false)}
      />
    </Box>
  )
}
