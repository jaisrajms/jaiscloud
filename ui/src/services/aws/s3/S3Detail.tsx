import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Alert,
  Box,
  Button,
  Container,
  ContentLayout,
  FileUpload,
  Header,
  Icon,
  Input,
  Link,
  Modal,
  SpaceBetween,
  Table,
} from '@cloudscape-design/components'
import { ErrorState } from '../../../components/ErrorState'
import type { TableProps } from '@cloudscape-design/components'
import {
  listObjects,
  deleteObject,
  deleteObjects,
  downloadObjectUrl,
  putObject,
  type S3Object,
} from '../../../api/s3'
import { formatDate } from '../../../lib/date'
import { useNotifications } from '../../../components/notifications'

function fmtSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`
}

export function S3Detail() {
  const { bucket: encodedBucket } = useParams<{ bucket: string }>()
  const bucket = decodeURIComponent(encodedBucket ?? '')
  const navigate = useNavigate()
  const qc = useQueryClient()

  const [prefix, setPrefix] = useState('')
  const [prefixInput, setPrefixInput] = useState('')
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [confirmDelete, setConfirmDelete] = useState<S3Object | null>(null)
  const [uploadOpen, setUploadOpen] = useState(false)
  const [files, setFiles] = useState<File[]>([])
  const { notify } = useNotifications()

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['s3', 'objects', bucket, prefix],
    queryFn: () => listObjects(bucket, { prefix, delimiter: '/' }),
  })

  const deleteMut = useMutation({
    mutationFn: (key: string) => deleteObject(bucket, key),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['s3', 'objects', bucket] })
      notify({ type: 'success', header: 'Object deleted' })
      setConfirmDelete(null)
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const deleteBatchMut = useMutation({
    mutationFn: (keys: string[]) => deleteObjects(bucket, keys),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['s3', 'objects', bucket] })
      notify({ type: 'success', header: 'Objects deleted' })
      setSelected(new Set())
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Delete failed', content: (err as Error).message }),
  })

  const uploadMut = useMutation({
    mutationFn: async () => {
      for (const file of files) {
        await putObject(bucket, `${prefix}${file.name}`, file)
      }
    },
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['s3', 'objects', bucket] })
      notify({
        type: 'success',
        header: `Uploaded ${files.length} object${files.length !== 1 ? 's' : ''}`,
      })
      setUploadOpen(false)
      setFiles([])
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Upload failed', content: (err as Error).message }),
  })

  const objects = data?.items ?? []
  const prefixes = data?.commonPrefixes ?? []
  const selectedItems = objects.filter((object) => selected.has(object.key))

  const navigatePrefix = (p: string) => {
    setPrefix(p)
    setPrefixInput(p)
    setSelected(new Set())
  }

  const columns: TableProps.ColumnDefinition<S3Object>[] = [
    {
      id: 'key',
      header: 'Key',
      cell: (object) => <Box variant="code">{object.key.replace(prefix, '')}</Box>,
    },
    {
      id: 'size',
      header: 'Size',
      cell: (object) => fmtSize(object.size),
    },
    {
      id: 'modified',
      header: 'Last modified',
      cell: (object) => formatDate(object.lastModified),
    },
    {
      id: 'actions',
      header: '',
      cell: (object) => (
        <SpaceBetween direction="horizontal" size="xs">
          <Button
            variant="inline-link"
            href={downloadObjectUrl(bucket, object.key)}
            download={object.key.split('/').pop()}
          >
            Download
          </Button>
          <Button variant="inline-link" onClick={() => setConfirmDelete(object)}>
            Delete
          </Button>
        </SpaceBetween>
      ),
    },
  ]

  return (
    <ContentLayout
      header={
        <Header
          variant="h1"
          description={
            <Link
              href="/ui/aws/s3"
              onFollow={(event) => {
                event.preventDefault()
                navigate('/aws/s3')
              }}
            >
              <Icon name="angle-left" /> All buckets
            </Link>
          }
          actions={
            <Button variant="primary" onClick={() => setUploadOpen(true)}>
              Upload
            </Button>
          }
        >
          {bucket}
        </Header>
      }
    >
      <SpaceBetween size="l">
        <SpaceBetween direction="horizontal" size="xs">
          <Input
            value={prefixInput}
            onChange={({ detail }) => setPrefixInput(detail.value)}
            onKeyDown={({ detail }) => {
              if (detail.key === 'Enter') navigatePrefix(prefixInput)
            }}
            placeholder="Filter by prefix…"
            ariaLabel="Prefix"
          />
          <Button onClick={() => navigatePrefix(prefixInput)}>Go</Button>
          {prefix && <Button onClick={() => navigatePrefix('')}>Clear</Button>}
        </SpaceBetween>

        {prefix && (
          <SpaceBetween direction="horizontal" size="xxs">
            <Link
              href={`/ui/aws/s3/${encodeURIComponent(bucket)}`}
              onFollow={(event) => {
                event.preventDefault()
                navigatePrefix('')
              }}
            >
              {bucket}
            </Link>
            {prefix
              .split('/')
              .filter(Boolean)
              .map((part, index, parts) => {
                const path = parts.slice(0, index + 1).join('/') + '/'
                const isLast = index === parts.length - 1
                return (
                  <SpaceBetween key={path} direction="horizontal" size="xxs">
                    <Box variant="span" color="text-body-secondary">
                      /
                    </Box>
                    {isLast ? (
                      <Box variant="span">{part}</Box>
                    ) : (
                      <Link
                        href={`/ui/aws/s3/${encodeURIComponent(bucket)}?prefix=${encodeURIComponent(path)}`}
                        onFollow={(event) => {
                          event.preventDefault()
                          navigatePrefix(path)
                        }}
                      >
                        {part}
                      </Link>
                    )}
                  </SpaceBetween>
                )
              })}
          </SpaceBetween>
        )}

        {error && (
          <ErrorState header="Failed to load objects" message={(error as Error).message} onRetry={() => void refetch()} />
        )}

        {!error && prefixes.length > 0 && (
          <Container header={<Header variant="h2">Folders</Header>}>
            <SpaceBetween size="xs">
              {prefixes.map((p) => (
                <Button key={p} variant="inline-link" onClick={() => navigatePrefix(p)}>
                  {p.replace(prefix, '')}
                </Button>
              ))}
            </SpaceBetween>
          </Container>
        )}

        {!error && (
          <Table
            items={objects}
            columnDefinitions={columns}
            trackBy={(object) => object.key}
            loading={isLoading}
            loadingText="Loading objects"
            selectionType="multi"
            selectedItems={selectedItems}
            onSelectionChange={({ detail }) =>
              setSelected(new Set(detail.selectedItems.map((object) => object.key)))
            }
            header={
              <Header
                variant="h2"
                counter={`(${objects.length})`}
                actions={
                  <Button
                    disabled={selected.size === 0}
                    loading={deleteBatchMut.isPending}
                    onClick={() => deleteBatchMut.mutate(Array.from(selected))}
                  >
                    Delete selected
                  </Button>
                }
              >
                Objects
              </Header>
            }
            empty={
              <Box textAlign="center" color="inherit">
                <b>No objects</b>
                <Box variant="p" color="inherit">
                  No objects{prefix ? ` with prefix "${prefix}"` : ' in this bucket'}.
                </Box>
              </Box>
            }
          />
        )}

        {data?.isTruncated && (
          <Box color="text-body-secondary" textAlign="center">
            More objects available — use prefix filter to narrow results.
          </Box>
        )}
      </SpaceBetween>

      <Modal
        visible={confirmDelete !== null}
        onDismiss={() => setConfirmDelete(null)}
        header="Delete object"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setConfirmDelete(null)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={deleteMut.isPending}
                onClick={() => confirmDelete && deleteMut.mutate(confirmDelete.key)}
              >
                Delete
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <SpaceBetween size="m">
          <Box variant="p">
            Permanently delete <b>{confirmDelete?.key}</b>?
          </Box>
          {deleteMut.error && <Alert type="error">{(deleteMut.error as Error).message}</Alert>}
        </SpaceBetween>
      </Modal>

      <Modal
        visible={uploadOpen}
        onDismiss={() => setUploadOpen(false)}
        header="Upload objects"
        footer={
          <Box float="right">
            <SpaceBetween direction="horizontal" size="xs">
              <Button variant="link" onClick={() => setUploadOpen(false)}>
                Cancel
              </Button>
              <Button
                variant="primary"
                loading={uploadMut.isPending}
                disabled={files.length === 0}
                onClick={() => uploadMut.mutate()}
              >
                Upload
              </Button>
            </SpaceBetween>
          </Box>
        }
      >
        <FileUpload
          value={files}
          onChange={({ detail }) => setFiles(detail.value)}
          multiple
          accept="*/*"
          i18nStrings={{
            uploadButtonText: (multiple) => (multiple ? 'Choose files' : 'Choose file'),
            dropzoneText: (multiple) => (multiple ? 'Drop files to upload' : 'Drop file to upload'),
            removeFileAriaLabel: (fileIndex) => `Remove file ${fileIndex + 1}`,
            limitShowFewer: 'Show fewer files',
            limitShowMore: 'Show more files',
            errorIconAriaLabel: 'Error',
          }}
          constraintText={prefix ? `Uploaded under prefix "${prefix}"` : undefined}
        />
      </Modal>
    </ContentLayout>
  )
}
