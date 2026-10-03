import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import {
  Alert,
  Badge,
  Box,
  Button,
  Container,
  FormField,
  Header,
  KeyValuePairs,
  Select,
  SpaceBetween,
  Tabs,
} from '@cloudscape-design/components'
import { JsonEditor } from '../../../components/JsonEditor'
import { invokeFunction, type InvokeResponse } from '../../../api/lambda'
import { useNotifications } from '../../../components/notifications'

interface Props {
  name: string
}

type ResultTab = 'response' | 'logs' | 'details'

const INVOCATION_OPTIONS = [
  { value: 'RequestResponse', label: 'RequestResponse' },
  { value: 'Event', label: 'Event (async)' },
  { value: 'DryRun', label: 'DryRun' },
]

function decodeBase64(s: string): string {
  try {
    return atob(s)
  } catch {
    return s
  }
}

function tryPrettyJson(s: string): string {
  if (!s) return ''
  try {
    return JSON.stringify(JSON.parse(s), null, 2)
  } catch {
    return s
  }
}

function logColor(line: string): string {
  if (/^START /.test(line)) return '#4caf78'
  if (/^END /.test(line)) return '#79b8ff'
  if (/^REPORT /.test(line)) return '#9da8b5'
  if (/^ERROR/.test(line)) return '#ff6b6b'
  if (/^WARN/.test(line)) return '#ffa94d'
  if (/^INFO/.test(line)) return '#74c7f0'
  if (/^DEBUG/.test(line)) return '#b197fc'
  return '#e0e0e0'
}

export function LambdaTest({ name }: Props) {
  const [payload, setPayload] = useState('{}')
  const [invocationType, setInvocationType] = useState<'RequestResponse' | 'Event' | 'DryRun'>(
    'RequestResponse',
  )
  const [result, setResult] = useState<InvokeResponse | null>(null)
  const [resultTab, setResultTab] = useState<ResultTab>('response')
  const navigate = useNavigate()
  const { notify } = useNotifications()

  const invokeMut = useMutation({
    mutationFn: () => invokeFunction(name, { payload, invocationType }),
    onSuccess: (data) => {
      setResult(data)
      setResultTab('response')
      notify({ type: 'success', header: 'Function invoked' })
    },
    onError: (err) =>
      notify({ type: 'error', header: 'Invoke failed', content: (err as Error).message }),
  })

  const logLines = result?.logResult ? decodeBase64(result.logResult).split('\n').filter(Boolean) : []
  const hasError = !!result?.functionError
  const logGroupPath = `/aws/logs/groups/%2Faws%2Flambda%2F${encodeURIComponent(name)}`

  return (
    <SpaceBetween size="l">
      <Container
        header={
          <Header
            variant="h2"
            actions={
              <Select
                selectedOption={
                  INVOCATION_OPTIONS.find((option) => option.value === invocationType) ??
                  INVOCATION_OPTIONS[0]!
                }
                onChange={({ detail }) =>
                  setInvocationType(detail.selectedOption.value as typeof invocationType)
                }
                options={INVOCATION_OPTIONS}
                ariaLabel="Invocation type"
              />
            }
          >
            Test event
          </Header>
        }
      >
        <FormField label="Test event payload">
          <JsonEditor
            value={payload}
            onChange={setPayload}
            height={200}
            ariaLabel="Test event payload"
          />
        </FormField>
      </Container>

      <SpaceBetween direction="horizontal" size="xs">
        <Button variant="primary" loading={invokeMut.isPending} onClick={() => invokeMut.mutate()}>
          Invoke
        </Button>
      </SpaceBetween>

      {invokeMut.error && (
        <Alert type="error" header="Invocation failed">
          {(invokeMut.error as Error).message}
        </Alert>
      )}

      {result && (
        <Container header={<Header variant="h2">Result</Header>}>
          <Tabs
            tabs={[
              {
                id: 'response',
                label: hasError ? (
                  <SpaceBetween direction="horizontal" size="xxs">
                    <span>Response</span>
                    <Badge color="red">Error</Badge>
                  </SpaceBetween>
                ) : (
                  'Response'
                ),
              },
              { id: 'logs', label: 'Logs' },
              { id: 'details', label: 'Details' },
            ]}
            activeTabId={resultTab}
            onChange={({ detail }) => setResultTab(detail.activeTabId as ResultTab)}
          />

          <Box margin={{ top: 'l' }}>
            {resultTab === 'response' &&
              (hasError ? (
                <Alert type="error" header="Function error">
                  {tryPrettyJson(result.payload) || '(empty response)'}
                </Alert>
              ) : (
                <Box variant="pre">{tryPrettyJson(result.payload) || '(empty response)'}</Box>
              ))}

            {resultTab === 'logs' && (
              <SpaceBetween size="m">
                <pre style={logsStyle}>
                  {logLines.length === 0 ? (
                    <span style={{ color: '#9da8b5' }}>No log output.</span>
                  ) : (
                    logLines.map((line, index) => (
                      <div key={index} style={{ color: logColor(line) }}>
                        {line}
                      </div>
                    ))
                  )}
                </pre>
                {result.requestId && (
                  <Button
                    iconName="angle-right"
                    iconAlign="right"
                    onClick={() =>
                      navigate(
                        `${logGroupPath}?requestId=${encodeURIComponent(result.requestId!)}`,
                      )
                    }
                  >
                    View logs for this invocation
                  </Button>
                )}
              </SpaceBetween>
            )}

            {resultTab === 'details' && (
              <KeyValuePairs
                columns={2}
                items={[
                  { label: 'Status code', value: String(result.statusCode) },
                  { label: 'Function error', value: result.functionError || '—' },
                  { label: 'Executed version', value: result.executedVersion || '—' },
                  {
                    label: 'Billed duration',
                    value: result.billedDurationMs ? `${result.billedDurationMs} ms` : '—',
                  },
                  { label: 'Request ID', value: result.requestId || '—' },
                ]}
              />
            )}
          </Box>
        </Container>
      )}
    </SpaceBetween>
  )
}

const logsStyle: React.CSSProperties = {
  margin: 0,
  padding: '0.75rem',
  background: '#1e1e1e',
  color: '#e0e0e0',
  borderRadius: 6,
  overflow: 'auto',
  maxHeight: 400,
  fontSize: '0.8em',
  fontFamily: 'monospace',
}
