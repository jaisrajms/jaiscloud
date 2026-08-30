import { Fragment, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { invokeFunction, type InvokeResponse } from '../../../api/lambda'

interface Props {
  name: string
}

type ResultTab = 'response' | 'logs' | 'details'

function decodeBase64(s: string): string {
  try { return atob(s) } catch { return s }
}

function tryPrettyJson(s: string): string {
  if (!s) return ''
  try { return JSON.stringify(JSON.parse(s), null, 2) } catch { return s }
}

function colorLogLine(line: string): React.CSSProperties {
  if (/^START /.test(line)) return { color: '#4caf78' }
  if (/^END /.test(line)) return { color: '#79b8ff' }
  if (/^REPORT /.test(line)) return { color: '#9da8b5' }
  if (/^ERROR/.test(line)) return { color: '#ff6b6b' }
  if (/^WARN/.test(line)) return { color: '#ffa94d' }
  if (/^INFO/.test(line)) return { color: '#74c7f0' }
  if (/^DEBUG/.test(line)) return { color: '#b197fc' }
  return { color: '#e0e0e0' }
}

export function LambdaTest({ name }: Props) {
  const [payload, setPayload] = useState('{}')
  const [invocationType, setInvocationType] = useState<'RequestResponse' | 'Event' | 'DryRun'>('RequestResponse')
  const [result, setResult] = useState<InvokeResponse | null>(null)
  const [resultTab, setResultTab] = useState<ResultTab>('response')
  const navigate = useNavigate()

  const invokeMut = useMutation({
    mutationFn: () => invokeFunction(name, { payload, invocationType }),
    onSuccess: (data) => {
      setResult(data)
      setResultTab('response')
    },
  })

  const logLines = result?.logResult ? decodeBase64(result.logResult).split('\n').filter(Boolean) : []
  const hasError = !!result?.functionError
  const logGroupPath = `/aws/logs/groups/%2Faws%2Flambda%2F${encodeURIComponent(name)}`

  return (
    <div>
      {/* Payload editor */}
      <div style={{ marginBottom: '1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.5rem' }}>
          <label style={{ fontSize: '0.9em', fontWeight: 500 }}>Test event payload</label>
          <select
            value={invocationType}
            onChange={(e) => setInvocationType(e.target.value as typeof invocationType)}
            style={{ marginLeft: 'auto', border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.3rem 0.5rem', fontSize: '0.85em' }}
          >
            <option value="RequestResponse">RequestResponse</option>
            <option value="Event">Event (async)</option>
            <option value="DryRun">DryRun</option>
          </select>
        </div>
        <textarea
          value={payload}
          onChange={(e) => setPayload(e.target.value)}
          rows={8}
          spellCheck={false}
          style={{
            width: '100%', boxSizing: 'border-box', fontFamily: 'monospace', fontSize: '0.85em',
            border: '1px solid #c9cdd4', borderRadius: 4, padding: '0.6rem 0.75rem',
            resize: 'vertical', background: '#1e1e1e', color: '#d4d4d4', outline: 'none',
          }}
          placeholder="{}"
        />
      </div>

      <div style={{ display: 'flex', gap: '0.75rem', marginBottom: '1.5rem' }}>
        <button
          onClick={() => invokeMut.mutate()}
          disabled={invokeMut.isPending}
          style={{ background: '#e77600', color: '#fff', border: 'none', borderRadius: 4, padding: '0.5rem 1.25rem', cursor: 'pointer', fontSize: '0.9em', fontWeight: 500, opacity: invokeMut.isPending ? 0.6 : 1 }}
        >
          {invokeMut.isPending ? 'Invoking…' : 'Invoke'}
        </button>
        {invokeMut.error && (
          <span style={{ color: '#d13212', fontSize: '0.85em', alignSelf: 'center' }}>
            {(invokeMut.error as Error).message}
          </span>
        )}
      </div>

      {result && (
        <div>
          {/* Result tabs */}
          <div style={{ display: 'flex', borderBottom: '2px solid #e7e9ec', marginBottom: '1rem' }}>
            {(['response', 'logs', 'details'] as ResultTab[]).map((t) => (
              <button
                key={t}
                onClick={() => setResultTab(t)}
                style={{
                  background: 'none', border: 'none', padding: '0.5rem 1rem', cursor: 'pointer',
                  fontSize: '0.85em', fontWeight: resultTab === t ? 600 : 400,
                  color: resultTab === t ? '#e77600' : '#5f6b7a',
                  borderBottom: `2px solid ${resultTab === t ? '#e77600' : 'transparent'}`,
                  marginBottom: -2,
                }}
              >
                {t.charAt(0).toUpperCase() + t.slice(1)}
                {t === 'response' && hasError && (
                  <span style={{ marginLeft: '0.4rem', background: '#d13212', color: '#fff', borderRadius: 10, padding: '0 0.4em', fontSize: '0.75em' }}>!</span>
                )}
              </button>
            ))}
          </div>

          {resultTab === 'response' && (
            <pre style={{
              margin: 0, padding: '0.75rem', borderRadius: 6, overflow: 'auto',
              fontSize: '0.82em', fontFamily: 'monospace', whiteSpace: 'pre-wrap', wordBreak: 'break-all',
              background: hasError ? '#fff5f5' : '#f4f5f7',
              border: `1px solid ${hasError ? '#f5c6cb' : '#e7e9ec'}`,
              color: hasError ? '#d13212' : '#16191f',
              maxHeight: 400,
            }}>
              {tryPrettyJson(result.payload) || '(empty response)'}
            </pre>
          )}

          {resultTab === 'logs' && (
            <div>
              <div style={{ border: '1px solid #e7e9ec', borderRadius: 6, overflow: 'hidden', marginBottom: '0.75rem' }}>
                <pre style={{ margin: 0, padding: '0.75rem', background: '#1e1e1e', color: '#e0e0e0', overflow: 'auto', maxHeight: 400, fontSize: '0.8em', fontFamily: 'monospace' }}>
                  {logLines.length === 0 ? (
                    <span style={{ color: '#5f6b7a' }}>No log output.</span>
                  ) : (
                    logLines.map((line, i) => (
                      <div key={i} style={colorLogLine(line)}>{line}</div>
                    ))
                  )}
                </pre>
              </div>
              {result.requestId && (
                <button
                  onClick={() => navigate(`${logGroupPath}?requestId=${encodeURIComponent(result.requestId!)}`)}
                  style={{ background: 'none', border: '1px solid #0972d3', color: '#0972d3', borderRadius: 4, padding: '0.35rem 0.8rem', cursor: 'pointer', fontSize: '0.85em' }}
                >
                  View logs for this invocation →
                </button>
              )}
            </div>
          )}

          {resultTab === 'details' && (
            <dl style={{ display: 'grid', gridTemplateColumns: 'max-content 1fr', gap: '0 2rem', fontSize: '0.9em', margin: 0 }}>
              {[
                ['Status code', String(result.statusCode)],
                ['Function error', result.functionError || '—'],
                ['Executed version', result.executedVersion || '—'],
                ['Billed duration', result.billedDurationMs ? `${result.billedDurationMs} ms` : '—'],
                ['Request ID', result.requestId || '—'],
              ].map(([label, value]) => (
                <Fragment key={label}>
                  <dt style={{ color: '#5f6b7a', fontWeight: 500, padding: '0.4rem 0', borderBottom: '1px solid #f4f5f7' }}>{label}</dt>
                  <dd style={{ margin: 0, padding: '0.4rem 0', borderBottom: '1px solid #f4f5f7', wordBreak: 'break-all' }}>{value}</dd>
                </Fragment>
              ))}
            </dl>
          )}
        </div>
      )}
    </div>
  )
}
