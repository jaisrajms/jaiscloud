import { describe, expect, it } from 'vitest'
import type { Project } from '../../api/gcp/resourcemanager'
import { PROJECT_ID_RE, parseKV, shortDate, stateColor, stateLabel, validProjectId } from './util'

function project(overrides: Partial<Project>): Project {
  return { projectId: 'proj', state: 'ACTIVE', ...overrides }
}

describe('shortDate', () => {
  it('renders an em dash when absent', () => {
    expect(shortDate()).toBe('—')
  })

  it('passes through an unparseable value', () => {
    expect(shortDate('not-a-date')).toBe('not-a-date')
  })
})

describe('stateColor', () => {
  it('maps known states', () => {
    expect(stateColor('ACTIVE')).toBe('success')
    expect(stateColor('DELETE_REQUESTED')).toBe('warning')
  })

  it('falls back for an unknown state', () => {
    expect(stateColor('SOMETHING')).toBe('default')
  })
})

describe('stateLabel', () => {
  it('renders the state, or an em dash when absent', () => {
    expect(stateLabel(project({ state: 'DELETE_REQUESTED' }))).toBe('DELETE_REQUESTED')
    expect(stateLabel(project({ state: '' }))).toBe('—')
  })
})

describe('validProjectId', () => {
  it('accepts the documented grammar', () => {
    expect(validProjectId('my-project')).toBe(true)
    expect(validProjectId('abc123')).toBe(true)
    expect(PROJECT_ID_RE.test('project2')).toBe(true)
  })

  it('rejects invalid ids', () => {
    expect(validProjectId('')).toBe(false)
    expect(validProjectId('Bad')).toBe(false)
    expect(validProjectId('1project')).toBe(false)
    expect(validProjectId('trailing-')).toBe(false)
    expect(validProjectId('shrt')).toBe(false)
  })
})

describe('parseKV', () => {
  it('parses key=value lines, ignoring blanks and comments', () => {
    expect(parseKV('env=dev\n\n# note\nowner = platform \n')).toEqual({
      env: 'dev',
      owner: 'platform',
    })
  })

  it('returns undefined when the block is empty', () => {
    expect(parseKV('')).toBeUndefined()
    expect(parseKV('  \n# only a comment\n')).toBeUndefined()
  })
})
