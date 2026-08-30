interface EmptyStateProps {
  icon?: React.ReactNode
  title: string
  description?: string
  cta?: string
  onCta?: () => void
}

export function EmptyState({ icon, title, description, cta, onCta }: EmptyStateProps) {
  return (
    <div style={{ textAlign: 'center', padding: '4rem 2rem', color: '#5f6b7a' }}>
      <div style={{ fontSize: '2.5rem', marginBottom: '1rem', opacity: 0.5 }}>
        {icon ?? '☁'}
      </div>
      <h3 style={{ margin: '0 0 0.5rem', color: '#16191f', fontSize: '1.1rem', fontWeight: 600 }}>{title}</h3>
      {description && (
        <p style={{ margin: '0 0 1.5rem', fontSize: '0.9em', maxWidth: 380, marginLeft: 'auto', marginRight: 'auto' }}>
          {description}
        </p>
      )}
      {cta && onCta && (
        <button
          onClick={onCta}
          style={{
            background: '#e77600',
            color: '#fff',
            border: 'none',
            borderRadius: 4,
            padding: '0.5rem 1.25rem',
            fontSize: '0.9em',
            cursor: 'pointer',
            fontWeight: 500,
          }}
        >
          {cta}
        </button>
      )}
    </div>
  )
}
