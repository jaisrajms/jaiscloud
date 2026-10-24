import type { ReactElement } from 'react'
import { Tab, Tabs } from '@mui/material'

export interface GcpTabItem {
  label: string | ReactElement
  value: string | number
  disabled?: boolean
  icon?: ReactElement
}

export interface GcpTabsProps {
  tabs: GcpTabItem[]
  value: string | number
  onChange: (value: string | number) => void
  'aria-label'?: string
  variant?: 'standard' | 'scrollable' | 'fullWidth'
}

/** Underlined tab bar matching the console's per-page sub-navigation. */
export function GcpTabs({
  tabs,
  value,
  onChange,
  'aria-label': ariaLabel,
  variant = 'scrollable',
}: GcpTabsProps) {
  return (
    <Tabs
      value={value}
      onChange={(_event, next: string | number) => onChange(next)}
      variant={variant}
      scrollButtons="auto"
      aria-label={ariaLabel}
      sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}
    >
      {tabs.map((tab) => (
        <Tab
          key={tab.value}
          label={tab.label}
          value={tab.value}
          disabled={tab.disabled}
          icon={tab.icon}
        />
      ))}
    </Tabs>
  )
}
