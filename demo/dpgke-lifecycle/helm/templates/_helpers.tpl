{{/*
Naming and labels. The release name is part of each object name, so two installs
in one namespace cannot collide; the ternary keeps the common case (release named
after the chart) free of a doubled prefix.
*/}}
{{- define "dpgke.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dpgke.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "dpgke.configMapName" -}}
{{- printf "%s-config" (include "dpgke.fullname" .) -}}
{{- end -}}

{{/*
The Secret the pod reads. `credentials.existingSecret` names one the reader
created out of band; otherwise the chart renders its own. The template that
creates the Secret uses the same branch, so the two never disagree.
*/}}
{{- define "dpgke.secretName" -}}
{{- if .Values.credentials.existingSecret -}}
{{- .Values.credentials.existingSecret -}}
{{- else -}}
{{- printf "%s-credentials" (include "dpgke.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "dpgke.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dpgke.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "dpgke.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "dpgke.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
