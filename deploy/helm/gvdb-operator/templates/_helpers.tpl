{{/*
Release-derived name (truncated to 63 chars for DNS-1123 labels).
*/}}
{{- define "gvdb-operator.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Standard labels applied to every operator-owned object.
*/}}
{{- define "gvdb-operator.labels" -}}
app.kubernetes.io/name: gvdb-operator
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/*
Selector labels used by the operator Deployment.
*/}}
{{- define "gvdb-operator.selectorLabels" -}}
app.kubernetes.io/name: gvdb-operator
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Image reference. Tag falls back to chart appVersion.
*/}}
{{- define "gvdb-operator.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}
