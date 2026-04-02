{{/*
Full name: release name
*/}}
{{- define "gvdb.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Container image with tag (falls back to appVersion)
*/}}
{{- define "gvdb.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "gvdb.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: gvdb
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{/*
Coordinator labels
*/}}
{{- define "gvdb.coordinator.labels" -}}
{{ include "gvdb.labels" . }}
app: {{ include "gvdb.fullname" . }}-coordinator
{{- end }}

{{- define "gvdb.coordinator.selectorLabels" -}}
app: {{ include "gvdb.fullname" . }}-coordinator
{{- end }}

{{/*
Data node labels
*/}}
{{- define "gvdb.dataNode.labels" -}}
{{ include "gvdb.labels" . }}
app: {{ include "gvdb.fullname" . }}-data-node
{{- end }}

{{- define "gvdb.dataNode.selectorLabels" -}}
app: {{ include "gvdb.fullname" . }}-data-node
{{- end }}

{{/*
Query node labels
*/}}
{{- define "gvdb.queryNode.labels" -}}
{{ include "gvdb.labels" . }}
app: {{ include "gvdb.fullname" . }}-query-node
{{- end }}

{{- define "gvdb.queryNode.selectorLabels" -}}
app: {{ include "gvdb.fullname" . }}-query-node
{{- end }}

{{/*
Proxy labels
*/}}
{{- define "gvdb.proxy.labels" -}}
{{ include "gvdb.labels" . }}
app: {{ include "gvdb.fullname" . }}-proxy
{{- end }}

{{- define "gvdb.proxy.selectorLabels" -}}
app: {{ include "gvdb.fullname" . }}-proxy
{{- end }}

{{/*
Coordinator headless service FQDN
*/}}
{{- define "gvdb.coordinator.serviceName" -}}
{{ include "gvdb.fullname" . }}-coordinator
{{- end }}

{{/*
Data node headless service FQDN
*/}}
{{- define "gvdb.dataNode.serviceName" -}}
{{ include "gvdb.fullname" . }}-data-node
{{- end }}

{{/*
Query node headless service FQDN
*/}}
{{- define "gvdb.queryNode.serviceName" -}}
{{ include "gvdb.fullname" . }}-query-node
{{- end }}

{{/*
Generate coordinator address for pod 0
*/}}
{{- define "gvdb.coordinator.address" -}}
{{ include "gvdb.fullname" . }}-coordinator-0.{{ include "gvdb.coordinator.serviceName" . }}.{{ .Release.Namespace }}.svc.cluster.local:50051
{{- end }}

{{/*
Generate comma-separated data node addresses from replica count
*/}}
{{- define "gvdb.dataNode.addresses" -}}
{{- $fullname := include "gvdb.fullname" . -}}
{{- $serviceName := include "gvdb.dataNode.serviceName" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $replicas := int .Values.dataNode.replicas -}}
{{- range $i := until $replicas -}}
{{- if $i }},{{ end -}}
{{ $fullname }}-data-node-{{ $i }}.{{ $serviceName }}.{{ $namespace }}.svc.cluster.local:50060
{{- end -}}
{{- end }}

{{/*
Generate comma-separated query node addresses from replica count
*/}}
{{- define "gvdb.queryNode.addresses" -}}
{{- $fullname := include "gvdb.fullname" . -}}
{{- $serviceName := include "gvdb.queryNode.serviceName" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $replicas := int .Values.queryNode.replicas -}}
{{- range $i := until $replicas -}}
{{- if $i }},{{ end -}}
{{ $fullname }}-query-node-{{ $i }}.{{ $serviceName }}.{{ $namespace }}.svc.cluster.local:50070
{{- end -}}
{{- end }}
