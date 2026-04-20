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
Generate coordinator address for pod 0. Other pods discover the cluster via
the Raft peer list (see gvdb.coordinator.raftPeers); clients and data-nodes
use this single-pod address because today there is only one coordinator
address for RouteQuery/Heartbeat RPCs. When replicas > 1, pod-0 still boots
and becomes reachable; leader election may move elsewhere but the gRPC
service is fronted by the headless service.
*/}}
{{- define "gvdb.coordinator.address" -}}
{{ include "gvdb.fullname" . }}-coordinator-0.{{ include "gvdb.coordinator.serviceName" . }}.{{ .Release.Namespace }}.svc.cluster.local:50051
{{- end }}

{{/*
Resolve the effective ServiceAccount name for each workload. If the user
enabled serviceAccount.create, prefer the explicit name override or fall
back to "<release>-<workload>" so the Pod spec reference matches what the
serviceaccount.yaml template created (roadmap 0b.5).
*/}}
{{- define "gvdb.coordinator.serviceAccountName" -}}
{{- if .Values.coordinator.serviceAccount.create -}}
{{ default (printf "%s-coordinator" (include "gvdb.fullname" .)) .Values.coordinator.serviceAccount.name }}
{{- else -}}
{{ default "default" .Values.coordinator.serviceAccount.name }}
{{- end -}}
{{- end }}

{{- define "gvdb.dataNode.serviceAccountName" -}}
{{- if .Values.dataNode.serviceAccount.create -}}
{{ default (printf "%s-data-node" (include "gvdb.fullname" .)) .Values.dataNode.serviceAccount.name }}
{{- else -}}
{{ default "default" .Values.dataNode.serviceAccount.name }}
{{- end -}}
{{- end }}

{{- define "gvdb.queryNode.serviceAccountName" -}}
{{- if .Values.queryNode.serviceAccount.create -}}
{{ default (printf "%s-query-node" (include "gvdb.fullname" .)) .Values.queryNode.serviceAccount.name }}
{{- else -}}
{{ default "default" .Values.queryNode.serviceAccount.name }}
{{- end -}}
{{- end }}

{{- define "gvdb.proxy.serviceAccountName" -}}
{{- if .Values.proxy.serviceAccount.create -}}
{{ default (printf "%s-proxy" (include "gvdb.fullname" .)) .Values.proxy.serviceAccount.name }}
{{- else -}}
{{ default "default" .Values.proxy.serviceAccount.name }}
{{- end -}}
{{- end }}

{{/*
Soft pod anti-affinity that keeps pods of the same workload on different
nodes when possible. `preferred` over `required` so a single-node dev
cluster (kind) can still schedule all replicas (roadmap 0b.5).
*/}}
{{- define "gvdb.antiAffinity" -}}
podAntiAffinity:
  preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        labelSelector:
          matchLabels:
            {{- .selectorLabels | nindent 12 }}
        topologyKey: kubernetes.io/hostname
{{- end }}

{{/*
Generate the Raft peer list for the coordinator StatefulSet in
"id:host:port" format. Node ids are 1-indexed (ordinal + 1) to match
NuRaft's requirement that server id > 0. Used when coordinator.replicas > 1
to enable HA Raft quorum (roadmap 0b.4).
*/}}
{{- define "gvdb.coordinator.raftPeers" -}}
{{- $fullname := include "gvdb.fullname" . -}}
{{- $serviceName := include "gvdb.coordinator.serviceName" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $replicas := int .Values.coordinator.replicas -}}
{{- range $i := until $replicas -}}
{{- if $i }},{{ end -}}
{{ add $i 1 }}:{{ $fullname }}-coordinator-{{ $i }}.{{ $serviceName }}.{{ $namespace }}.svc.cluster.local:8300
{{- end -}}
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
