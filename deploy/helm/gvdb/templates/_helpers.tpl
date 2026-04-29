{{/*
Full name: release name
*/}}
{{- define "gvdb.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Cluster DNS domain suffix. Defaults to cluster.local; override with
.Values.clusterDomain for clusters using a custom domain like
"cluster.internal".
*/}}
{{- define "gvdb.clusterDomain" -}}
{{ .Values.clusterDomain | default "cluster.local" }}
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
{{ include "gvdb.fullname" . }}-coordinator-0.{{ include "gvdb.coordinator.serviceName" . }}.{{ .Release.Namespace }}.svc.{{ include "gvdb.clusterDomain" . }}:50051
{{- end }}

{{/*
Resolve the effective ServiceAccount name for each workload. If the user
enabled serviceAccount.create, prefer the explicit name override or fall
back to "<release>-<workload>" so the Pod spec reference matches what the
serviceaccount.yaml template created.
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
Cluster-scoped PriorityClass names. Because PriorityClass is cluster
scoped, the name must be unique across all namespaces — not just across
releases. We embed the namespace into the generated name so two installs
with the same release name in different namespaces don't collide.
*/}}
{{- define "gvdb.priorityClass.name.coordinator" -}}
{{ include "gvdb.fullname" . }}-{{ .Release.Namespace }}-coordinator
{{- end }}

{{- define "gvdb.priorityClass.name.dataNode" -}}
{{ include "gvdb.fullname" . }}-{{ .Release.Namespace }}-data-node
{{- end }}

{{- define "gvdb.priorityClass.name.queryNode" -}}
{{ include "gvdb.fullname" . }}-{{ .Release.Namespace }}-query-node
{{- end }}

{{- define "gvdb.priorityClass.name.proxy" -}}
{{ include "gvdb.fullname" . }}-{{ .Release.Namespace }}-proxy
{{- end }}

{{/*
Resolve priorityClassName for each workload. Explicit value from values
wins. Otherwise, when priorityClasses.create=true we auto-wire the chart-
managed PriorityClass name so users don't have to duplicate the string.
Returns empty string when neither applies.
*/}}
{{- define "gvdb.coordinator.priorityClassName" -}}
{{- if .Values.coordinator.priorityClassName -}}
{{ .Values.coordinator.priorityClassName }}
{{- else if .Values.priorityClasses.create -}}
{{ include "gvdb.priorityClass.name.coordinator" . }}
{{- end -}}
{{- end }}

{{- define "gvdb.dataNode.priorityClassName" -}}
{{- if .Values.dataNode.priorityClassName -}}
{{ .Values.dataNode.priorityClassName }}
{{- else if .Values.priorityClasses.create -}}
{{ include "gvdb.priorityClass.name.dataNode" . }}
{{- end -}}
{{- end }}

{{- define "gvdb.queryNode.priorityClassName" -}}
{{- if .Values.queryNode.priorityClassName -}}
{{ .Values.queryNode.priorityClassName }}
{{- else if .Values.priorityClasses.create -}}
{{ include "gvdb.priorityClass.name.queryNode" . }}
{{- end -}}
{{- end }}

{{- define "gvdb.proxy.priorityClassName" -}}
{{- if .Values.proxy.priorityClassName -}}
{{ .Values.proxy.priorityClassName }}
{{- else if .Values.priorityClasses.create -}}
{{ include "gvdb.priorityClass.name.proxy" . }}
{{- end -}}
{{- end }}

{{/*
Pod anti-affinity helper. Caller passes:
  - selectorLabels (string): rendered pod selector labels for the workload
  - type ("preferred" | "required"): scheduling hardness
  - topologyKey: topology domain (e.g. kubernetes.io/hostname or
    topology.kubernetes.io/zone)
`preferred` keeps kind-style single-node dev clusters schedulable; use
`required` + zone topology in production across AZs.
*/}}
{{- define "gvdb.antiAffinity" -}}
{{- $type := .type | default "preferred" -}}
{{- $topologyKey := .topologyKey | default "kubernetes.io/hostname" -}}
podAntiAffinity:
  {{- if eq $type "required" }}
  requiredDuringSchedulingIgnoredDuringExecution:
    - labelSelector:
        matchLabels:
          {{- .selectorLabels | nindent 10 }}
      topologyKey: {{ $topologyKey }}
  {{- else }}
  preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        labelSelector:
          matchLabels:
            {{- .selectorLabels | nindent 12 }}
        topologyKey: {{ $topologyKey }}
  {{- end }}
{{- end }}

{{/*
Generate the Raft peer list for the coordinator StatefulSet in
"id:host:port" format. Node ids are 1-indexed (ordinal + 1) to match
NuRaft's requirement that server id > 0. Used when coordinator.replicas > 1
to enable HA Raft quorum.
*/}}
{{- define "gvdb.coordinator.raftPeers" -}}
{{- $fullname := include "gvdb.fullname" . -}}
{{- $serviceName := include "gvdb.coordinator.serviceName" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $clusterDomain := include "gvdb.clusterDomain" . -}}
{{- $replicas := int .Values.coordinator.replicas -}}
{{- range $i := until $replicas -}}
{{- if $i }},{{ end -}}
{{ add $i 1 }}:{{ $fullname }}-coordinator-{{ $i }}.{{ $serviceName }}.{{ $namespace }}.svc.{{ $clusterDomain }}:8300
{{- end -}}
{{- end }}

{{/*
Generate a gRPC dns:/// URI for the query-node headless service. The proxy
dials this single target and gRPC's built-in round_robin LB distributes
requests across all Ready pods, re-resolving DNS on failures — so
`kubectl scale query-node` is picked up without helm upgrade.

Note: the previous `gvdb.dataNode.addresses` / `gvdb.queryNode.addresses`
helpers (comma-separated static pod-FQDN lists) were removed when the
proxy switched to DNS-based discovery. The `--data-nodes` flag no longer
exists; data-node routing goes entirely through the coordinator's
RouteQuery RPC.
*/}}
{{- define "gvdb.queryNode.dnsUri" -}}
{{- $serviceName := include "gvdb.queryNode.serviceName" . -}}
{{- $namespace := .Release.Namespace -}}
{{- $clusterDomain := include "gvdb.clusterDomain" . -}}
dns:///{{ $serviceName }}.{{ $namespace }}.svc.{{ $clusterDomain }}:50070
{{- end }}

{{/*
Topology zone-spread helper. Emits a single topologySpreadConstraints
entry keyed on topology.kubernetes.io/zone so StatefulSet replicas land
in different AZs (data safety) and the proxy spreads for availability.
Caller passes:
  - selectorLabels (string): rendered pod selector labels for the workload
  - whenUnsatisfiable: "DoNotSchedule" (StatefulSets) or "ScheduleAnyway"
    (proxy / stateless workloads)
  - maxSkew (int, default 1)
The user-provided <workload>.topologySpreadConstraints list is appended
verbatim by the caller after this block, so explicit overrides win
without losing the zone-spread default.
*/}}
{{- define "gvdb.zoneSpread" -}}
{{- $maxSkew := .maxSkew | default 1 -}}
- maxSkew: {{ $maxSkew }}
  topologyKey: topology.kubernetes.io/zone
  whenUnsatisfiable: {{ .whenUnsatisfiable }}
  labelSelector:
    matchLabels:
      {{- .selectorLabels | nindent 6 }}
{{- end }}

