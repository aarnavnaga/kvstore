{{- define "kvstore.fullname" -}}
{{- printf "%s" .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{- define "kvstore.headless" -}}
{{- printf "%s-peers" (include "kvstore.fullname" .) -}}
{{- end -}}

{{- define "kvstore.labels" -}}
app.kubernetes.io/name: kvstore
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{- define "kvstore.selector" -}}
app.kubernetes.io/name: kvstore
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Stable DNS name of pod i through the headless service. */}}
{{- define "kvstore.podHost" -}}
{{- $root := index . 0 -}}{{- $i := index . 1 -}}
{{- printf "%s-%d.%s.%s.svc.cluster.local" (include "kvstore.fullname" $root) $i (include "kvstore.headless" $root) $root.Release.Namespace -}}
{{- end -}}

{{/* --peers value: 1=<pod-0>:7000,2=<pod-1>:7000,... */}}
{{- define "kvstore.peers" -}}
{{- $root := . -}}
{{- $peers := list -}}
{{- range $i := until (int .Values.replicas) -}}
{{- $peers = append $peers (printf "%d=%s:7000" (add1 $i) (include "kvstore.podHost" (list $root $i))) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end -}}
