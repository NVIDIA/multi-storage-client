{{/* Resolved chart name. */}}
{{- define "msfs-csi.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully-qualified application name (e.g. "release-name-msfs-csi"). Truncated to
63 chars to fit DNS-1123 limits and lowercased to satisfy K8s name rules.
*/}}
{{- define "msfs-csi.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Standard labels applied to every rendered object. */}}
{{- define "msfs-csi.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "msfs-csi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* Selector labels for the node DaemonSet. */}}
{{- define "msfs-csi.selectorLabels" -}}
app.kubernetes.io/name: {{ include "msfs-csi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: node-plugin
{{- end -}}

{{/* ServiceAccount name (honors create=false). */}}
{{- define "msfs-csi.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (printf "%s-node" (include "msfs-csi.fullname" .)) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Effective image reference (image.tag falls back to .Chart.AppVersion). */}}
{{- define "msfs-csi.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
