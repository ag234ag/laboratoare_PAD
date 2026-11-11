{{- define "laborator.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "laborator.labels" -}}
app.kubernetes.io/name: {{ include "laborator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "laborator.houseIdsJson" -}}
{{- $ids := list -}}
{{- range .Values.dataHouses -}}
{{- $ids = append $ids .id -}}
{{- end -}}
{{- toJson $ids -}}
{{- end -}}
