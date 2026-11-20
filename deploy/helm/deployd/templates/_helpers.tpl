{{- define "deployd.labels" -}}
app.kubernetes.io/name: deployd
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{- define "deployd.selector" -}}
app.kubernetes.io/name: deployd
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
