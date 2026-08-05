{{- define "gcm-md.fullname" -}}
{{ .Release.Name }}
{{- end -}}

{{- define "gcm-md.labels" -}}
app.kubernetes.io/part-of: gcm-md-sequencer-aeron
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "gcm-md.clusterNode.name" -}}
{{ include "gcm-md.fullname" . }}-cluster-node
{{- end -}}

{{- define "gcm-md.natsBridge.name" -}}
{{ include "gcm-md.fullname" . }}-nats-bridge
{{- end -}}

{{- define "gcm-md.lineHandlerTemplate.name" -}}
{{ include "gcm-md.fullname" . }}-line-handler-template
{{- end -}}

{{- define "gcm-md.mockUpstreamSource.name" -}}
{{ include "gcm-md.fullname" . }}-mock-upstream-source
{{- end -}}

{{/*
Design §10: the full N-member Aeron clusterMembers string (ClusterNodeConfig's raw format,
memberId,client:port,member:port,log:port,transfer:port,archive:port groups joined by "|", with
a trailing "|"), computed once from replicas + the headless Service's per-pod DNS scheme
(<clusterNode-name>-<ordinal>.<clusterNode-name>.<namespace>.svc.cluster.local — see
headless-service.yaml's comment). Identical on every pod, so it's safe to render as a plain env
var value rather than something each pod derives for itself.
*/}}
{{- define "gcm-md.clusterNode.membersList" -}}
{{- $name := include "gcm-md.clusterNode.name" . -}}
{{- $ns := .Release.Namespace -}}
{{- $ports := .Values.clusterNode.ports -}}
{{- $members := list -}}
{{- range $i := until (int .Values.clusterNode.replicas) -}}
{{- $host := printf "%s-%d.%s.%s.svc.cluster.local" $name $i $name $ns -}}
{{- $entry := printf "%d,%s:%d,%s:%d,%s:%d,%s:%d,%s:%d" $i $host (int $ports.client) $host (int $ports.member) $host (int $ports.log) $host (int $ports.transfer) $host (int $ports.archive) -}}
{{- $members = append $members $entry -}}
{{- end -}}
{{- join "|" $members -}}|
{{- end -}}

{{/*
Design §7/§10: AeronCluster.Context.ingressEndpoints() format - comma-separated
"memberId=host:port" pairs, one per member, so an IngressTransport client (line-handler-template,
when line-handler.ingress-transport=aeron) can find whichever member is currently leader. This is
a *different* format from membersList above (which is the consensus-module-internal clusterMembers
string) - Aeron's client and server-side membership config are deliberately separate properties
with separate formats. The bare headless-Service DNS name alone is not a substitute for this:
with replicas > 1 it round-robins across every pod's IP, not a specific one, which is not what
AeronCluster's ingress connect logic expects per member entry.
*/}}
{{- define "gcm-md.clusterNode.ingressEndpoints" -}}
{{- $name := include "gcm-md.clusterNode.name" . -}}
{{- $ns := .Release.Namespace -}}
{{- $port := int .Values.clusterNode.ports.client -}}
{{- $entries := list -}}
{{- range $i := until (int .Values.clusterNode.replicas) -}}
{{- $host := printf "%s-%d.%s.%s.svc.cluster.local" $name $i $name $ns -}}
{{- $entries = append $entries (printf "%d=%s:%d" $i $host $port) -}}
{{- end -}}
{{- join "," $entries -}}
{{- end -}}

{{/*
NatsBridgeProperties.Cluster.archiveControlChannels format - comma-separated full Aeron channel
URIs (aeron:udp?endpoint=host:port), one per member, matching each member's
ClusterNodeConfig.archiveControlChannel() exactly (fixed port 9050 = .Values.clusterNode.ports.archive
- see ClusterNodeConfig.java). LeaderArchiveConnector tries each in turn and keeps whichever one
actually has the egress recording, since Archive control connections have no AeronCluster-style
automatic leader-following and only the leader's local archive ever records the egress stream - a
single bare headless-Service DNS name can silently land on a follower's empty archive.
*/}}
{{- define "gcm-md.clusterNode.archiveControlChannels" -}}
{{- $name := include "gcm-md.clusterNode.name" . -}}
{{- $ns := .Release.Namespace -}}
{{- $port := int .Values.clusterNode.ports.archive -}}
{{- $entries := list -}}
{{- range $i := until (int .Values.clusterNode.replicas) -}}
{{- $host := printf "%s-%d.%s.%s.svc.cluster.local" $name $i $name $ns -}}
{{- $entries = append $entries (printf "aeron:udp?endpoint=%s:%d" $host $port) -}}
{{- end -}}
{{- join "," $entries -}}
{{- end -}}
