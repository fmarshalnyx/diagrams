package gcm.md.sequencer.cluster;

import gcm.md.sequencer.stamping.StampingConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Everything {@link ClusterNodeLauncher} needs to embed a single cluster member (design §5.2).
 * Hosts building a real deployment supply their own instance, resolved from Kubernetes headless
 * service DNS names (design §10) rather than the hardcoded loopback addresses
 * {@link #localSingleMember()} uses for local dev.
 *
 * @param clusterMemberId          this member's id (0-based) within {@code clusterMembers}.
 * @param clusterMembers           Aeron's raw member-list format: one
 *                                 {@code memberId,clientEndpoint,memberEndpoint,logEndpoint,transferEndpoint,archiveEndpoint}
 *                                 group per member, separated by {@code |}.
 * @param aeronDirectoryName       the embedded media driver's {@code aeron.dir} (design §5.2: the
 *                                 mounted {@code /dev/shm} tmpfs in a real deployment).
 * @param clusterDirectoryName     directory holding this member's consensus module / Raft log state.
 * @param archiveDirectoryName     directory holding this member's recorded cluster log + egress stream (design §5.2).
 * @param ingressChannel           channel *template* (no endpoint) for client ingress — the
 *                                 actual per-member bind endpoint comes from each member's own
 *                                 {@code clientEndpoint} field in {@code clusterMembers}; an
 *                                 explicit endpoint here conflicts with that derivation.
 * @param logChannel               channel *template* (no endpoint) for log replication — same
 *                                 reasoning as {@code ingressChannel}, derived from each member's
 *                                 {@code logEndpoint} field instead.
 * @param replicationChannel       the channel used for archive-to-archive replication (member catch-up).
 * @param archiveControlChannel    the archive's control (request) channel.
 * @param archiveControlResponseChannel the archive client's control-response channel.
 * @param stamping                 the {@link StampingEngine} configuration (design §4).
 * @param sources                  configured ingress source principals (design §5.1, §7).
 * @param heartbeatIntervalNanos   sequencer heartbeat emission interval, in cluster-clock nanos.
 */
public record ClusterNodeConfig(
        int clusterMemberId,
        String clusterMembers,
        String aeronDirectoryName,
        String clusterDirectoryName,
        String archiveDirectoryName,
        String ingressChannel,
        String logChannel,
        String replicationChannel,
        String archiveControlChannel,
        String archiveControlResponseChannel,
        StampingConfig stamping,
        List<SourcePrincipal> sources,
        long heartbeatIntervalNanos) {

    /**
     * A single-member cluster on loopback addresses (design §5.2: "local profile allows 1 — a
     * single-member cluster is valid Raft and keeps local dev light"). Not for any shared
     * environment — directories and ports collide across concurrent runs by design, since local
     * dev only ever runs one instance at a time.
     */
    public static ClusterNodeConfig localSingleMember() {
        return singleMember("localhost", System.getProperty("java.io.tmpdir") + "/gcm-md-cluster-node", List.of());
    }

    /**
     * A single-member cluster addressable at {@code host} (design §5.2/§10: in Kubernetes this is
     * the pod's own headless-service DNS name, e.g.
     * {@code gcm-md-cluster-node-0.gcm-md-cluster-node.<namespace>.svc.cluster.local} — a real
     * pod DNS name, unlike {@code localhost}, is reachable from other pods, which is what lets
     * {@code nats-bridge} and any Aeron-transport line handler actually connect cross-pod). Port
     * numbers are fixed (matching {@link AeronEgressConfig#forHost}'s egress port and this
     * class's own defaults), since one member per pod means no port collision risk.
     */
    public static ClusterNodeConfig singleMember(String host, String dataDir, List<SourcePrincipal> sources) {
        return new ClusterNodeConfig(
                0,
                "0," + host + ":9010," + host + ":9020," + host + ":9030," + host + ":9040," + host + ":9050|",
                dataDir + "/aeron",
                dataDir + "/cluster",
                dataDir + "/archive",
                "aeron:udp?term-length=64k",
                "aeron:udp?term-length=64k",
                "aeron:udp?endpoint=" + host + ":9060",
                "aeron:udp?endpoint=" + host + ":9050",
                "aeron:udp?endpoint=" + host + ":9051",
                StampingConfig.v4Defaults(),
                sources,
                TimeUnit.MILLISECONDS.toNanos(100));
    }

    /**
     * A member of a real Kubernetes-deployed N-member cluster (design §5.2/§10): {@code
     * clusterMemberId} and {@code host} are this pod's own identity (derived by {@link
     * ClusterNodeLauncher} from {@code POD_NAME}'s ordinal and {@code CLUSTER_NODE_HOST});
     * {@code clusterMembers} is the full pipe-joined membership string for every member,
     * identical across all pods, computed once by the Helm StatefulSet template from {@code
     * replicas} and the headless-service DNS scheme — this class has no way to enumerate its
     * peers on its own. Same fixed port scheme and endpoint-less ingress/log templates as
     * {@link #singleMember}; this factory is that one generalized to a caller-supplied {@code
     * clusterMemberId} and membership view instead of always being member 0 of a 1-member view.
     */
    public static ClusterNodeConfig kubernetesMember(int clusterMemberId, String clusterMembers,
                                                       String host, String dataDir,
                                                       List<SourcePrincipal> sources) {
        return new ClusterNodeConfig(
                clusterMemberId,
                clusterMembers,
                dataDir + "/aeron",
                dataDir + "/cluster",
                dataDir + "/archive",
                "aeron:udp?term-length=64k",
                "aeron:udp?term-length=64k",
                "aeron:udp?endpoint=" + host + ":9060",
                "aeron:udp?endpoint=" + host + ":9050",
                "aeron:udp?endpoint=" + host + ":9051",
                StampingConfig.v4Defaults(),
                sources,
                TimeUnit.MILLISECONDS.toNanos(100));
    }

    /**
     * {@code memberCount} configs for an in-process multi-member cluster, all on {@code
     * localhost} with distinct port ranges per member (base {@code 9010 + memberIndex * 100}) —
     * built for {@code services/sequencer-aeron/integration-tests}' in-process harness (design
     * §12.2), which runs every member as a separate embedded {@code ClusteredMediaDriver} in the
     * same JVM. Not a Kubernetes construct: real multi-member Kubernetes deployment (N replicas,
     * one per pod, addressed by headless-service DNS) is a documented gap — see
     * {@link ClusterNodeLauncher}'s Javadoc — this factory only serves the local test harness,
     * where distinct ports substitute for the pod-per-member isolation Kubernetes would provide.
     */
    public static List<ClusterNodeConfig> localMultiMember(int memberCount, String baseDataDir,
                                                             List<SourcePrincipal> sources) {
        StringBuilder membersBuilder = new StringBuilder();
        for (int i = 0; i < memberCount; i++) {
            int base = 9010 + i * 100;
            membersBuilder.append(i).append(",localhost:").append(base)
                    .append(",localhost:").append(base + 10)
                    .append(",localhost:").append(base + 20)
                    .append(",localhost:").append(base + 30)
                    .append(",localhost:").append(base + 40)
                    .append('|');
        }
        String clusterMembers = membersBuilder.toString();

        List<ClusterNodeConfig> configs = new ArrayList<>();
        for (int i = 0; i < memberCount; i++) {
            int base = 9010 + i * 100;
            String dataDir = baseDataDir + "/member" + i;
            configs.add(new ClusterNodeConfig(
                    i,
                    clusterMembers,
                    dataDir + "/aeron",
                    dataDir + "/cluster",
                    dataDir + "/archive",
                    "aeron:udp?term-length=64k",
                    "aeron:udp?term-length=64k",
                    "aeron:udp?endpoint=localhost:" + (base + 60),
                    "aeron:udp?endpoint=localhost:" + (base + 40),
                    "aeron:udp?endpoint=localhost:" + (base + 41),
                    StampingConfig.v4Defaults(),
                    sources,
                    TimeUnit.MILLISECONDS.toNanos(100)));
        }
        return configs;
    }
}
