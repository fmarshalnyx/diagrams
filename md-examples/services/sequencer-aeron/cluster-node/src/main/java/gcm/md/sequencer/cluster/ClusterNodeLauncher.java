package gcm.md.sequencer.cluster;

import gcm.md.sequencer.cluster.metrics.AeronCountersExporter;
import gcm.md.sequencer.cluster.metrics.ClusterMetrics;
import gcm.md.sequencer.cluster.metrics.MetricsHttpServer;
import gcm.md.sequencer.stamping.EngineListener;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.NanosecondClusterClock;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Wires one cluster member (design §5): an embedded {@link ClusteredMediaDriver} (media driver +
 * Archive + consensus module) plus the {@link ClusteredServiceContainer} hosting
 * {@link SequencerClusteredService}, in a single JVM. Single-JVM embedded is the phase-2
 * baseline (design §5); a standalone/C media driver is a documented colo-path upgrade behind a
 * launcher abstraction (design §14), not built now.
 *
 * <p>This launcher's {@code main} wires the real {@link AeronEgressPublisher} (Milestone 3) and a
 * single {@link ClusterMetrics} bean (design §17, Milestone 10) as both the
 * {@link EngineListener}/{@link EgressListener}/{@link ClusterServiceListener} every component
 * reports through, plus {@link AeronCountersExporter} (Aeron's own driver/archive/consensus-module
 * counters) and {@link MetricsHttpServer} (the {@code /metrics} scrape endpoint) — see
 * {@link #main}.
 *
 * <p><b>Environment variables</b> (design §10):
 * <ul>
 *   <li>{@code CLUSTER_NODE_HOST} — this pod's addressable DNS name (e.g. the headless-service
 *       FQDN). Defaults to {@code localhost} (single-process local dev, unreachable cross-pod).</li>
 *   <li>{@code CLUSTER_DATA_DIR} — base directory for aeron/cluster/archive subdirectories.
 *       Defaults to a {@code java.io.tmpdir} path.</li>
 *   <li>{@code CLUSTER_SOURCES} — {@code name:sourceId:credential} triples separated by
 *       {@code ;} (design §5.1). Defaults to none configured (every ingress session rejected).</li>
 *   <li>{@code CLUSTER_METRICS_PORT} — port {@link MetricsHttpServer} binds {@code /metrics} on.
 *       Defaults to {@code 9100}.</li>
 *   <li>{@code CLUSTER_MEMBERS} — the full N-member Aeron membership string (design §10:
 *       rendered once by the Helm StatefulSet template from {@code replicas} and the
 *       headless-service DNS scheme, identical on every pod). When present, this member's own
 *       {@code clusterMemberId} is parsed from {@code POD_NAME}'s StatefulSet ordinal suffix
 *       (see {@link #parsePodOrdinal}) and {@link ClusterNodeConfig#kubernetesMember} is used
 *       instead of {@link ClusterNodeConfig#singleMember}. Absent (the default) reproduces
 *       today's single-member behavior exactly — this is a strictly additive branch.</li>
 *   <li>{@code CLUSTER_MTU_LENGTH} - the embedded media driver's MTU, in bytes (design section
 *       5.2: externalized per environment - 1408 for Docker Desktop's default bridge MTU, 8k for
 *       AWS jumbo frames). Defaults to {@link #DEFAULT_MTU_LENGTH}.</li>
 *   <li>{@code CLUSTER_DRIVER_TIMEOUT_MS} - how long the embedded media driver's client and the
 *       driver itself tolerate not hearing from each other before assuming the other died
 *       ({@code Aeron.Context}/{@code MediaDriver.Context}'s {@code driverTimeoutMs}, plus the
 *       driver's own {@code clientLivenessTimeoutNs}, kept in lock-step here). Defaults to
 *       {@link #DEFAULT_DRIVER_TIMEOUT_MS}, Aeron's own out-of-the-box default (10s) — real
 *       incident, not hypothetical: on a resource-constrained shared host (Docker Desktop, this
 *       repo's own local dev target), a GC pause or scheduling delay past that 10s window makes
 *       the client conclude its own co-located driver died, which is a genuine, if narrow, Aeron
 *       failure mode - the consensus module's default reaction is a clean, silent, exit-0 shutdown
 *       (its default {@code terminationHook} just signals the shutdown barrier {@link #main}
 *       blocks on; nothing here previously logged *why*, which is what made this so hard to
 *       diagnose - see the {@code terminationHook} override in {@link #launchNonBlocking} for the
 *       fix to the silence, independent of this timeout). Bump this per-environment
 *       (`environments/local/values.yaml`) rather than raising the shared default, which would
 *       slow down genuine-failure detection everywhere, including production.</li>
 * </ul>
 */
public final class ClusterNodeLauncher {

    /** Aeron's own documented MTU default, and Docker Desktop's typical bridge MTU (design section 5.2). */
    static final int DEFAULT_MTU_LENGTH = 1408;

    /** Aeron's own out-of-the-box default for both {@code driverTimeoutMs} and (in nanos) {@code clientLivenessTimeoutNs}. */
    static final long DEFAULT_DRIVER_TIMEOUT_MS = 10_000;

    private ClusterNodeLauncher() {
    }

    public static void main(String[] args) throws Exception {
        // Must happen before any Aeron class is touched (including ones imported above, whose
        // static initializers read these) - both properties are consulted exactly once, at class
        // load, to compute the JVM-wide default for every Aeron/Archive/ConsensusModule Context
        // that doesn't set its own value explicitly (most of the ones this launcher creates
        // don't, since only MediaDriver.Context and AeronArchive.Context expose a direct setter -
        // ConsensusModule.Context and Archive.Context have no equivalent method at all, so this
        // system-property route is the only way to reach their own auto-created internal Aeron
        // clients). See CLUSTER_DRIVER_TIMEOUT_MS's Javadoc above for why this exists.
        long driverTimeoutMs = Long.parseLong(System.getenv().getOrDefault("CLUSTER_DRIVER_TIMEOUT_MS",
                String.valueOf(DEFAULT_DRIVER_TIMEOUT_MS)));
        System.setProperty("aeron.driver.timeout", String.valueOf(driverTimeoutMs));
        System.setProperty("aeron.client.liveness.timeout", driverTimeoutMs + "ms");

        String host = System.getenv().getOrDefault("CLUSTER_NODE_HOST", "localhost");
        String dataDir = System.getenv().getOrDefault("CLUSTER_DATA_DIR",
                System.getProperty("java.io.tmpdir") + "/gcm-md-cluster-node");
        List<SourcePrincipal> sources = parseSources(System.getenv("CLUSTER_SOURCES"));
        int metricsPort = Integer.parseInt(System.getenv().getOrDefault("CLUSTER_METRICS_PORT", "9100"));
        int mtuLength = Integer.parseInt(System.getenv().getOrDefault("CLUSTER_MTU_LENGTH",
                String.valueOf(DEFAULT_MTU_LENGTH)));

        String clusterMembers = System.getenv("CLUSTER_MEMBERS");
        ClusterNodeConfig config = (clusterMembers != null && !clusterMembers.isBlank())
                ? ClusterNodeConfig.kubernetesMember(parsePodOrdinal(System.getenv("POD_NAME")),
                        clusterMembers, host, dataDir, sources)
                : ClusterNodeConfig.singleMember(host, dataDir, sources);

        PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        ClusterMetrics clusterMetrics = new ClusterMetrics(meterRegistry, sources);

        AeronArchive.Context egressArchiveClientCtx = new AeronArchive.Context()
                .controlRequestChannel(config.archiveControlChannel())
                .controlResponseChannel(config.archiveControlResponseChannel())
                .messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(driverTimeoutMs));
        EgressPublisher egressPublisher = new AeronEgressPublisher(AeronEgressConfig.forHost(host),
                egressArchiveClientCtx, clusterMetrics);

        try (ClusterMemberHandle member = launchNonBlocking(config, egressPublisher, clusterMetrics, clusterMetrics,
                mtuLength, driverTimeoutMs)) {
            new AeronCountersExporter(meterRegistry, member.countersReader());
            try (MetricsHttpServer metricsHttpServer = new MetricsHttpServer(meterRegistry, metricsPort)) {
                new ShutdownSignalBarrier().await();
            }
        }
    }

    /**
     * Parses a StatefulSet pod's ordinal from its {@code POD_NAME} (standard {@code
     * <name>-<ordinal>} pattern, e.g. {@code gcm-md-seq-cluster-node-1} → {@code 1}) — this is
     * the only per-pod value the Helm StatefulSet template can't inject directly, since every
     * replica shares one {@code PodTemplateSpec}. Fails fast on a null/malformed name rather than
     * defaulting: two pods silently both claiming member 0 is a far worse failure mode than
     * crashing at startup.
     */
    static int parsePodOrdinal(String podName) {
        if (podName == null || podName.isBlank()) {
            throw new IllegalStateException("CLUSTER_MEMBERS is set but POD_NAME is missing — "
                    + "cannot determine this pod's clusterMemberId");
        }
        int lastDash = podName.lastIndexOf('-');
        if (lastDash < 0 || lastDash == podName.length() - 1) {
            throw new IllegalStateException("POD_NAME '" + podName + "' does not match the expected "
                    + "StatefulSet <name>-<ordinal> pattern");
        }
        try {
            return Integer.parseInt(podName.substring(lastDash + 1));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("POD_NAME '" + podName + "' does not end in a numeric "
                    + "StatefulSet ordinal", e);
        }
    }

    private static List<SourcePrincipal> parseSources(String csv) {
        List<SourcePrincipal> sources = new ArrayList<>();
        if (csv == null || csv.isBlank()) {
            return sources;
        }
        for (String entry : csv.split(";")) {
            String[] parts = entry.split(":", 3);
            if (parts.length != 3) {
                throw new IllegalArgumentException("Malformed CLUSTER_SOURCES entry (want name:sourceId:credential): " + entry);
            }
            sources.add(new SourcePrincipal(parts[0], Long.parseLong(parts[1]), parts[2]));
        }
        return sources;
    }

    /** Launches one member and blocks the calling thread until a shutdown signal (SIGINT/SIGTERM) arrives. */
    public static void launch(ClusterNodeConfig config, EgressPublisher egressPublisher, EngineListener engineListener,
                               ClusterServiceListener serviceListener) {
        try (ClusterMemberHandle member = launchNonBlocking(config, egressPublisher, engineListener, serviceListener)) {
            new ShutdownSignalBarrier().await();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to run cluster member", e);
        }
    }

    /**
     * Launches one member without blocking; the returned handle's {@code close()} shuts the
     * member down (media driver, Archive, consensus module, and the clustered service container,
     * in that order), and {@code countersReader()} exposes the embedded media driver's Aeron
     * counters for {@link AeronCountersExporter} (design §17). Used by {@link #launch} for the
     * normal {@code main()} entry point, and directly by {@code integration-tests}' in-process
     * multi-member harness (design §12.2) to start/stop individual members programmatically —
     * e.g. to simulate a leader failing.
     *
     * <p>Closing this handle is a <em>graceful</em> shutdown, not a {@code kill -9}: it cannot
     * reproduce the ungraceful-death failure mode design §12.2's "leader-kill" scenario really
     * wants (that needs a real process kill, i.e. {@code kubectl delete pod} in the Kubernetes
     * environment — see {@code 50-failover-drill.sh}). It's still useful for proving failover
     * produces correct state under a clean stop, just not the whole failure-mode space.
     */
    public static ClusterMemberHandle launchNonBlocking(ClusterNodeConfig config, EgressPublisher egressPublisher,
                                                          EngineListener engineListener,
                                                          ClusterServiceListener serviceListener) {
        return launchNonBlocking(config, egressPublisher, engineListener, serviceListener, DEFAULT_MTU_LENGTH,
                DEFAULT_DRIVER_TIMEOUT_MS);
    }

    /**
     * Same as the 4-arg {@link #launchNonBlocking(ClusterNodeConfig, EgressPublisher,
     * EngineListener, ClusterServiceListener)}, with an explicit media-driver MTU (design §5.2:
     * externalized per environment via {@code CLUSTER_MTU_LENGTH} - none of the channel URIs in
     * {@link ClusterNodeConfig} set an inline {@code mtu=} parameter, so this driver-context
     * default applies uniformly to ingress/log/replication/archive-control/egress traffic).
     * {@link #DEFAULT_DRIVER_TIMEOUT_MS} is used for the driver-timeout knob — see the 6-arg
     * overload for callers (currently just {@link #main}) that need that configurable too.
     */
    public static ClusterMemberHandle launchNonBlocking(ClusterNodeConfig config, EgressPublisher egressPublisher,
                                                          EngineListener engineListener,
                                                          ClusterServiceListener serviceListener,
                                                          int mtuLength) {
        return launchNonBlocking(config, egressPublisher, engineListener, serviceListener, mtuLength,
                DEFAULT_DRIVER_TIMEOUT_MS);
    }

    /**
     * Same as the 5-arg {@link #launchNonBlocking(ClusterNodeConfig, EgressPublisher,
     * EngineListener, ClusterServiceListener, int)}, with an explicit driver timeout (design
     * section 5.2, {@code CLUSTER_DRIVER_TIMEOUT_MS} - see {@link #main}'s Javadoc for why this
     * exists: Aeron's own 10s default is too tight for a resource-constrained host, where a GC
     * pause or scheduling delay past that window makes the client conclude its co-located driver
     * died). Applied to every context here that has its own {@code driverTimeoutMs} — the
     * consensus module, container, and archive all run their own Aeron client connections to the
     * shared embedded driver, each independently subject to this same failure mode, not just the
     * top-level one — plus the driver's own (reverse-direction) {@code clientLivenessTimeoutNs},
     * kept in lock-step with this same value rather than introducing a second configurable knob
     * for what is, from an operator's perspective, one "how patient should Aeron be" setting.
     *
     * <p>The consensus module's {@code terminationHook} is also overridden here (Aeron's own
     * default just signals the shutdown barrier {@link #main} blocks on, silently) to log first -
     * found this because 97 restarts over 25 hours produced zero application-level log evidence
     * of why, across every existing log statement in this codebase; regardless of whether the
     * timeout above actually eliminates the underlying cause, this makes any future recurrence
     * immediately diagnosable instead of a silent mystery.
     */
    public static ClusterMemberHandle launchNonBlocking(ClusterNodeConfig config, EgressPublisher egressPublisher,
                                                          EngineListener engineListener,
                                                          ClusterServiceListener serviceListener,
                                                          int mtuLength, long driverTimeoutMs) {
        // publicationUnblockTimeoutNs must be strictly greater than clientLivenessTimeoutNs
        // (Aeron's own Configuration.validateUnblockTimeout enforces this at MediaDriver.Context
        // conclude() and refuses to start otherwise) - Aeron's stock defaults (15s/10s) already
        // satisfy this, but raising clientLivenessTimeoutNs alone without also raising this one
        // breaks the invariant once it exceeds the 15s default. Doubling driverTimeoutMs keeps
        // the same ratio as Aeron's own defaults instead of picking an arbitrary margin.
        MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
                .aeronDirectoryName(config.aeronDirectoryName())
                .threadingMode(ThreadingMode.SHARED)
                .mtuLength(mtuLength)
                .driverTimeoutMs(driverTimeoutMs)
                .clientLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(driverTimeoutMs))
                .publicationUnblockTimeoutNs(TimeUnit.MILLISECONDS.toNanos(driverTimeoutMs * 2))
                .dirDeleteOnStart(true);

        // The consensus module and the clustered service container are co-located with the
        // Archive in this same JVM/process — Aeron requires their own connection to it to be
        // IPC, not UDP ("local archive control must be IPC"). config.archiveControlChannel()
        // (UDP) is for the Archive's own *external*-facing listen channel below, which is what
        // remote clients like nats-bridge actually connect to.
        AeronArchive.Context archiveClientCtx = new AeronArchive.Context()
                .controlRequestChannel("aeron:ipc")
                .controlResponseChannel("aeron:ipc")
                .messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(driverTimeoutMs));

        Archive.Context archiveCtx = new Archive.Context()
                .aeronDirectoryName(config.aeronDirectoryName())
                .archiveDir(new File(config.archiveDirectoryName()))
                .controlChannel(config.archiveControlChannel())
                .localControlChannel("aeron:ipc")
                // Archive-to-archive replication channel (member catch-up) — a distinct
                // requirement from the consensus module's own replicationChannel() below, despite
                // the identical name; Aeron's Archive.Context.conclude() fails hard without it.
                .replicationChannel(config.replicationChannel())
                .threadingMode(ArchiveThreadingMode.SHARED)
                // No driverTimeoutMs-equivalent setter on Archive.Context itself (unlike
                // MediaDriver.Context/AeronArchive.Context, both set explicitly above) - its own
                // auto-created internal Aeron client picks up the aeron.driver.timeout /
                // aeron.client.liveness.timeout system properties set at the top of main() instead.
                .deleteArchiveOnStart(true);

        ConsensusModule.Context consensusModuleCtx = new ConsensusModule.Context()
                .aeronDirectoryName(config.aeronDirectoryName())
                .clusterDir(new File(config.clusterDirectoryName()))
                .archiveContext(archiveClientCtx.clone())
                .clusterMemberId(config.clusterMemberId())
                .clusterMembers(config.clusterMembers())
                .ingressChannel(config.ingressChannel())
                .logChannel(config.logChannel())
                .replicationChannel(config.replicationChannel())
                .serviceCount(1)
                // Design §5.1: nanosecond-resolution cluster clock so sequenceTimestamp (abs
                // offset 32) keeps its epoch-nanos contract. OffsetEpochNanoClock stays forbidden
                // inside SequencerClusteredService itself — it is never referenced here either;
                // NanosecondClusterClock derives cluster time deterministically from the log.
                .clusterClock(new NanosecondClusterClock())
                // No driverTimeoutMs-equivalent setter on ConsensusModule.Context itself either -
                // its own auto-created internal Aeron client (the one ConsensusModuleAgent.idle()
                // checks via aeron.isClosed(), the actual trigger this whole method's Javadoc
                // describes) picks up the system properties set at the top of main() instead.
                // See this method's own Javadoc: overrides Aeron's default (silent) hook, which
                // is exactly why dozens of prior restarts left no trace of why they happened.
                .terminationHook(() -> {
                    System.err.println(System.currentTimeMillis()
                            + " cluster-node: ConsensusModule termination hook invoked - shutting "
                            + "down (most likely cause: aeron.isClosed(), i.e. this member's Aeron "
                            + "client concluded its own co-located media driver is unresponsive - "
                            + "see CLUSTER_DRIVER_TIMEOUT_MS). Exiting cleanly, not a crash.");
                    new ShutdownSignalBarrier().signalAll();
                })
                .deleteDirOnStart(true);

        ClusteredMediaDriver clusteredMediaDriver =
                ClusteredMediaDriver.launch(mediaDriverCtx, archiveCtx, consensusModuleCtx);
        try {
            ClusteredServiceContainer.Context containerCtx = new ClusteredServiceContainer.Context()
                    .aeronDirectoryName(config.aeronDirectoryName())
                    .archiveContext(archiveClientCtx.clone())
                    .clusterDir(new File(config.clusterDirectoryName()))
                    .clusteredService(new SequencerClusteredService(config.stamping(), config.sources(),
                            config.heartbeatIntervalNanos(), egressPublisher, engineListener, serviceListener));
            ClusteredServiceContainer container = ClusteredServiceContainer.launch(containerCtx);

            return new ClusterMemberHandle(clusteredMediaDriver, container);
        } catch (RuntimeException e) {
            clusteredMediaDriver.close();
            throw e;
        }
    }

    /**
     * Handle to a running member: {@link #close()} shuts it down gracefully;
     * {@link #countersReader()} exposes the embedded media driver's Aeron counters for
     * {@link AeronCountersExporter} (design §17) — see {@link #launchNonBlocking}'s Javadoc.
     */
    public static final class ClusterMemberHandle implements AutoCloseable {
        private final ClusteredMediaDriver clusteredMediaDriver;
        private final ClusteredServiceContainer container;

        private ClusterMemberHandle(ClusteredMediaDriver clusteredMediaDriver, ClusteredServiceContainer container) {
            this.clusteredMediaDriver = clusteredMediaDriver;
            this.container = container;
        }

        public CountersReader countersReader() {
            return clusteredMediaDriver.mediaDriver().context().countersManager();
        }

        @Override
        public void close() {
            container.close();
            clusteredMediaDriver.close();
        }
    }
}
