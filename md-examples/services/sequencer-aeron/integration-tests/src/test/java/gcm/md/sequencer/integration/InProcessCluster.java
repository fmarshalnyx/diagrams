package gcm.md.sequencer.integration;

import gcm.md.sequencer.cluster.ClusterNodeConfig;
import gcm.md.sequencer.cluster.ClusterNodeLauncher;
import gcm.md.sequencer.cluster.ClusterServiceListener;
import gcm.md.sequencer.cluster.EgressPublisher;
import gcm.md.sequencer.cluster.SourcePrincipal;
import gcm.md.sequencer.stamping.EngineListener;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Test-support harness (not itself a test) for design §12.2's in-process multi-member Aeron
 * cluster suite: runs {@code memberCount} cluster-node members as separate embedded
 * {@code ClusteredMediaDriver}s in this one JVM, using
 * {@link ClusterNodeConfig#localMultiMember} for distinct-port-per-member local config.
 *
 * <p>See {@link ClusterNodeLauncher#launchNonBlocking}'s Javadoc for why {@link #killMember} is
 * a graceful stop, not a real {@code kill -9} — that failure mode needs a real process kill,
 * which only the Kubernetes environment's {@code 50-failover-drill.sh} can actually exercise.
 */
final class InProcessCluster implements AutoCloseable {

    /**
     * Every {@code *IT} class in this suite connects with this same credential (see each class's
     * {@code CREDENTIAL} constant) — a single configured principal is enough since these tests
     * only ever exercise one source.
     */
    static final String SOURCE_CREDENTIAL = "it-source-token";

    private final List<ClusterNodeConfig> configs;
    private final Function<Integer, EgressPublisher> egressPublisherFactory;
    private final EngineListener engineListener;
    private final Map<Integer, AutoCloseable> runningMembers = new ConcurrentHashMap<>();

    private InProcessCluster(List<ClusterNodeConfig> configs,
                              Function<Integer, EgressPublisher> egressPublisherFactory,
                              EngineListener engineListener) {
        this.configs = configs;
        this.egressPublisherFactory = egressPublisherFactory;
        this.engineListener = engineListener;
    }

    /**
     * Builds and starts every member. {@code egressPublisherFactory} is called once per member
     * id (0-based), so tests can supply per-member recording fakes or the real
     * {@code AeronEgressPublisher}.
     */
    static InProcessCluster start(int memberCount, String baseDataDir,
                                   Function<Integer, EgressPublisher> egressPublisherFactory,
                                   EngineListener engineListener) throws Exception {
        List<ClusterNodeConfig> configs = ClusterNodeConfig.localMultiMember(memberCount, baseDataDir,
                List.of(new SourcePrincipal("it-source", 1L, SOURCE_CREDENTIAL)));
        InProcessCluster cluster = new InProcessCluster(configs, egressPublisherFactory, engineListener);
        for (int memberId = 0; memberId < memberCount; memberId++) {
            cluster.startMember(memberId);
        }
        return cluster;
    }

    private void startMember(int memberId) throws Exception {
        ClusterNodeConfig config = configs.get(memberId);
        EgressPublisher egressPublisher = egressPublisherFactory.apply(memberId);
        AutoCloseable handle = ClusterNodeLauncher.launchNonBlocking(config, egressPublisher, engineListener,
                new ClusterServiceListener() {
                });
        runningMembers.put(memberId, handle);
    }

    /** Gracefully stops one member (see class Javadoc: not a true {@code kill -9}). */
    void killMember(int memberId) throws Exception {
        AutoCloseable handle = runningMembers.remove(memberId);
        if (handle != null) {
            handle.close();
        }
    }

    /** Restarts a previously killed member against its same on-disk state (data dirs are untouched by killMember). */
    void restartMember(int memberId) throws Exception {
        if (runningMembers.containsKey(memberId)) {
            throw new IllegalStateException("Member " + memberId + " is already running");
        }
        startMember(memberId);
    }

    /** The on-disk consensus-module/Raft-log directory for one member, e.g. for {@code ClusterTool} requests. */
    String clusterDirectoryName(int memberId) {
        return configs.get(memberId).clusterDirectoryName();
    }

    @Override
    public void close() throws Exception {
        Exception firstFailure = null;
        for (AutoCloseable handle : runningMembers.values()) {
            try {
                handle.close();
            } catch (Exception e) {
                if (firstFailure == null) {
                    firstFailure = e;
                }
            }
        }
        runningMembers.clear();
        if (firstFailure != null) {
            throw firstFailure;
        }
    }
}
