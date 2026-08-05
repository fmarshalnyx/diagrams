package gcm.md.sequencer.bench;

import gcm.md.sequencer.cluster.AeronEgressConfig;
import gcm.md.sequencer.cluster.AeronEgressPublisher;
import gcm.md.sequencer.cluster.ClusterNodeConfig;
import gcm.md.sequencer.cluster.ClusterNodeLauncher;
import gcm.md.sequencer.cluster.ClusterServiceListener;
import gcm.md.sequencer.cluster.EgressListener;
import gcm.md.sequencer.cluster.SourcePrincipal;
import gcm.md.sequencer.clusterclient.ClusterIngressClient;
import gcm.md.sequencer.clusterclient.IngressClientConfig;
import gcm.md.sequencer.clusterclient.SourcePrincipalCredentialsSupplier;
import gcm.md.sequencer.stamping.EngineListener;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * Design §12.5's "cluster offer path" benchmark: a single embedded cluster-node member in this
 * JVM (matching {@link ClusterNodeConfig#localSingleMember()}), timing the round trip from
 * {@link ClusterIngressClient#offer} to the same message observed on the real egress wire — i.e.
 * {@link StampingEngineBenchmark}'s pure in-memory stamp cost <b>plus</b> everything Aeron Cluster
 * adds around it: SBE-independent Raft log append, local consensus commit, and archive recording.
 *
 * <p>This is deliberately a <b>single-member, single-AZ local baseline</b>, not a multi-AZ
 * measurement — this in-process harness has no way to introduce real network latency between
 * replicas (see {@code integration-tests}' {@code InProcessCluster} Javadoc for the same
 * limitation). Design §12.5 asks this suite to "document multi-AZ vs single-AZ consensus cost":
 * the honest way to do that is {@code (this benchmark's number)} as the floor, plus a
 * <b>separately measured</b> real quorum-replication round-trip time (inter-AZ RTT × the number of
 * round trips Raft's replication protocol needs per commit) gathered once the Kubernetes
 * validation phase stands up an actual multi-AZ cluster — that number does not exist yet and must
 * not be estimated here.
 *
 * <p>Run with {@code java -jar target/sequencer-bench.jar ClusterOfferBenchmark}. Unlike the other
 * two benchmarks in this module, this one is comparatively heavy (embeds a full
 * {@code ClusteredMediaDriver} + {@code ConsensusModule} + {@code ClusteredServiceContainer}) —
 * expect JMH startup to take significantly longer than {@link StampBenchmark} or
 * {@link StampingEngineBenchmark}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(java.util.concurrent.TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class ClusterOfferBenchmark {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final String CREDENTIAL = "bench-source-token";
    private static final String EGRESS_CHANNEL = "aeron:udp?control=localhost:9070|control-mode=dynamic";
    private static final int EGRESS_STREAM_ID = 1;

    private AutoCloseable clusterMember;
    private ClusterIngressClient client;
    private MediaDriver observerDriver;
    private Aeron observerAeron;
    private Subscription observerSubscription;
    private UnsafeBuffer buffer;
    private int sourceSeqNum;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        String dataDir = System.getProperty("java.io.tmpdir") + "/gcm-md-bench-cluster-" + System.nanoTime();

        AeronEgressConfig egressConfig = AeronEgressConfig.localDefaults();
        AeronArchive.Context archiveClientCtx = new AeronArchive.Context()
                .controlRequestChannel("aeron:udp?endpoint=localhost:9050")
                .controlResponseChannel("aeron:udp?endpoint=localhost:9051");
        AeronEgressPublisher egressPublisher = new AeronEgressPublisher(egressConfig, archiveClientCtx,
                new EgressListener() {
                });

        ClusterNodeConfig config = ClusterNodeConfig.singleMember("localhost", dataDir,
                List.of(new SourcePrincipal("bench-source", 1L, CREDENTIAL)));
        clusterMember = ClusterNodeLauncher.launchNonBlocking(config, egressPublisher, new EngineListener() {
        }, new ClusterServiceListener() {
        });

        observerDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED).dirDeleteOnStart(true).dirDeleteOnShutdown(true));
        observerAeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(observerDriver.aeronDirectoryName()));
        observerSubscription = observerAeron.addSubscription(EGRESS_CHANNEL, EGRESS_STREAM_ID);

        AeronCluster.Context clusterContext = new AeronCluster.Context()
                .aeronDirectoryName(observerDriver.aeronDirectoryName())
                .ingressChannel("aeron:udp?term-length=64k")
                .ingressEndpoints("0=localhost:9010")
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .credentialsSupplier(new SourcePrincipalCredentialsSupplier(CREDENTIAL));
        client = new ClusterIngressClient(clusterContext, IngressClientConfig.defaults());

        buffer = new UnsafeBuffer(new byte[68]); // sourceSeqNum at abs offset 64 (int)
        buffer.putShort(2, (short) 9, LE);   // templateId 9 (MarketDataDelta) — source-tracked
        buffer.putShort(4, (short) 100, LE); // schemaId
    }

    @Benchmark
    public void offerToEgressRoundTrip() {
        buffer.putInt(64, ++sourceSeqNum, LE);
        long result;
        do {
            result = client.offer(buffer, 0, buffer.capacity());
            if (result < 0) {
                LockSupport.parkNanos(1_000L);
            }
        } while (result < 0);

        while (observerSubscription.poll((fragment, offset, length, header) -> {
        }, 1) == 0) {
            LockSupport.parkNanos(1_000L);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        CloseHelper.quietClose(client);
        CloseHelper.quietClose(observerAeron);
        CloseHelper.quietClose(observerDriver);
        if (clusterMember != null) {
            clusterMember.close();
        }
    }
}
