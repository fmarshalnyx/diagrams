package gcm.md.sequencer.bench;

import gcm.md.sequencer.stamping.EngineListener;
import gcm.md.sequencer.stamping.StampingConfig;
import gcm.md.sequencer.stamping.StampingEngine;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;

/**
 * Phase-2 counterpart of {@link StampBenchmark}: isolates {@link StampingEngine#onMessage}, the
 * body of {@code SequencerClusteredService.onSessionMessage} (design §12.5), with no Aeron, no
 * cluster, no allocation in the timed region — the pure per-message cost the clustered service
 * pays on top of whatever Raft/log-append overhead {@link ClusterOfferBenchmark} measures
 * separately. Comparing the two numbers is how design §12.5's "document multi-AZ vs single-AZ
 * consensus cost" is meant to be read: this benchmark is the floor: consensus can only add
 * latency on top of it, never remove any.
 *
 * <p>Run with {@code java -jar target/sequencer-bench.jar StampingEngineBenchmark}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class StampingEngineBenchmark {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    private StampingEngine engine;
    private UnsafeBuffer buffer;
    private int sourceSeqNum;

    @Setup(Level.Trial)
    public void setUp() {
        engine = new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });

        buffer = new UnsafeBuffer(new byte[128]);
        buffer.putShort(2, (short) 9, LE);   // templateId 9 (MarketDataDelta) — source-tracked
        buffer.putShort(4, (short) 100, LE); // schemaId
    }

    @Benchmark
    public void onMessage(Blackhole blackhole) {
        buffer.putInt(64, ++sourceSeqNum, LE); // strictly increasing: never hits the DUPLICATE branch
        blackhole.consume(engine.onMessage(buffer, 0, buffer.capacity(), sourceSeqNum, 1L));
    }
}
