package gcm.md.sequencer.bench;

import com.usb.gcm.md.sbe.MarketDataDeltaEncoder;
import com.usb.gcm.md.sbe.MessageHeaderEncoder;
import gcm.md.sequencer.config.SequencerProperties;
import gcm.md.sequencer.core.SequenceStamper;
import gcm.md.sequencer.metrics.SequencerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.agrona.concurrent.OffsetEpochNanoClock;
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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Isolates the sequencer's entire hot-path stamping logic (project spec §4, §15 acceptance:
 * "JMH: stamp path in isolation, target: low double-digit ns"). No NATS, no Spring, no
 * allocation in the timed region — only {@link SequenceStamper#stamp}.
 *
 * <p>Run with {@code java -jar target/sequencer-bench.jar StampBenchmark}.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class StampBenchmark {

    private SequenceStamper stamper;
    private UnsafeBuffer buffer;
    private long sequenceId;

    @Setup(Level.Trial)
    public void setUp() {
        SequencerProperties properties = new SequencerProperties();
        stamper = new SequenceStamper(properties, new OffsetEpochNanoClock(),
                new SequencerMetrics(new SimpleMeterRegistry()));

        byte[] data = new byte[128];
        buffer = new UnsafeBuffer(ByteBuffer.wrap(data));
        MarketDataDeltaEncoder encoder = new MarketDataDeltaEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        encoder.header().sequenceId(0).sourceTimestamp(0).ingestTimestamp(0).sequenceTimestamp(0).eventId(0).reserved1(0);
        encoder.fieldPresence().clear();
        encoder.instrumentId(1);
        encoder.sourceSeqNum(1);
        encoder.changedFieldsCount(0);
    }

    @Benchmark
    public void stamp(Blackhole blackhole) {
        blackhole.consume(stamper.stamp(buffer, ++sequenceId));
    }
}
