package gcm.md.sequencer.stamping;

import com.sun.management.ThreadMXBean;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Design §4 acceptance criterion: {@code libs/sequencer-core} must show zero per-message
 * allocation on {@link StampingEngine#onMessage}. Measured via {@code
 * com.sun.management.ThreadMXBean#getThreadAllocatedBytes}, the standard technique for
 * allocation assertions without pulling in an agent/profiler dependency — consistent with the
 * module's "Agrona only" dependency rule.
 */
class StampingEngineAllocationTest {

    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;
    private static final int WARMUP_ITERATIONS = 50_000;
    private static final int MEASURED_ITERATIONS = 100_000;

    private long allocatedBytes(Runnable work) {
        ThreadMXBean threadMXBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        long threadId = Thread.currentThread().threadId();
        work.run();
        long before = threadMXBean.getThreadAllocatedBytes(threadId);
        work.run();
        return threadMXBean.getThreadAllocatedBytes(threadId) - before;
    }

    @Test
    void onMessageAllocatesNothingOnTheDefaultUntrackedPath() {
        StampingEngine engine = new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(96));
        buffer.putShort(2, (short) 1, LE); // templateId 1: not source-tracked, not the boundary template
        buffer.putShort(4, (short) 100, LE);

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            engine.onMessage(buffer, 0, buffer.capacity(), i, 1L);
        }

        long allocated = allocatedBytes(() -> {
            for (int i = 0; i < MEASURED_ITERATIONS; i++) {
                engine.onMessage(buffer, 0, buffer.capacity(), i, 1L);
            }
        });

        assertThat(allocated).isZero();
    }

    @Test
    void onMessageAllocatesNothingOnTheSourceTrackedNormalAdvancePath() {
        StampingEngine engine = new StampingEngine(StampingConfig.v4Defaults(), new EngineListener() {
        });
        UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(96));
        buffer.putShort(2, (short) 9, LE); // templateId 9: source-tracked by default at abs offset 64
        buffer.putShort(4, (short) 100, LE);

        long[] sourceSeqNum = {0};
        Runnable step = () -> {
            sourceSeqNum[0]++;
            buffer.putInt(64, (int) sourceSeqNum[0], LE);
            engine.onMessage(buffer, 0, buffer.capacity(), sourceSeqNum[0], 1L);
        };

        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            step.run();
        }

        long allocated = allocatedBytes(() -> {
            for (int i = 0; i < MEASURED_ITERATIONS; i++) {
                step.run();
            }
        });

        assertThat(allocated).isZero();
    }
}
