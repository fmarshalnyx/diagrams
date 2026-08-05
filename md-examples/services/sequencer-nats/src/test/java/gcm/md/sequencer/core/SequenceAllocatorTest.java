package gcm.md.sequencer.core;

import gcm.md.sequencer.config.SequencerProperties;
import gcm.md.sequencer.metrics.SequencerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.Error;
import io.nats.client.api.KeyValueEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SequenceAllocator} against an in-memory fake of the NATS KV bucket
 * (project spec §15: block exhaustion, proactive lease-ahead, restart-resume monotonicity,
 * KV CAS conflict / split-brain).
 */
class SequenceAllocatorTest {

    /** In-memory stand-in for the server-side KV bucket state, shared across fake KeyValue mocks. */
    private static final class FakeBucket {
        final AtomicReference<String> value = new AtomicReference<>();
        final AtomicLong revision = new AtomicLong(0);
    }

    private FakeBucket bucket;
    private LeaderElection leaderElection;
    private SequencerMetrics metrics;

    @BeforeEach
    void setUp() {
        bucket = new FakeBucket();
        leaderElection = mock(LeaderElection.class);
        when(leaderElection.isLeader()).thenReturn(true);
        metrics = new SequencerMetrics(new SimpleMeterRegistry());
    }

    private KeyValue fakeKeyValue(FakeBucket state) throws Exception {
        KeyValue kv = mock(KeyValue.class);
        when(kv.get(anyString())).thenAnswer(inv -> {
            if (state.value.get() == null) {
                return null;
            }
            KeyValueEntry entry = mock(KeyValueEntry.class);
            when(entry.getValueAsString()).thenReturn(state.value.get());
            when(entry.getRevision()).thenReturn(state.revision.get());
            return entry;
        });
        when(kv.create(anyString(), any(byte[].class))).thenAnswer(inv -> {
            if (state.value.get() != null) {
                throw new JetStreamApiException(Error.JsBadRequestErr);
            }
            byte[] bytes = inv.getArgument(1);
            state.value.set(new String(bytes, StandardCharsets.UTF_8));
            return state.revision.incrementAndGet();
        });
        when(kv.update(anyString(), any(byte[].class), anyLong())).thenAnswer(inv -> {
            long expectedRevision = inv.getArgument(2);
            if (state.revision.get() != expectedRevision) {
                throw new JetStreamApiException(Error.JsBadRequestErr);
            }
            byte[] bytes = inv.getArgument(1);
            state.value.set(new String(bytes, StandardCharsets.UTF_8));
            return state.revision.incrementAndGet();
        });
        return kv;
    }

    private SequenceAllocator newAllocator(long blockSize, double leaseAheadFraction) throws Exception {
        SequencerProperties properties = new SequencerProperties();
        properties.getAllocator().setBlockSize(blockSize);
        properties.getAllocator().setLeaseAheadFraction(leaseAheadFraction);
        properties.getAllocator().setKvBucket("sequencer-lease");
        properties.getAllocator().setKvKey("high-water");

        Connection connection = mock(Connection.class);
        KeyValue kv = fakeKeyValue(bucket);
        when(connection.keyValue("sequencer-lease")).thenReturn(kv);

        SequenceAllocator allocator = new SequenceAllocator(connection, properties, leaderElection, metrics);
        allocator.initialize();
        return allocator;
    }

    @Test
    void firstBlockStartsAtOneOnAFreshBucket() throws Exception {
        SequenceAllocator allocator = newAllocator(10, 0.8);
        assertThat(allocator.next()).isEqualTo(1L);
        assertThat(allocator.next()).isEqualTo(2L);
    }

    @Test
    void sequenceIdsAreMonotonicAcrossABlockBoundary() throws Exception {
        SequenceAllocator allocator = newAllocator(3, 0.9);
        long previous = 0;
        for (int i = 0; i < 20; i++) {
            long next = allocator.next();
            assertThat(next).isEqualTo(previous + 1);
            previous = next;
        }
    }

    @Test
    void leaseAheadFractionTriggersProactiveLeaseBeforeExhaustion() throws Exception {
        KeyValue kv = fakeKeyValue(bucket);
        Connection connection = mock(Connection.class);
        when(connection.keyValue("sequencer-lease")).thenReturn(kv);

        SequencerProperties properties = new SequencerProperties();
        properties.getAllocator().setBlockSize(10);
        properties.getAllocator().setLeaseAheadFraction(0.5);

        SequenceAllocator allocator = new SequenceAllocator(connection, properties, leaderElection, metrics);
        allocator.initialize();
        // initial lease: 1 create call. Crossing the 50% threshold (id 5 of [1..10]) should
        // trigger a second (async) lease before the block is actually exhausted at id 10.
        for (int i = 0; i < 6; i++) {
            allocator.next();
        }
        // Give the background lease-ahead executor a moment to run.
        long deadline = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < deadline && bucket.revision.get() < 2) {
            Thread.sleep(10);
        }
        verify(kv, org.mockito.Mockito.atLeast(1)).update(anyString(), any(byte[].class), anyLong());
    }

    @Test
    void restartResumesMonotonicallyFromThePersistedHighWaterMark() throws Exception {
        SequenceAllocator first = newAllocator(5, 0.8);
        long lastFromFirstInstance = 0;
        for (int i = 0; i < 5; i++) {
            lastFromFirstInstance = first.next();
        }

        // Simulate a crash + restart: a brand-new allocator instance against the same KV state.
        Connection connection = mock(Connection.class);
        KeyValue kv = fakeKeyValue(bucket);
        when(connection.keyValue("sequencer-lease")).thenReturn(kv);
        SequencerProperties properties = new SequencerProperties();
        properties.getAllocator().setBlockSize(5);
        SequenceAllocator second = new SequenceAllocator(connection, properties, leaderElection, metrics);
        second.initialize();

        assertThat(second.next()).isGreaterThan(lastFromFirstInstance);
    }

    @Test
    void casConflictIsRetriedAgainstTheFreshlyStoredValue() throws Exception {
        KeyValue kv = mock(KeyValue.class);
        // First read: bucket empty -> create() succeeds, revision 1, value "10".
        // Simulate a concurrent writer having already bumped the stored value/revision by the
        // time our update() lands: first update() attempt fails, second (after reread) succeeds.
        AtomicLong revision = new AtomicLong(0);
        AtomicReference<String> value = new AtomicReference<>();
        AtomicReference<Boolean> conflictInjected = new AtomicReference<>(false);

        when(kv.get(anyString())).thenAnswer(inv -> {
            if (value.get() == null) {
                return null;
            }
            KeyValueEntry entry = mock(KeyValueEntry.class);
            when(entry.getValueAsString()).thenReturn(value.get());
            when(entry.getRevision()).thenReturn(revision.get());
            return entry;
        });
        when(kv.create(anyString(), any(byte[].class))).thenAnswer(inv -> {
            byte[] bytes = inv.getArgument(1);
            value.set(new String(bytes, StandardCharsets.UTF_8));
            // Inject a concurrent external bump right after our create, before our next update.
            revision.set(5);
            return revision.get();
        });
        when(kv.update(anyString(), any(byte[].class), anyLong())).thenAnswer(inv -> {
            long expectedRevision = inv.getArgument(2);
            if (!conflictInjected.get()) {
                conflictInjected.set(true);
                throw new JetStreamApiException(Error.JsBadRequestErr);
            }
            if (revision.get() != expectedRevision) {
                throw new JetStreamApiException(Error.JsBadRequestErr);
            }
            byte[] bytes = inv.getArgument(1);
            value.set(new String(bytes, StandardCharsets.UTF_8));
            return revision.incrementAndGet();
        });

        Connection connection = mock(Connection.class);
        when(connection.keyValue("sequencer-lease")).thenReturn(kv);

        SequencerProperties properties = new SequencerProperties();
        properties.getAllocator().setBlockSize(3);

        SequenceAllocator allocator = new SequenceAllocator(connection, properties, leaderElection, metrics);
        allocator.initialize();

        assertThat(allocator.next()).isEqualTo(1L);
    }

    @Test
    void refusesToLeaseWithoutLeadership() throws Exception {
        when(leaderElection.isLeader()).thenReturn(false);
        Connection connection = mock(Connection.class);
        KeyValue kv = fakeKeyValue(bucket);
        when(connection.keyValue(anyString())).thenReturn(kv);

        SequencerProperties properties = new SequencerProperties();
        SequenceAllocator allocator = new SequenceAllocator(connection, properties, leaderElection, metrics);

        assertThatThrownBy(allocator::initialize).isInstanceOf(IllegalStateException.class);
    }
}
