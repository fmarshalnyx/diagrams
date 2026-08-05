package gcm.md.natsbridge.bridge;

import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * NATS KV-backed checkpoint of the last-bridged sequenceId (design §9). Purely an optimization
 * to avoid re-replaying the whole recording from the start on every restart — the bridge is
 * "stateless-restartable, never authoritative" (design §9): if this checkpoint were lost
 * entirely, the bridge would just re-derive everything from a full replay and skip whatever
 * {@link ContiguityTracker} recognizes as already-bridged once it reaches the live edge.
 */
public final class BridgeCheckpoint {

    private final KeyValue kv;
    private final String key;

    public BridgeCheckpoint(KeyValue kv, String key) {
        this.kv = kv;
        this.key = key;
    }

    /** Returns the last checkpointed sequenceId, or 0 if none has ever been written. */
    long read() {
        try {
            KeyValueEntry entry = kv.get(key);
            return entry == null ? 0L : Long.parseLong(entry.getValueAsString());
        } catch (IOException | JetStreamApiException e) {
            throw new IllegalStateException("Failed to read bridge checkpoint", e);
        }
    }

    void write(long sequenceId) {
        try {
            kv.put(key, Long.toString(sequenceId).getBytes(StandardCharsets.UTF_8));
        } catch (IOException | JetStreamApiException e) {
            throw new IllegalStateException("Failed to write bridge checkpoint", e);
        }
    }
}
