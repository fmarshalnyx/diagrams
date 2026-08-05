package gcm.md.natsbridge.bridge;

import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BridgeCheckpointTest {

    private static final String KEY = "last-bridged-sequence-id";

    @Test
    void readReturnsZeroWhenNoCheckpointHasEverBeenWritten() throws Exception {
        KeyValue kv = mock(KeyValue.class);
        when(kv.get(KEY)).thenReturn(null);

        BridgeCheckpoint checkpoint = new BridgeCheckpoint(kv, KEY);
        assertThat(checkpoint.read()).isZero();
    }

    @Test
    void readParsesThePersistedValue() throws Exception {
        KeyValue kv = mock(KeyValue.class);
        KeyValueEntry entry = mock(KeyValueEntry.class);
        when(entry.getValueAsString()).thenReturn("42");
        when(kv.get(KEY)).thenReturn(entry);

        BridgeCheckpoint checkpoint = new BridgeCheckpoint(kv, KEY);
        assertThat(checkpoint.read()).isEqualTo(42L);
    }

    @Test
    void writePersistsTheSequenceIdAsUtf8Text() throws Exception {
        KeyValue kv = mock(KeyValue.class);
        BridgeCheckpoint checkpoint = new BridgeCheckpoint(kv, KEY);

        checkpoint.write(123L);

        verify(kv).put(eq(KEY), eq("123".getBytes(StandardCharsets.UTF_8)));
    }
}
