package gcm.md.sequencer.cluster;

import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Object2LongHashMap;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Resolves a session's {@code encodedPrincipal()} credential to a configured {@code sourceId}
 * (design §5.1, §7): "clients map to sourceIds at session-open via a credentials/principal
 * scheme... Reject unknown principals." Also remembers the sourceId for each open session so
 * {@code onSessionMessage} can look it up without re-decoding the principal on every message.
 */
final class SourcePrincipalRegistry {

    private static final long NO_SOURCE = -1L;
    private static final long UNRESOLVED_SESSION = -1L;

    private final Object2LongHashMap<String> sourceIdByCredential = new Object2LongHashMap<>(NO_SOURCE);
    private final Long2LongHashMap sourceIdBySessionId = new Long2LongHashMap(UNRESOLVED_SESSION);

    SourcePrincipalRegistry(List<SourcePrincipal> sources) {
        for (SourcePrincipal source : sources) {
            sourceIdByCredential.put(source.credential(), source.sourceId());
        }
    }

    /**
     * Resolves {@code encodedPrincipal} to a configured sourceId and remembers it for
     * {@code sessionId}. Returns {@code false} (session must be rejected) if the credential is
     * not configured.
     */
    boolean admit(long sessionId, byte[] encodedPrincipal) {
        String credential = new String(encodedPrincipal, StandardCharsets.UTF_8);
        long sourceId = sourceIdByCredential.getValue(credential);
        if (sourceId == NO_SOURCE) {
            return false;
        }
        sourceIdBySessionId.put(sessionId, sourceId);
        return true;
    }

    /** Returns the sourceId resolved for {@code sessionId} at session-open, or -1 if unknown. */
    long sourceIdFor(long sessionId) {
        return sourceIdBySessionId.get(sessionId);
    }

    void onSessionClose(long sessionId) {
        sourceIdBySessionId.remove(sessionId);
    }
}
