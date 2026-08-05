package gcm.md.natsbridge.bridge;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LeaderArchiveConnectorTest {

    private static final String LIVE_DESTINATION = "aeron:udp?control=cluster:9070|control-mode=dynamic";
    private static final int STREAM_ID = 1;
    private static final int SESSION_ID = 1_000_100;

    private AeronArchive.Context context(String label) {
        // Context has no useful equals(); use the archiveDir-like control channel field as a
        // stand-in identity so the fake connector map below can distinguish candidates.
        return new AeronArchive.Context().controlRequestChannel(label);
    }

    private LeaderArchiveConnector connectorReturning(Map<String, AeronArchive> byLabel) {
        Function<AeronArchive.Context, AeronArchive> fake = ctx -> {
            AeronArchive archive = byLabel.get(ctx.controlRequestChannel());
            if (archive == null) {
                throw new RuntimeException("unreachable: " + ctx.controlRequestChannel());
            }
            return archive;
        };
        return new LeaderArchiveConnector(fake);
    }

    @Test
    void picksTheFirstMemberThatHasAMatchingRecording() {
        AeronArchive noRecording = mock(AeronArchive.class);
        when(noRecording.findLastMatchingRecording(anyLong(), anyString(), anyInt(), anyInt()))
                .thenReturn((long) Aeron.NULL_VALUE);
        AeronArchive hasRecording = mock(AeronArchive.class);
        when(hasRecording.findLastMatchingRecording(anyLong(), anyString(), anyInt(), anyInt()))
                .thenReturn(42L);

        LeaderArchiveConnector connector = connectorReturning(Map.of(
                "member-0", noRecording,
                "member-1", hasRecording));

        AeronArchive result = connector.connectToRecordingOwner(
                List.of(context("member-0"), context("member-1")), LIVE_DESTINATION, STREAM_ID, SESSION_ID);

        assertThat(result).isSameAs(hasRecording);
        verify(noRecording).close();
        verify(hasRecording, never()).close();
    }

    @Test
    void skipsAnUnreachableMemberAndTriesTheNextOne() {
        AeronArchive reachable = mock(AeronArchive.class);
        when(reachable.findLastMatchingRecording(anyLong(), anyString(), anyInt(), anyInt())).thenReturn(7L);

        // Only "member-1" is present in the fake map — "member-0" throws (simulating unreachable).
        LeaderArchiveConnector connector = connectorReturning(Map.of("member-1", reachable));

        AeronArchive result = connector.connectToRecordingOwner(
                List.of(context("member-0"), context("member-1")), LIVE_DESTINATION, STREAM_ID, SESSION_ID);

        assertThat(result).isSameAs(reachable);
    }

    @Test
    void fallsBackToTheFirstReachableCandidateWhenNoneHaveARecordingYet() {
        AeronArchive first = mock(AeronArchive.class);
        when(first.findLastMatchingRecording(anyLong(), anyString(), anyInt(), anyInt())).thenReturn((long) Aeron.NULL_VALUE);
        AeronArchive second = mock(AeronArchive.class);
        when(second.findLastMatchingRecording(anyLong(), anyString(), anyInt(), anyInt())).thenReturn((long) Aeron.NULL_VALUE);

        LeaderArchiveConnector connector = connectorReturning(Map.of("member-0", first, "member-1", second));

        AeronArchive result = connector.connectToRecordingOwner(
                List.of(context("member-0"), context("member-1")), LIVE_DESTINATION, STREAM_ID, SESSION_ID);

        // Fresh cluster, nothing recorded anywhere yet — first reachable candidate is fine, and
        // it must be the one actually returned (not closed out from under the caller).
        assertThat(result).isSameAs(first);
        verify(first, never()).close();
        verify(second).close();
    }

    @Test
    void throwsWhenNoCandidateIsReachableAtAll() {
        LeaderArchiveConnector connector = connectorReturning(Map.of());

        assertThatThrownBy(() -> connector.connectToRecordingOwner(
                List.of(context("member-0"), context("member-1")), LIVE_DESTINATION, STREAM_ID, SESSION_ID))
                .isInstanceOf(IllegalStateException.class);
    }
}
