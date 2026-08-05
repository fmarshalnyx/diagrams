package gcm.md.sequencer.cluster;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SourcePrincipalRegistryTest {

    private static final List<SourcePrincipal> SOURCES = List.of(
            new SourcePrincipal("cme-smart-stream", 1L, "cme-token"),
            new SourcePrincipal("cme-mdp3", 2L, "mdp3-token"));

    @Test
    void admitsAConfiguredCredentialAndResolvesItsSourceId() {
        SourcePrincipalRegistry registry = new SourcePrincipalRegistry(SOURCES);
        boolean admitted = registry.admit(100L, "cme-token".getBytes(StandardCharsets.UTF_8));
        assertThat(admitted).isTrue();
        assertThat(registry.sourceIdFor(100L)).isEqualTo(1L);
    }

    @Test
    void rejectsAnUnconfiguredCredential() {
        SourcePrincipalRegistry registry = new SourcePrincipalRegistry(SOURCES);
        boolean admitted = registry.admit(100L, "not-a-real-token".getBytes(StandardCharsets.UTF_8));
        assertThat(admitted).isFalse();
    }

    @Test
    void distinctSessionsForTheSameCredentialEachResolveIndependently() {
        SourcePrincipalRegistry registry = new SourcePrincipalRegistry(SOURCES);
        registry.admit(100L, "cme-token".getBytes(StandardCharsets.UTF_8));
        registry.admit(200L, "mdp3-token".getBytes(StandardCharsets.UTF_8));
        assertThat(registry.sourceIdFor(100L)).isEqualTo(1L);
        assertThat(registry.sourceIdFor(200L)).isEqualTo(2L);
    }

    @Test
    void forgetsASessionsSourceIdOnceClosed() {
        SourcePrincipalRegistry registry = new SourcePrincipalRegistry(SOURCES);
        registry.admit(100L, "cme-token".getBytes(StandardCharsets.UTF_8));
        registry.onSessionClose(100L);
        assertThat(registry.sourceIdFor(100L)).isEqualTo(-1L);
    }
}
