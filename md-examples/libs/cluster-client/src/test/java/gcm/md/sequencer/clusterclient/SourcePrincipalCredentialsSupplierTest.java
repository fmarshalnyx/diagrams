package gcm.md.sequencer.clusterclient;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class SourcePrincipalCredentialsSupplierTest {

    @Test
    void encodedCredentialsIsTheUtf8EncodedConfiguredToken() {
        SourcePrincipalCredentialsSupplier supplier = new SourcePrincipalCredentialsSupplier("cme-token");
        assertThat(supplier.encodedCredentials()).isEqualTo("cme-token".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void onChallengeReturnsTheSameCredentialsSinceThisSchemeNeverIssuesAChallenge() {
        SourcePrincipalCredentialsSupplier supplier = new SourcePrincipalCredentialsSupplier("cme-token");
        assertThat(supplier.onChallenge(new byte[0])).isEqualTo(supplier.encodedCredentials());
    }
}
