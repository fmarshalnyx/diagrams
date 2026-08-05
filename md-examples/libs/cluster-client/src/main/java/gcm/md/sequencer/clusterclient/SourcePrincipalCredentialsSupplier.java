package gcm.md.sequencer.clusterclient;

import io.aeron.security.CredentialsSupplier;

import java.nio.charset.StandardCharsets;

/**
 * Supplies this client's configured credential as the session's {@code encodedPrincipal} at
 * connect time (design §5.1, §7: "Credentials → sourceId principal"). The server side
 * ({@code services/sequencer-aeron/cluster-node}'s {@code cluster.sources} config) maps this
 * same credential string back to a {@code sourceId}; the two must be configured to agree, but
 * this module holds no dependency on that server-side config type (design §3.3: {@code libs/*}
 * must not depend on {@code services/*}) — just the raw credential string.
 */
public final class SourcePrincipalCredentialsSupplier implements CredentialsSupplier {

    private final byte[] encodedCredentials;

    public SourcePrincipalCredentialsSupplier(String credential) {
        this.encodedCredentials = credential.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public byte[] encodedCredentials() {
        return encodedCredentials;
    }

    @Override
    public byte[] onChallenge(byte[] encodedChallenge) {
        // This module's credential scheme is a single shared-secret token verified at
        // session-open (design §5.1) — no interactive challenge/response round trip. Aeron only
        // invokes this if the server's authenticator issues a challenge, which the cluster-node
        // authenticator built for this scheme never does.
        return encodedCredentials;
    }
}
