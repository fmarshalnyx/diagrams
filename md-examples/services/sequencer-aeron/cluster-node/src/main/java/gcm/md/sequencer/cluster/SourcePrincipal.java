package gcm.md.sequencer.cluster;

/**
 * One entry of {@code cluster.sources} config (design §5.1, §5.5): maps an ingress client's
 * credential to the {@code sourceId} {@link gcm.md.sequencer.stamping.StampingEngine#onMessage}
 * tracks per-source dedupe/gap state against. {@code credential} is the exact byte sequence
 * (UTF-8 encoded) a session must present via {@code ClientSession.encodedPrincipal()} at
 * session-open to be admitted as this source.
 */
public record SourcePrincipal(String name, long sourceId, String credential) {
}
