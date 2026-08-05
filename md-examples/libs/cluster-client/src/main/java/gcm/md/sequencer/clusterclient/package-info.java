/**
 * Cluster ingress client (design §7): a thin wrapper over {@code AeronCluster}, one of the two
 * config-selectable {@code IngressTransport} implementations line handlers depend on (see
 * {@code libs/ingress-transport}'s package-info for why transport selection is a config choice
 * rather than a staged migration through a shim service).
 *
 * <h2>The idempotency contract</h2>
 * The clustered service dedupes ingress per source on {@code sourceSeqNum} (design §4's
 * per-source tracking: {@code sourceSeqNum <= last} is a no-op {@code DUPLICATE}, never a new
 * sequenceId). Because of that, crash-recovery for any caller of {@link
 * gcm.md.sequencer.clusterclient.ClusterIngressClient} is simply:
 *
 * <blockquote>Republish your tail since last known-processed.</blockquote>
 *
 * A caller that crashes mid-batch, restarts, and re-offers everything from its last
 * known-committed {@code sourceSeqNum} onward is guaranteed correct: whatever the cluster had
 * already durably assigned a sequenceId to is recognized and skipped as a duplicate; whatever it
 * hadn't yet seen is assigned normally. There is no way to "double count" a message by
 * over-republishing, and no coordination is required between the caller and the cluster about
 * exactly where the crash landed.
 *
 * <p><b>This supersedes phase-1's "redelivery gets a new sequenceId" caveat</b> (documented in
 * {@code services/sequencer-nats}'s README, where it exists because phase-1's sequencer never
 * decodes message bodies and therefore cannot recognize a redelivered duplicate). That caveat
 * becomes stale — and should be struck from downstream docs — once a line handler's config
 * switches it onto this transport and phase-1 ingress is retired for that source.
 */
package gcm.md.sequencer.clusterclient;
