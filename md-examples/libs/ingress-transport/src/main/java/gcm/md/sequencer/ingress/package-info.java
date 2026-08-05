/**
 * Transport-agnostic line-handler ingress ({@link gcm.md.sequencer.ingress.IngressTransport}).
 *
 * <h2>Why this exists</h2>
 * The original phase-2 migration plan (design §8) routed every line handler through a temporary
 * {@code ingress-shim} strangler service, so upstream producers never had to change during the
 * migration. That trade only pays for itself when there are many independently-deployed producer
 * teams and live external traffic to protect. This project has neither (one team owns both line
 * handlers and the sequencer; no external clients before first release), so the coordination cost
 * the shim exists to avoid is not actually present — while the shim itself is a whole extra
 * service to build, deploy, and eventually delete.
 *
 * <p>Instead, each line handler depends on this interface directly and picks its concrete
 * transport via config (a {@code @ConditionalOnProperty}-selected bean, per this repo's
 * ServiceConfiguration convention) — {@code gcm.md.sequencer.clusterclient.ClusterIngressClient}
 * for the Aeron cluster, or {@code gcm.md.sequencer.egress.NatsIngressTransport} for NATS
 * JetStream. Migration cutover becomes "flip the config," not "adopt a new client library," and
 * rollback is the same flip in reverse — no shim to delete, no shadow-traffic diffing
 * infrastructure required (see the implementation-steps doc's revised Milestone 12 for what that
 * simplifies).
 */
package gcm.md.sequencer.ingress;
