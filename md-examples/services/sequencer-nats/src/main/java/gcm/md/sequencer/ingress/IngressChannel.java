package gcm.md.sequencer.ingress;

/**
 * Pluggable sequencer ingress (project spec §5). Implementations are selected purely by
 * {@code sequencer.ingress.nats.mode} config.
 */
public interface IngressChannel {

    /** Starts delivering messages to {@code handler}, invoked on the ingress client thread. */
    void start(MessageHandler handler);

    /** Drain-stops ingress. Idempotent. */
    void stop();
}
