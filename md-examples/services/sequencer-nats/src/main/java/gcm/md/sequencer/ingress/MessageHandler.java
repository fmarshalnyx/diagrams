package gcm.md.sequencer.ingress;

/** Invoked on the ingress client thread for every received message; may mutate {@code data} in place. */
@FunctionalInterface
public interface MessageHandler {

    /** Handles one raw SBE-encoded message. Called synchronously on the ingress client thread. */
    void onMessage(byte[] data);
}
