package gcm.md.sequencer.ingress;

import gcm.md.sequencer.config.SequencerProperties;
import gcm.md.sequencer.egress.DestinationChannel;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

/**
 * NATS ingress (project spec §5). JetStream mode uses a pull consumer bound to a named durable —
 * the server-side single-active-consumer constraint doubles as a fencing layer (§10) — and
 * batch-fetches to amortize overhead. Core mode is plain at-most-once subscription for fast-path
 * inputs.
 *
 * <p>The JetStream pull loop IS the stamping thread: one dedicated thread fetches a batch,
 * synchronously invokes the handler (which stamps + hands off to egress) for every message in
 * it, drains egress in-flight state, then acks the whole batch — satisfying "ack AFTER egress
 * publish is confirmed" (§5) without any queues or thread handoffs (§4).
 */
public final class NatsSequencerIngress implements IngressChannel {

    private final Connection connection;
    private final SequencerProperties.Ingress.Nats config;
    private final DestinationChannel destination;

    private volatile boolean running;
    private Dispatcher dispatcher;
    private Thread pullThread;

    /**
     * Creates the ingress. {@code destination} is used only to gate JetStream batch acks on
     * egress confirmation (spec §5) — messages themselves flow through the {@link MessageHandler}
     * passed to {@link #start}, never directly through this reference.
     */
    public NatsSequencerIngress(Connection connection, SequencerProperties properties,
                                 DestinationChannel destination) {
        this.connection = connection;
        this.config = properties.getIngress().getNats();
        this.destination = destination;
    }

    @Override
    public void start(MessageHandler handler) {
        running = true;
        if ("core".equalsIgnoreCase(config.getMode())) {
            startCore(handler);
        } else {
            startJetStream(handler);
        }
    }

    private void startCore(MessageHandler handler) {
        dispatcher = connection.createDispatcher(msg -> handler.onMessage(msg.getData()));
        dispatcher.subscribe(config.getSubject());
    }

    private void startJetStream(MessageHandler handler) {
        try {
            JetStream jetStream = connection.jetStream();
            PullSubscribeOptions options = PullSubscribeOptions.builder()
                    .durable(config.getConsumer())
                    .stream(config.getStream())
                    .build();
            JetStreamSubscription subscription = jetStream.subscribe(config.getSubject(), options);
            pullThread = new Thread(() -> pullLoop(subscription, handler), "sequencer-ingress-pull");
            pullThread.setDaemon(true);
            pullThread.start();
        } catch (IOException | JetStreamApiException e) {
            throw new IllegalStateException("Failed to start JetStream ingress pull consumer", e);
        }
    }

    private void pullLoop(JetStreamSubscription subscription, MessageHandler handler) {
        while (running) {
            List<Message> batch = subscription.fetch(config.getBatchSize(), Duration.ofSeconds(5));
            if (batch.isEmpty()) {
                continue;
            }
            for (Message message : batch) {
                handler.onMessage(message.getData());
            }
            // Redelivery on crash between receive and publish is expected and documented: a
            // redelivered message gets a NEW sequenceId (spec §5). Downstream dedupe key is
            // (source-scope) instrumentId + sourceSeqNum.
            destination.awaitInFlightDrained();
            for (Message message : batch) {
                message.ack();
            }
        }
    }

    @Override
    public void stop() {
        running = false;
        if (pullThread != null) {
            pullThread.interrupt();
        }
        if (dispatcher != null) {
            connection.closeDispatcher(dispatcher);
        }
    }
}
