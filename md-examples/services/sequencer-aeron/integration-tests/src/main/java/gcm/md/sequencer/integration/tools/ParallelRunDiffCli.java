package gcm.md.sequencer.integration.tools;

import gcm.md.sequencer.stamping.StampingConfig;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PushSubscribeOptions;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * Design §12.4's parallel-run diff harness CLI. Run once, pre-launch, as Milestone 12's (revised)
 * go/no-go validation gate — no live shadow-traffic deploy required, just a dev environment that
 * already has:
 * <ul>
 *   <li>{@code services/sequencer-nats} (phase-1) consuming {@code MD_RAW} and publishing to its
 *       {@code MD_SEQUENCED} subject, exactly as it does in production today;</li>
 *   <li>a line handler configured for the Aeron {@code IngressTransport} (or the loadgen Aeron
 *       mode noted as a Milestone 8 follow-up) → {@code cluster-node} → {@code nats-bridge}
 *       (phase-2) also consuming {@code MD_RAW} — the same JetStream stream/subject, fanned out
 *       to both consumers, is what makes "mirror one input into both paths" true without this
 *       tool publishing twice — and publishing to a distinct subject (never phase-1's own
 *       {@code MD_SEQUENCED} subject, or this tool would corrupt its output).</li>
 * </ul>
 *
 * <p>This class has not been run against a live NATS deployment — see this module's pom.xml
 * confidence note; unlike the {@code *IT} classes, though, its actual join/diff logic is covered
 * by {@link ParallelRunDiffHarnessTest} (no NATS needed for that part), so only the NATS wiring
 * below is unvalidated.
 *
 * <p>Usage: {@code mvn -pl services/sequencer-aeron/integration-tests exec:java
 * -Dexec.mainClass=gcm.md.sequencer.integration.tools.ParallelRunDiffCli
 * -Dexec.args="nats://localhost:4222 tick.sbe.parallelrun md.sequenced md.sequenced.shadow 1000 60"}
 * (natsUrl, inputSubject, phase1OutputSubject, phase2OutputSubject, messageCount, timeoutSeconds
 * — the last two are optional, defaulting to 1000 and 60). Exits non-zero if the run wasn't clean
 * (see {@link DiffReport#isClean()}) so this is CI-gate-friendly once dev NATS access exists.
 */
public final class ParallelRunDiffCli {

    private static final DateTimeFormatter FILENAME_TIMESTAMP = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private ParallelRunDiffCli() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("usage: natsUrl inputSubject phase1OutputSubject phase2OutputSubject [messageCount] [timeoutSeconds]");
            System.exit(2);
            return;
        }
        String natsUrl = args[0];
        String inputSubject = args[1];
        String phase1OutputSubject = args[2];
        String phase2OutputSubject = args[3];
        int messageCount = args.length > 4 ? Integer.parseInt(args[4]) : 1000;
        int timeoutSeconds = args.length > 5 ? Integer.parseInt(args[5]) : 60;

        StampingConfig stampingConfig = StampingConfig.v4Defaults();
        int sourceSeqNumOffset = stampingConfig.sourceTracking().sourceSeqNumOffsetByTemplateId().get(9);

        try (Connection connection = Nats.connect(natsUrl)) {
            JetStream jetStream = connection.jetStream();
            ParallelRunDiffHarness harness = new ParallelRunDiffHarness(stampingConfig);

            JetStreamSubscription phase1Sub = jetStream.subscribe(phase1OutputSubject,
                    PushSubscribeOptions.builder().build());
            JetStreamSubscription phase2Sub = jetStream.subscribe(phase2OutputSubject,
                    PushSubscribeOptions.builder().build());

            publishSyntheticInput(jetStream, inputSubject, stampingConfig, sourceSeqNumOffset, messageCount);

            drainUntilTimeoutOrComplete(phase1Sub, phase2Sub, harness, sourceSeqNumOffset, messageCount, timeoutSeconds);

            DiffReport report = harness.finish();
            String summary = report.toSummaryText();
            System.out.println(summary);

            Path reportPath = writeReport(summary);
            System.out.println("Report written to " + reportPath);

            System.exit(report.isClean() ? 0 : 1);
        }
    }

    private static void publishSyntheticInput(JetStream jetStream, String inputSubject,
                                                StampingConfig stampingConfig, int sourceSeqNumOffset,
                                                int messageCount) throws Exception {
        for (long sourceSeqNum = 1; sourceSeqNum <= messageCount; sourceSeqNum++) {
            byte[] payload = new byte[128];
            org.agrona.concurrent.UnsafeBuffer buffer = new org.agrona.concurrent.UnsafeBuffer(payload);
            buffer.putShort(stampingConfig.templateIdOffset(), (short) 9, ByteOrder.LITTLE_ENDIAN);
            buffer.putShort(stampingConfig.schemaIdOffset(), (short) stampingConfig.schemaId(), ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(sourceSeqNumOffset, (int) sourceSeqNum, ByteOrder.LITTLE_ENDIAN);
            jetStream.publish(inputSubject, payload);
        }
    }

    private static void drainUntilTimeoutOrComplete(JetStreamSubscription phase1Sub, JetStreamSubscription phase2Sub,
                                                      ParallelRunDiffHarness harness, int sourceSeqNumOffset,
                                                      int messageCount, int timeoutSeconds) throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(timeoutSeconds);
        int matched = 0;
        while (Instant.now().isBefore(deadline) && matched < messageCount) {
            matched += pollOnce(phase1Sub, harness::onPhase1Message, sourceSeqNumOffset);
            matched += pollOnce(phase2Sub, harness::onPhase2Message, sourceSeqNumOffset);
        }
    }

    private static int pollOnce(JetStreamSubscription subscription,
                                 java.util.function.Consumer<CapturedMessage> sink,
                                 int sourceSeqNumOffset) throws InterruptedException {
        Message message = subscription.nextMessage(Duration.ofMillis(50));
        if (message == null) {
            return 0;
        }
        byte[] payload = message.getData();
        org.agrona.concurrent.UnsafeBuffer buffer = new org.agrona.concurrent.UnsafeBuffer(payload);
        long sourceSeqNum = buffer.getInt(sourceSeqNumOffset, ByteOrder.LITTLE_ENDIAN) & 0xFFFFFFFFL;
        sink.accept(new CapturedMessage(sourceSeqNum, payload, Instant.now()));
        message.ack();
        return 1;
    }

    private static Path writeReport(String summary) throws IOException {
        Path dir = Path.of("docs/migration");
        Files.createDirectories(dir);
        Path file = dir.resolve("parallel-run-" + FILENAME_TIMESTAMP.format(java.time.LocalDateTime.now()) + ".txt");
        Files.writeString(file, summary, StandardCharsets.UTF_8);
        return file;
    }
}
