package gcm.md.mockupstreamsource.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Root binding for the {@code mock-upstream-source.*} configuration tree (implementation-steps.md
 * Milestone 5B). Flat (no nested classes): unlike {@code line-handler-template}/{@code
 * nats-bridge}, this service only ever needs one NATS connection, used for both generating and
 * verifying.
 */
@ConfigurationProperties(prefix = "mock-upstream-source")
public class MockUpstreamSourceProperties {

    private String url = "nats://localhost:4222";
    private String stream = "MOCK_UPSTREAM";
    private String subject = "upstream.mock.marketdata";
    private String egressSubject = "md.sequenced";
    private long rate = 100_000;
    private String pattern = "steady";
    private double gapProbability = 0.0;
    private double duplicateProbability = 0.0;
    private long seed = 0;
    private boolean batched = false;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    /** JetStream stream name; created idempotently on startup if it doesn't already exist. */
    public String getStream() {
        return stream;
    }

    public void setStream(String stream) {
        this.stream = stream;
    }

    /** Published to continuously — what a line handler (or a sequencer directly) consumes from. */
    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    /** The sequencer's final observed output subject, continuously verified. */
    public String getEgressSubject() {
        return egressSubject;
    }

    public void setEgressSubject(String egressSubject) {
        this.egressSubject = egressSubject;
    }

    /** Target publish rate, messages/sec (peak rate during burst windows if {@code pattern=bursty}). */
    public long getRate() {
        return rate;
    }

    public void setRate(long rate) {
        this.rate = rate;
    }

    /** {@code steady} or {@code bursty} — see {@code TrafficPattern}. */
    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    /** Per-tick probability of a deliberate, un-sent gap in {@code sourceSeqNum}. */
    public double getGapProbability() {
        return gapProbability;
    }

    public void setGapProbability(double gapProbability) {
        this.gapProbability = gapProbability;
    }

    /** Per-tick probability of a deliberate byte-identical re-publish. */
    public double getDuplicateProbability() {
        return duplicateProbability;
    }

    public void setDuplicateProbability(double duplicateProbability) {
        this.duplicateProbability = duplicateProbability;
    }

    /** RNG seed for gap/duplicate decisions — same seed reproduces the identical run. */
    public long getSeed() {
        return seed;
    }

    public void setSeed(long seed) {
        this.seed = seed;
    }

    /** Whether to expect {@code MessageBatch} envelopes on the egress subject. */
    public boolean isBatched() {
        return batched;
    }

    public void setBatched(boolean batched) {
        this.batched = batched;
    }
}
