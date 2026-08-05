package gcm.md.natsbridge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * Root binding for the {@code nats-bridge.*} configuration tree (design §9).
 */
@ConfigurationProperties(prefix = "nats-bridge")
public class NatsBridgeProperties {

    @NestedConfigurationProperty
    private final Nats nats = new Nats();

    @NestedConfigurationProperty
    private final Cluster cluster = new Cluster();

    @NestedConfigurationProperty
    private final Stamping stamping = new Stamping();

    @NestedConfigurationProperty
    private final Batching batching = new Batching();

    public Nats getNats() {
        return nats;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Stamping getStamping() {
        return stamping;
    }

    public Batching getBatching() {
        return batching;
    }

    /** NATS republish destination and checkpoint storage (design §9). */
    public static class Nats {
        private String url = "nats://localhost:4222";
        private String stream = "MD_SEQUENCED";
        private String subject = "md.sequenced";
        private String checkpointKvBucket = "bridge-checkpoint";
        private String checkpointKvKey = "last-bridged-sequence-id";
        private int checkpointIntervalMessages = 1000;
        // ContiguityTracker's checkpoint-reset recovery threshold (see its class Javadoc): a
        // heuristic "give up and assume the checkpoint is stale" cutoff, not a proof - too low
        // risks rebasing mid-legitimate-replay and duplicate-publishing already-bridged messages,
        // too high delays recovery from a genuine reset. 100,000 is deliberately well above
        // checkpointIntervalMessages (the live-operation checkpoint-write cadence) since replay
        // backlog is bounded by Archive recording retention, not by that cadence, and could
        // legitimately be large after extended bridge downtime.
        private int checkpointResetThresholdMessages = 100_000;
        private long maxStallMs = 500;
        private int jetstreamMaxInFlight = 8192;

        /** Returns the NATS server URL to connect to for republishing. */
        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        /** Returns the JetStream stream republished to. */
        public String getStream() {
            return stream;
        }

        public void setStream(String stream) {
            this.stream = stream;
        }

        /** Returns the base subject; {@code .batch} is appended automatically when batching is enabled. */
        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        /** Returns the NATS KV bucket holding the last-bridged-sequenceId checkpoint (design §9). */
        public String getCheckpointKvBucket() {
            return checkpointKvBucket;
        }

        public void setCheckpointKvBucket(String checkpointKvBucket) {
            this.checkpointKvBucket = checkpointKvBucket;
        }

        /** Returns the KV key within {@link #getCheckpointKvBucket()} holding the checkpoint. */
        public String getCheckpointKvKey() {
            return checkpointKvKey;
        }

        public void setCheckpointKvKey(String checkpointKvKey) {
            this.checkpointKvKey = checkpointKvKey;
        }

        /** Returns how many bridged messages elapse between checkpoint writes (bounds KV write rate). */
        public int getCheckpointIntervalMessages() {
            return checkpointIntervalMessages;
        }

        public void setCheckpointIntervalMessages(int checkpointIntervalMessages) {
            this.checkpointIntervalMessages = checkpointIntervalMessages;
        }

        /** Returns the consecutive-skip threshold before the bridge's contiguity tracker rebases a stale checkpoint. */
        public int getCheckpointResetThresholdMessages() {
            return checkpointResetThresholdMessages;
        }

        public void setCheckpointResetThresholdMessages(int checkpointResetThresholdMessages) {
            this.checkpointResetThresholdMessages = checkpointResetThresholdMessages;
        }

        /** Returns the max stall duration before a JetStream publish backpressure stall alarms. */
        public long getMaxStallMs() {
            return maxStallMs;
        }

        public void setMaxStallMs(long maxStallMs) {
            this.maxStallMs = maxStallMs;
        }

        /** Returns the bounded async publish-ack window size for the JetStream destination. */
        public int getJetstreamMaxInFlight() {
            return jetstreamMaxInFlight;
        }

        public void setJetstreamMaxInFlight(int jetstreamMaxInFlight) {
            this.jetstreamMaxInFlight = jetstreamMaxInFlight;
        }
    }

    /**
     * Aeron egress subscription settings (design §9). {@code replayDestination}/{@code
     * replayChannel} follow {@code io.aeron.archive.client.ReplayMerge}'s contract: the
     * subscription itself must use {@code control-mode=manual}.
     */
    public static class Cluster {
        // One archive control channel per cluster member, comma-separated — NOT a single bare
        // headless-Service DNS name. Archive control connections have no AeronCluster-style
        // automatic leader-following (libs/cluster-client's ingress protocol does; a plain
        // point-to-point Archive session doesn't), and only the current leader's local archive
        // ever has the egress recording (SuppressionGate only lets the leader publish). A single
        // fixed connection resolved via round-robin DNS can land on a follower's empty archive —
        // a real, previously-shipped bug, see docs/AERON-SEQUENCER-3-MEMBER-CLUSTER-STATUS.md.
        // LeaderArchiveConnector tries each of these in turn and keeps whichever one actually has
        // the matching recording.
        private String archiveControlChannels = "aeron:udp?endpoint=localhost:9050";
        private String archiveControlResponseChannel = "aeron:udp?endpoint=localhost:9051";
        private String subscriptionChannel = "aeron:udp?control-mode=manual";
        // Must match cluster-node's AeronEgressConfig egress channel format exactly (control=,
        // not endpoint=) — this is a dynamic-MDC publisher, and this string doubles as the
        // Archive.findLastMatchingRecording channel fragment, which must be a substring of what
        // AeronEgressPublisher actually recorded under.
        private String liveDestination = "aeron:udp?control=localhost:9070|control-mode=dynamic";
        private String replayDestination = "aeron:udp?endpoint=localhost:9081";
        private String replayChannel = "aeron:udp?endpoint=localhost:9081";
        private int egressStreamId = 1;

        /** Comma-separated, one entry per cluster member — see the field's own comment for why. */
        public String getArchiveControlChannels() {
            return archiveControlChannels;
        }

        public void setArchiveControlChannels(String archiveControlChannels) {
            this.archiveControlChannels = archiveControlChannels;
        }

        public String getArchiveControlResponseChannel() {
            return archiveControlResponseChannel;
        }

        public void setArchiveControlResponseChannel(String archiveControlResponseChannel) {
            this.archiveControlResponseChannel = archiveControlResponseChannel;
        }

        /** Returns the multi-destination subscription channel (must include {@code control-mode=manual}). */
        public String getSubscriptionChannel() {
            return subscriptionChannel;
        }

        public void setSubscriptionChannel(String subscriptionChannel) {
            this.subscriptionChannel = subscriptionChannel;
        }

        /** Returns the destination added to the subscription for the live egress stream. */
        public String getLiveDestination() {
            return liveDestination;
        }

        public void setLiveDestination(String liveDestination) {
            this.liveDestination = liveDestination;
        }

        /** Returns the destination the archive sends replayed data to. */
        public String getReplayDestination() {
            return replayDestination;
        }

        public void setReplayDestination(String replayDestination) {
            this.replayDestination = replayDestination;
        }

        /** Returns the channel template used to request a replay from the archive. */
        public String getReplayChannel() {
            return replayChannel;
        }

        public void setReplayChannel(String replayChannel) {
            this.replayChannel = replayChannel;
        }

        /** Returns the egress stream id (must match {@code AeronEgressConfig.egressStreamId} on cluster-node). */
        public int getEgressStreamId() {
            return egressStreamId;
        }

        public void setEgressStreamId(int egressStreamId) {
            this.egressStreamId = egressStreamId;
        }
    }

    /** Byte-offset contract for reading {@code sequenceId} out of raw egress fragments (design §3). */
    public static class Stamping {
        private int sequenceIdOffset = 8;
        private int schemaId = 100;
        private int schemaIdOffset = 4;

        public int getSequenceIdOffset() {
            return sequenceIdOffset;
        }

        public void setSequenceIdOffset(int sequenceIdOffset) {
            this.sequenceIdOffset = sequenceIdOffset;
        }

        public int getSchemaId() {
            return schemaId;
        }

        public void setSchemaId(int schemaId) {
            this.schemaId = schemaId;
        }

        public int getSchemaIdOffset() {
            return schemaIdOffset;
        }

        public void setSchemaIdOffset(int schemaIdOffset) {
            this.schemaIdOffset = schemaIdOffset;
        }
    }

    /** {@code MessageBatch} framing config for {@code libs/nats-egress}'s {@code BatchingDestination} (design §9). */
    public static class Batching {
        private boolean enabled = true;
        private int maxMessages = 100;
        private int maxBytes = 65536;
        private long maxLingerMicros = 1000;
        private boolean flushOnEventBoundary = true;
        private int templateIdOffset = 2;
        private int boundaryTemplateId = 6;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getMaxMessages() {
            return maxMessages;
        }

        public void setMaxMessages(int maxMessages) {
            this.maxMessages = maxMessages;
        }

        public int getMaxBytes() {
            return maxBytes;
        }

        public void setMaxBytes(int maxBytes) {
            this.maxBytes = maxBytes;
        }

        public long getMaxLingerMicros() {
            return maxLingerMicros;
        }

        public void setMaxLingerMicros(long maxLingerMicros) {
            this.maxLingerMicros = maxLingerMicros;
        }

        public boolean isFlushOnEventBoundary() {
            return flushOnEventBoundary;
        }

        public void setFlushOnEventBoundary(boolean flushOnEventBoundary) {
            this.flushOnEventBoundary = flushOnEventBoundary;
        }

        public int getTemplateIdOffset() {
            return templateIdOffset;
        }

        public void setTemplateIdOffset(int templateIdOffset) {
            this.templateIdOffset = templateIdOffset;
        }

        public int getBoundaryTemplateId() {
            return boundaryTemplateId;
        }

        public void setBoundaryTemplateId(int boundaryTemplateId) {
            this.boundaryTemplateId = boundaryTemplateId;
        }
    }
}
