package gcm.md.sequencer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.util.List;

/**
 * Root binding for the {@code sequencer.*} configuration tree (see application.yml).
 * All hot-path constants (offsets, thresholds, feature flags) live here so the
 * stamping/allocation/egress code never hardcodes a byte offset or template id.
 */
@ConfigurationProperties(prefix = "sequencer")
public class SequencerProperties {

    @NestedConfigurationProperty
    private final Stamping stamping = new Stamping();

    @NestedConfigurationProperty
    private final Allocator allocator = new Allocator();

    @NestedConfigurationProperty
    private final Ingress ingress = new Ingress();

    @NestedConfigurationProperty
    private final Egress egress = new Egress();

    @NestedConfigurationProperty
    private final Heartbeat heartbeat = new Heartbeat();

    @NestedConfigurationProperty
    private final Leadership leadership = new Leadership();

    /** Returns the SBE stamping configuration (offsets, profile, event enrichment). */
    public Stamping getStamping() {
        return stamping;
    }

    /** Returns the sequence block-leasing configuration. */
    public Allocator getAllocator() {
        return allocator;
    }

    /** Returns the ingress channel configuration. */
    public Ingress getIngress() {
        return ingress;
    }

    /** Returns the egress channel configuration. */
    public Egress getEgress() {
        return egress;
    }

    /** Returns the sequencer heartbeat configuration. */
    public Heartbeat getHeartbeat() {
        return heartbeat;
    }

    /** Returns the Kubernetes leader-election configuration. */
    public Leadership getLeadership() {
        return leadership;
    }

    /** SBE stamping profile and byte-offset contract; see schema §3 and md-models-sbe-v4.xml. */
    public static class Stamping {
        private String profile = "v4";
        private int sequenceIdOffset = 8;
        private int sequenceTimestampOffset = 32;
        private int schemaId = 100;
        private int schemaIdOffset = 4;
        private int templateIdOffset = 2;
        private boolean validateSchemaIdPerMessage = false;
        private List<Integer> timestampTemplateIds = List.of(1, 9);

        @NestedConfigurationProperty
        private final EventEnrichment eventEnrichment = new EventEnrichment();

        /** Returns the stamping profile: {@code v4} (default, unconditional stamping) or {@code v3} (compatibility mode). */
        public String getProfile() {
            return profile;
        }

        public void setProfile(String profile) {
            this.profile = profile;
        }

        /** Returns the absolute byte offset the sequencer writes sequenceId to. */
        public int getSequenceIdOffset() {
            return sequenceIdOffset;
        }

        public void setSequenceIdOffset(int sequenceIdOffset) {
            this.sequenceIdOffset = sequenceIdOffset;
        }

        /** Returns the absolute byte offset the sequencer writes sequenceTimestamp to. */
        public int getSequenceTimestampOffset() {
            return sequenceTimestampOffset;
        }

        public void setSequenceTimestampOffset(int sequenceTimestampOffset) {
            this.sequenceTimestampOffset = sequenceTimestampOffset;
        }

        /** Returns the expected SBE schemaId (v4 = 100), verified as a sanity guard. */
        public int getSchemaId() {
            return schemaId;
        }

        public void setSchemaId(int schemaId) {
            this.schemaId = schemaId;
        }

        /** Returns the absolute byte offset of the schemaId field in the SBE messageHeader. */
        public int getSchemaIdOffset() {
            return schemaIdOffset;
        }

        public void setSchemaIdOffset(int schemaIdOffset) {
            this.schemaIdOffset = schemaIdOffset;
        }

        /** Returns the absolute byte offset of the templateId field in the SBE messageHeader. */
        public int getTemplateIdOffset() {
            return templateIdOffset;
        }

        public void setTemplateIdOffset(int templateIdOffset) {
            this.templateIdOffset = templateIdOffset;
        }

        /** Returns whether the schemaId sanity guard runs on every message (default: first message only). */
        public boolean isValidateSchemaIdPerMessage() {
            return validateSchemaIdPerMessage;
        }

        public void setValidateSchemaIdPerMessage(boolean validateSchemaIdPerMessage) {
            this.validateSchemaIdPerMessage = validateSchemaIdPerMessage;
        }

        /** Returns the template ids stamped at {@link #getSequenceTimestampOffset()} under the v3 profile. */
        public List<Integer> getTimestampTemplateIds() {
            return timestampTemplateIds;
        }

        public void setTimestampTemplateIds(List<Integer> timestampTemplateIds) {
            this.timestampTemplateIds = timestampTemplateIds;
        }

        /** Returns the MatchEventBoundary enrichment configuration. */
        public EventEnrichment getEventEnrichment() {
            return eventEnrichment;
        }
    }

    /** MatchEventBoundary firstSequenceId/lastSequenceId enrichment (schema §3, §8). */
    public static class EventEnrichment {
        private boolean enabled = true;
        private int eventIdOffset = 40;
        private int boundaryTemplateId = 6;
        private int firstSequenceIdOffset = 56;
        private int lastSequenceIdOffset = 64;
        private int maxTrackedEvents = 65536;

        /** Returns whether MatchEventBoundary firstSequenceId/lastSequenceId enrichment is enabled. */
        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        /** Returns the absolute byte offset of gcmHeader.eventId. */
        public int getEventIdOffset() {
            return eventIdOffset;
        }

        public void setEventIdOffset(int eventIdOffset) {
            this.eventIdOffset = eventIdOffset;
        }

        /** Returns the SBE templateId of MatchEventBoundary. */
        public int getBoundaryTemplateId() {
            return boundaryTemplateId;
        }

        public void setBoundaryTemplateId(int boundaryTemplateId) {
            this.boundaryTemplateId = boundaryTemplateId;
        }

        /** Returns the absolute byte offset firstSequenceId is stamped at. */
        public int getFirstSequenceIdOffset() {
            return firstSequenceIdOffset;
        }

        public void setFirstSequenceIdOffset(int firstSequenceIdOffset) {
            this.firstSequenceIdOffset = firstSequenceIdOffset;
        }

        /** Returns the absolute byte offset lastSequenceId is stamped at. */
        public int getLastSequenceIdOffset() {
            return lastSequenceIdOffset;
        }

        public void setLastSequenceIdOffset(int lastSequenceIdOffset) {
            this.lastSequenceIdOffset = lastSequenceIdOffset;
        }

        /** Returns the bound on the concurrently-tracked eventId to firstSeq map before eviction. */
        public int getMaxTrackedEvents() {
            return maxTrackedEvents;
        }

        public void setMaxTrackedEvents(int maxTrackedEvents) {
            this.maxTrackedEvents = maxTrackedEvents;
        }
    }

    /** High-water-mark block-leasing configuration (schema §6). */
    public static class Allocator {
        private long blockSize = 1_000_000L;
        private double leaseAheadFraction = 0.8;
        private String kvBucket = "sequencer-lease";
        private String kvKey = "high-water";

        /** Returns the number of sequenceIds leased per KV compare-and-swap. */
        public long getBlockSize() {
            return blockSize;
        }

        public void setBlockSize(long blockSize) {
            this.blockSize = blockSize;
        }

        /** Returns the fraction of a block consumed before the next block is proactively leased. */
        public double getLeaseAheadFraction() {
            return leaseAheadFraction;
        }

        public void setLeaseAheadFraction(double leaseAheadFraction) {
            this.leaseAheadFraction = leaseAheadFraction;
        }

        /** Returns the NATS KV bucket holding the leased-up-to high-water mark. */
        public String getKvBucket() {
            return kvBucket;
        }

        public void setKvBucket(String kvBucket) {
            this.kvBucket = kvBucket;
        }

        /** Returns the KV key holding the leased-up-to high-water mark. */
        public String getKvKey() {
            return kvKey;
        }

        public void setKvKey(String kvKey) {
            this.kvKey = kvKey;
        }
    }

    /** Ingress channel configuration (schema §5). */
    public static class Ingress {
        @NestedConfigurationProperty
        private final Nats nats = new Nats();

        /** Returns the NATS ingress configuration. */
        public Nats getNats() {
            return nats;
        }

        public static class Nats {
            private String url = "nats://localhost:4222";
            private String mode = "jetstream";
            private String stream = "MD_RAW";
            private String subject = "tick.sbe.>";
            private String consumer = "sequencer";
            private int batchSize = 1000;

            /** Returns the NATS server URL to connect to for ingress. */
            public String getUrl() {
                return url;
            }

            public void setUrl(String url) {
                this.url = url;
            }

            /** Returns the ingress mode: {@code jetstream} (durable pull consumer) or {@code core} (at-most-once). */
            public String getMode() {
                return mode;
            }

            public void setMode(String mode) {
                this.mode = mode;
            }

            /** Returns the JetStream stream name to consume from. */
            public String getStream() {
                return stream;
            }

            public void setStream(String stream) {
                this.stream = stream;
            }

            /** Returns the subject (or wildcard) subscribed to. */
            public String getSubject() {
                return subject;
            }

            public void setSubject(String subject) {
                this.subject = subject;
            }

            /** Returns the named durable JetStream consumer name (fencing layer, schema §10). */
            public String getConsumer() {
                return consumer;
            }

            public void setConsumer(String consumer) {
                this.consumer = consumer;
            }

            /** Returns the number of messages fetched per JetStream pull batch. */
            public int getBatchSize() {
                return batchSize;
            }

            public void setBatchSize(int batchSize) {
                this.batchSize = batchSize;
            }
        }
    }

    /** Egress channel configuration (schema §7, §8, §9). */
    public static class Egress {
        private String type = "jetstream";
        private String backpressure = "block";
        private long maxStallMs = 500;

        @NestedConfigurationProperty
        private final Nats nats = new Nats();

        @NestedConfigurationProperty
        private final Jetstream jetstream = new Jetstream();

        @NestedConfigurationProperty
        private final Batching batching = new Batching();

        /** Returns the destination type: {@code core} or {@code jetstream}. */
        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        /** Returns the backpressure policy: {@code block} (stall + propagate) or {@code drop} (never stall). */
        public String getBackpressure() {
            return backpressure;
        }

        public void setBackpressure(String backpressure) {
            this.backpressure = backpressure;
        }

        /** Returns the max stall duration before {@code sequencer_backpressure_stall_seconds} alarms. */
        public long getMaxStallMs() {
            return maxStallMs;
        }

        public void setMaxStallMs(long maxStallMs) {
            this.maxStallMs = maxStallMs;
        }

        /** Returns the NATS egress connection/subject configuration. */
        public Nats getNats() {
            return nats;
        }

        /** Returns the JetStream-specific egress configuration. */
        public Jetstream getJetstream() {
            return jetstream;
        }

        /** Returns the batching decorator configuration. */
        public Batching getBatching() {
            return batching;
        }

        public static class Nats {
            private String url = "nats://localhost:4222";
            private String stream = "MD_SEQUENCED";
            private String subject = "md.sequenced";

            /** Returns the NATS server URL to connect to for egress. */
            public String getUrl() {
                return url;
            }

            public void setUrl(String url) {
                this.url = url;
            }

            /** Returns the JetStream stream name published to (jetstream egress only). */
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
        }

        public static class Jetstream {
            private int maxInFlight = 8192;

            /** Returns the bounded in-flight JetStream publish-ack window size. */
            public int getMaxInFlight() {
                return maxInFlight;
            }

            public void setMaxInFlight(int maxInFlight) {
                this.maxInFlight = maxInFlight;
            }
        }

        public static class Batching {
            private boolean enabled = true;
            private int maxMessages = 100;
            private int maxBytes = 65536;
            private long maxLingerMicros = 1000;
            private boolean flushOnEventBoundary = true;

            /** Returns whether the {@link gcm.md.sequencer.egress.BatchingDestination} decorator is applied. */
            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            /** Returns the max messages per batch before a flush is forced. */
            public int getMaxMessages() {
                return maxMessages;
            }

            public void setMaxMessages(int maxMessages) {
                this.maxMessages = maxMessages;
            }

            /** Returns the max packed bytes per batch before a flush is forced. */
            public int getMaxBytes() {
                return maxBytes;
            }

            public void setMaxBytes(int maxBytes) {
                this.maxBytes = maxBytes;
            }

            /** Returns the max linger duration (microseconds) before a partial batch is flushed. */
            public long getMaxLingerMicros() {
                return maxLingerMicros;
            }

            public void setMaxLingerMicros(long maxLingerMicros) {
                this.maxLingerMicros = maxLingerMicros;
            }

            /** Returns whether a batch is force-flushed immediately after a MatchEventBoundary. */
            public boolean isFlushOnEventBoundary() {
                return flushOnEventBoundary;
            }

            public void setFlushOnEventBoundary(boolean flushOnEventBoundary) {
                this.flushOnEventBoundary = flushOnEventBoundary;
            }
        }
    }

    /** Sequencer-emitted heartbeat configuration (schema §8). */
    public static class Heartbeat {
        private boolean enabled = true;
        private long intervalMs = 100;
        private String sourceId = "SEQR";

        /** Returns whether the sequencer emits its own high-water-mark heartbeat. */
        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        /** Returns the heartbeat emission interval in milliseconds. */
        public long getIntervalMs() {
            return intervalMs;
        }

        public void setIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
        }

        /** Returns the {@code source} field value stamped into sequencer heartbeats. */
        public String getSourceId() {
            return sourceId;
        }

        public void setSourceId(String sourceId) {
            this.sourceId = sourceId;
        }
    }

    /** Kubernetes Lease-based leader election configuration (schema §10). */
    public static class Leadership {
        private String leaseName = "gcm-md-sequencer";
        private String leaseNamespace = "market-data";
        private int leaseDurationSeconds = 10;
        private int renewDeadlineSeconds = 7;
        private int retryPeriodSeconds = 2;

        /** Returns the Kubernetes Lease object name used for leader election. */
        public String getLeaseName() {
            return leaseName;
        }

        public void setLeaseName(String leaseName) {
            this.leaseName = leaseName;
        }

        /** Returns the namespace the Lease object lives in. */
        public String getLeaseNamespace() {
            return leaseNamespace;
        }

        public void setLeaseNamespace(String leaseNamespace) {
            this.leaseNamespace = leaseNamespace;
        }

        /** Returns how long a held lease remains valid without renewal. */
        public int getLeaseDurationSeconds() {
            return leaseDurationSeconds;
        }

        public void setLeaseDurationSeconds(int leaseDurationSeconds) {
            this.leaseDurationSeconds = leaseDurationSeconds;
        }

        /** Returns the deadline within which the leader must renew before losing the lease. */
        public int getRenewDeadlineSeconds() {
            return renewDeadlineSeconds;
        }

        public void setRenewDeadlineSeconds(int renewDeadlineSeconds) {
            this.renewDeadlineSeconds = renewDeadlineSeconds;
        }

        /** Returns how often the elector retries acquiring/renewing the lease. */
        public int getRetryPeriodSeconds() {
            return retryPeriodSeconds;
        }

        public void setRetryPeriodSeconds(int retryPeriodSeconds) {
            this.retryPeriodSeconds = retryPeriodSeconds;
        }
    }
}
