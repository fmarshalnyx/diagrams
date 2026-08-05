package gcm.md.linehandlertemplate.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * Root binding for the {@code line-handler.*} configuration tree (implementation-steps.md
 * Milestone 5B).
 */
@ConfigurationProperties(prefix = "line-handler")
public class LineHandlerProperties {

    /** {@code aeron} or {@code nats} — selects which {@code IngressTransport} bean is wired. */
    private String ingressTransport = "aeron";

    @NestedConfigurationProperty
    private final Upstream upstream = new Upstream();

    @NestedConfigurationProperty
    private final Aeron aeron = new Aeron();

    @NestedConfigurationProperty
    private final NatsIngress natsIngress = new NatsIngress();

    @NestedConfigurationProperty
    private final Stamping stamping = new Stamping();

    @NestedConfigurationProperty
    private final Offer offer = new Offer();

    public String getIngressTransport() {
        return ingressTransport;
    }

    public void setIngressTransport(String ingressTransport) {
        this.ingressTransport = ingressTransport;
    }

    public Upstream getUpstream() {
        return upstream;
    }

    public Aeron getAeron() {
        return aeron;
    }

    public NatsIngress getNatsIngress() {
        return natsIngress;
    }

    public Stamping getStamping() {
        return stamping;
    }

    public Offer getOffer() {
        return offer;
    }

    /**
     * The mock/real upstream feed this handler consumes from (design: Milestone 5B.2's
     * {@code mock-upstream-source} publishes here) — always used, regardless of
     * {@link #ingressTransport}. A durable JetStream pull consumer, not core NATS: the crash
     * recovery demo (see {@code UpstreamRelay}) relies on durable-consumer redelivery.
     */
    public static class Upstream {
        private String url = "nats://localhost:4222";
        private String stream = "MOCK_UPSTREAM";
        private String subject = "upstream.mock.marketdata";
        private String durableConsumerName = "line-handler-template";
        private int maxFetchBatch = 100;
        private long fetchWaitMillis = 200;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getStream() {
            return stream;
        }

        public void setStream(String stream) {
            this.stream = stream;
        }

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        /** Durable consumer name — stable across restarts so redelivery resumes correctly. */
        public String getDurableConsumerName() {
            return durableConsumerName;
        }

        public void setDurableConsumerName(String durableConsumerName) {
            this.durableConsumerName = durableConsumerName;
        }

        public int getMaxFetchBatch() {
            return maxFetchBatch;
        }

        public void setMaxFetchBatch(int maxFetchBatch) {
            this.maxFetchBatch = maxFetchBatch;
        }

        public long getFetchWaitMillis() {
            return fetchWaitMillis;
        }

        public void setFetchWaitMillis(long fetchWaitMillis) {
            this.fetchWaitMillis = fetchWaitMillis;
        }
    }

    /** Only used when {@link #ingressTransport} is {@code aeron}. */
    public static class Aeron {
        private String ingressChannel = "aeron:udp?term-length=64k";
        private String ingressEndpoints = "0=localhost:9010";
        // term-length is not optional here: without it, Aeron falls back to its default term
        // buffer length (~16MB x 3 terms = ~48MB per publication) for both this client's own
        // subscription and the leader's matching outbound publication (the leader creates its
        // publication using this exact channel string, verbatim, from the SessionConnectRequest).
        // A real bug found running this against a live cluster: with the default 64Mi /dev/shm a
        // pod gets, a single ~48MB response-channel buffer barely fits, and every reconnect
        // leaked another one (visible as growing *.logbuffer files in /dev/shm and on the
        // leader's own aeron dir) until /dev/shm filled up and new allocations silently failed —
        // symptom was ingressPublication connecting fine but egress never did, easily mistaken
        // for a network routing problem. 64k matches the ingress channel above.
        private String egressChannel = "aeron:udp?endpoint=localhost:0|term-length=64k";
        private String credential = "line-handler-template";
        // Aeron's own out-of-the-box default (matches cluster-node's identical knob and the
        // incident that motivated it - see ClusterNodeLauncher's Javadoc): on a resource-
        // constrained shared host, a GC pause or scheduling delay past this window makes this
        // process's own embedded-driver Aeron client conclude the driver died and self-close
        // ("Aeron client is closed", surfacing as permanent relay failure - observed live in this
        // session after ~30h uninterrupted uptime). Environments/local overlays should raise this
        // the same way environments/local/values.yaml raises clusterNode.aeron.driverTimeoutMs.
        private long driverTimeoutMs = 10_000;

        public String getIngressChannel() {
            return ingressChannel;
        }

        public void setIngressChannel(String ingressChannel) {
            this.ingressChannel = ingressChannel;
        }

        /** {@code AeronCluster.Context.ingressEndpoints()} format: {@code "0=host:port,1=host:port,..."}. */
        public String getIngressEndpoints() {
            return ingressEndpoints;
        }

        public void setIngressEndpoints(String ingressEndpoints) {
            this.ingressEndpoints = ingressEndpoints;
        }

        public String getEgressChannel() {
            return egressChannel;
        }

        public void setEgressChannel(String egressChannel) {
            this.egressChannel = egressChannel;
        }

        public String getCredential() {
            return credential;
        }

        public void setCredential(String credential) {
            this.credential = credential;
        }

        public long getDriverTimeoutMs() {
            return driverTimeoutMs;
        }

        public void setDriverTimeoutMs(long driverTimeoutMs) {
            this.driverTimeoutMs = driverTimeoutMs;
        }
    }

    /**
     * Only used when {@link #ingressTransport} is {@code nats}. Deliberately named
     * {@code nats-ingress}, not {@code nats}, so it can't be confused with {@link #upstream}'s
     * own, separate NATS connection.
     */
    public static class NatsIngress {
        private String url = "nats://localhost:4222";
        private String subject = "MD_RAW";
        private int maxInFlight = 10_000;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public int getMaxInFlight() {
            return maxInFlight;
        }

        public void setMaxInFlight(int maxInFlight) {
            this.maxInFlight = maxInFlight;
        }
    }

    /** Where {@code sourceSeqNum} gets stamped — matches {@code StampingConfig}'s v4 default for templateId 9. */
    public static class Stamping {
        private int sourceSeqNumOffset = 64;

        public int getSourceSeqNumOffset() {
            return sourceSeqNumOffset;
        }

        public void setSourceSeqNumOffset(int sourceSeqNumOffset) {
            this.sourceSeqNumOffset = sourceSeqNumOffset;
        }
    }

    /** Retry/backoff tuning for {@code UpstreamRelay}'s offer loop. */
    public static class Offer {
        private long retryParkNanos = 1_000_000; // 1ms
        private int warnEveryNAttempts = 100;

        public long getRetryParkNanos() {
            return retryParkNanos;
        }

        public void setRetryParkNanos(long retryParkNanos) {
            this.retryParkNanos = retryParkNanos;
        }

        public int getWarnEveryNAttempts() {
            return warnEveryNAttempts;
        }

        public void setWarnEveryNAttempts(int warnEveryNAttempts) {
            this.warnEveryNAttempts = warnEveryNAttempts;
        }
    }
}
