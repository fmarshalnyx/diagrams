package gcm.md.linehandlertemplate.config;

import gcm.md.linehandlertemplate.metrics.LineHandlerMetrics;
import gcm.md.linehandlertemplate.relay.SourceSeqNumStamper;
import gcm.md.linehandlertemplate.relay.UpstreamRelay;
import gcm.md.sequencer.clusterclient.ClusterIngressClient;
import gcm.md.sequencer.clusterclient.IngressClientConfig;
import gcm.md.sequencer.clusterclient.SourcePrincipalCredentialsSupplier;
import gcm.md.sequencer.egress.NatsIngressConfig;
import gcm.md.sequencer.egress.NatsIngressTransport;
import gcm.md.sequencer.ingress.IngressTransport;
import io.aeron.Aeron;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Sole source of bean definitions for {@code line-handler-template}, per project convention: no
 * {@code @Autowired}, no stereotype-annotated business classes — every collaborator is wired
 * explicitly here.
 *
 * <p><b>Transport selection</b> is the one thing this service does that no other service in this
 * reactor demonstrates yet: exactly one {@code IngressTransport} bean is registered, chosen by
 * {@code line-handler.ingress-transport} via {@code @ConditionalOnProperty}. The two transport
 * bean groups below are mutually exclusive by construction — {@code aeron}'s beans use {@code
 * havingValue = "aeron", matchIfMissing = true} (so an unset property defaults to Aeron, matching
 * {@link LineHandlerProperties}'s own default), {@code nats}'s beans use {@code havingValue =
 * "nats"} with no {@code matchIfMissing}. Exactly one of the two groups is ever active, so {@link
 * UpstreamRelay}'s constructor can inject {@code IngressTransport} by type with no ambiguity and
 * no {@code @Qualifier} needed.
 */
@Configuration
@EnableConfigurationProperties(LineHandlerProperties.class)
public class ServiceConfiguration {

    // ---- Upstream feed (5B.2's mock-upstream-source) — always wired, independent of transport ----

    /** NATS connection to the upstream mock/real feed — distinct from any ingress-transport NATS connection below. */
    @Bean(destroyMethod = "close")
    public Connection upstreamNatsConnection(LineHandlerProperties properties) throws IOException, InterruptedException {
        return Nats.connect(properties.getUpstream().getUrl());
    }

    @Bean
    public JetStream upstreamJetStream(Connection upstreamNatsConnection) throws IOException {
        return upstreamNatsConnection.jetStream();
    }

    @Bean
    public SourceSeqNumStamper sourceSeqNumStamper(LineHandlerProperties properties) {
        return new SourceSeqNumStamper(properties.getStamping().getSourceSeqNumOffset());
    }

    @Bean
    public LineHandlerMetrics lineHandlerMetrics(MeterRegistry registry) {
        return new LineHandlerMetrics(registry);
    }

    @Bean
    public UpstreamRelay upstreamRelay(JetStream upstreamJetStream, LineHandlerProperties properties,
                                        IngressTransport ingressTransport, SourceSeqNumStamper sourceSeqNumStamper,
                                        LineHandlerMetrics lineHandlerMetrics) {
        return new UpstreamRelay(upstreamJetStream, properties, ingressTransport, sourceSeqNumStamper,
                lineHandlerMetrics);
    }

    // ---- Aeron cluster transport (line-handler.ingress-transport=aeron, the default) ----

    /**
     * A lightweight embedded media driver so this process's {@code AeronCluster} client has
     * something to attach to — {@code AeronCluster.connect()} does not launch one implicitly
     * (see {@code libs/cluster-client}'s note: every {@code IngressTransport} consumer needs
     * this, mirroring what {@code nats-bridge} already does for its own Aeron client).
     *
     * <p>{@code driverTimeoutMs}/{@code clientLivenessTimeoutNs} are raised from Aeron's 10s
     * default per {@link LineHandlerProperties.Aeron#getDriverTimeoutMs()}'s own comment — the
     * same fix as {@code ClusterNodeLauncher}'s, for the same host-resource-constraint reason.
     * {@code publicationUnblockTimeoutNs} must stay strictly above {@code clientLivenessTimeoutNs}
     * (Aeron's {@code Configuration.validateUnblockTimeout} refuses to start otherwise) — doubled
     * here to preserve Aeron's own 15s/10s default ratio, exactly as cluster-node does.
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "line-handler", name = "ingress-transport", havingValue = "aeron", matchIfMissing = true)
    public MediaDriver embeddedMediaDriver(LineHandlerProperties properties) {
        long driverTimeoutMs = properties.getAeron().getDriverTimeoutMs();
        MediaDriver.Context context = new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .driverTimeoutMs(driverTimeoutMs)
                .clientLivenessTimeoutNs(TimeUnit.MILLISECONDS.toNanos(driverTimeoutMs))
                .publicationUnblockTimeoutNs(TimeUnit.MILLISECONDS.toNanos(driverTimeoutMs * 2))
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        return MediaDriver.launchEmbedded(context);
    }

    /**
     * A single {@code Aeron} client shared across this session's entire lifetime, including every
     * {@link ClusterIngressClient} reconnect. Without this, {@code AeronCluster.Context.conclude()}
     * spins up a brand-new underlying {@code Aeron} client (its own conductor, its own client id
     * against the embedded driver) on <em>every</em> connect attempt whenever {@code .aeron(...)}
     * is left unset — the root cause of a real bug found running this service against a live
     * cluster: the very first connect (at bean-creation time, against a freshly-launched driver)
     * would succeed, but every subsequent {@code ClusterIngressClient.reconnect()} timed out with
     * {@code egress.isConnected=false}, because each one raced a brand-new client against the
     * embedded driver's reclamation of the previous one's resources. Mirrors the exact pattern
     * {@code nats-bridge} already uses for its {@code AeronArchive} client
     * (a shared {@code aeron} bean, {@code .ownsAeronClient(false)}).
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "line-handler", name = "ingress-transport", havingValue = "aeron", matchIfMissing = true)
    public Aeron aeron(MediaDriver embeddedMediaDriver, LineHandlerProperties properties) {
        // Aeron.Context extends CommonContext, so it has its own driverTimeoutMs independent of
        // the driver's - kept in lock-step with the embedded driver's above (see that bean's
        // Javadoc). Without this, this client's own liveness detection stayed at Aeron's 10s
        // default even after the driver's was raised, and self-closed exactly as before
        // ("Aeron client is closed") - the actual bug observed live in this session.
        long driverTimeoutMs = properties.getAeron().getDriverTimeoutMs();
        return Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(embeddedMediaDriver.aeronDirectoryName())
                .driverTimeoutMs(driverTimeoutMs));
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "line-handler", name = "ingress-transport", havingValue = "aeron", matchIfMissing = true)
    public IngressTransport clusterIngressTransport(Aeron aeron, LineHandlerProperties properties) {
        LineHandlerProperties.Aeron aeronConfig = properties.getAeron();
        // messageTimeoutNs governs AeronCluster's own session-connect handshake (AsyncConnect's
        // AWAIT_PUBLICATION_CONNECTED etc.) - a third, independent Aeron timeout from the
        // driverTimeoutMs/clientLivenessTimeoutNs pair fixed on the beans above. Aeron's 5s
        // default is a permanent-crash bug here, not a transient one: this connect happens
        // synchronously inside a @Bean factory method at Spring context startup, so a timeout
        // throws, kills the whole context, and the pod CrashLoopBackOffs with no retry - observed
        // live in this session when cluster-node was busy draining a large backlog and couldn't
        // ack a new session-connect within 5s. Reuses the same driverTimeoutMs-derived value as
        // the beans above for one "how patient should Aeron be" knob.
        AeronCluster.Context context = new AeronCluster.Context()
                .aeron(aeron)
                .ownsAeronClient(false)
                .messageTimeoutNs(TimeUnit.MILLISECONDS.toNanos(aeronConfig.getDriverTimeoutMs()))
                .ingressChannel(aeronConfig.getIngressChannel())
                .ingressEndpoints(aeronConfig.getIngressEndpoints())
                .egressChannel(aeronConfig.getEgressChannel())
                .credentialsSupplier(new SourcePrincipalCredentialsSupplier(aeronConfig.getCredential()));
        return new ClusterIngressClient(context, IngressClientConfig.defaults());
    }

    // ---- NATS transport (line-handler.ingress-transport=nats) ----

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "line-handler", name = "ingress-transport", havingValue = "nats")
    public Connection ingressNatsConnection(LineHandlerProperties properties) throws IOException, InterruptedException {
        return Nats.connect(properties.getNatsIngress().getUrl());
    }

    @Bean
    @ConditionalOnProperty(prefix = "line-handler", name = "ingress-transport", havingValue = "nats")
    public JetStream ingressJetStream(Connection ingressNatsConnection) throws IOException {
        return ingressNatsConnection.jetStream();
    }

    @Bean
    @ConditionalOnProperty(prefix = "line-handler", name = "ingress-transport", havingValue = "nats")
    public IngressTransport natsIngressTransport(JetStream ingressJetStream, LineHandlerProperties properties,
                                                  LineHandlerMetrics lineHandlerMetrics) {
        LineHandlerProperties.NatsIngress natsIngressConfig = properties.getNatsIngress();
        return new NatsIngressTransport(ingressJetStream,
                new NatsIngressConfig(natsIngressConfig.getSubject(), natsIngressConfig.getMaxInFlight()),
                lineHandlerMetrics);
    }
}
