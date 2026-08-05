package gcm.md.sequencer.integration;

import gcm.md.sequencer.clusterclient.ClusterIngressClient;
import gcm.md.sequencer.clusterclient.IngressClientConfig;
import gcm.md.sequencer.clusterclient.SourcePrincipalCredentialsSupplier;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

/**
 * Test-support: builds a {@link ClusterIngressClient} against an in-process
 * {@link InProcessCluster}, including the embedded media driver every Aeron client process needs
 * (design §7's client wrapper doesn't launch one itself — every real {@code IngressTransport}
 * consumer, e.g. a line handler configured for the Aeron transport, has the identical need).
 */
final class TestIngressClients {

    private TestIngressClients() {
    }

    /** {@code memberCount} must match the {@link InProcessCluster}'s own member count. */
    static ClusterIngressClient connect(int memberCount, String credential) {
        MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        MediaDriver mediaDriver = MediaDriver.launchEmbedded(mediaDriverCtx);

        AeronCluster.Context clusterContext = new AeronCluster.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .ingressChannel("aeron:udp?term-length=64k")
                .ingressEndpoints(ingressEndpoints(memberCount))
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .credentialsSupplier(new SourcePrincipalCredentialsSupplier(credential));

        return new ClusterIngressClient(clusterContext, IngressClientConfig.defaults());
    }

    /** Matches {@link gcm.md.sequencer.cluster.ClusterNodeConfig#localMultiMember}'s port scheme. */
    private static String ingressEndpoints(int memberCount) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < memberCount; i++) {
            if (i > 0) {
                builder.append(',');
            }
            int base = 9010 + i * 100;
            builder.append(i).append('=').append("localhost:").append(base);
        }
        return builder.toString();
    }
}
