package gcm.md.sequencer.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Confirms each of the five deployment-environment profiles (local, dev, uat, prod, prod-dr —
 * see md-sequencer/k8s/README.md) actually overrides its NATS endpoints and Kubernetes leadership
 * namespace, and that they don't collide with one another.
 */
class SequencerEnvironmentProfilesTest {

    @EnableConfigurationProperties(SequencerProperties.class)
    @Configuration
    static class PropertiesOnlyConfig {
    }

    private ConfigurableApplicationContext context;

    @AfterEach
    void tearDown() {
        if (context != null) {
            context.close();
        }
    }

    private SequencerProperties bind(String profile) {
        context = new SpringApplicationBuilder(PropertiesOnlyConfig.class)
                .web(WebApplicationType.NONE)
                .profiles(profile)
                .run();
        return context.getBean(SequencerProperties.class);
    }

    @Test
    void localProfileUsesInClusterNatsAndItsOwnNamespace() {
        SequencerProperties properties = bind("local");
        assertThat(properties.getIngress().getNats().getUrl()).isEqualTo("nats://nats:4222");
        assertThat(properties.getLeadership().getLeaseNamespace()).isEqualTo("md-sequencer-local");
        assertThat(context.getEnvironment().getProperty("info.environment")).isEqualTo("local");
    }

    @Test
    void prodAndProdDrPointAtIndependentNatsClustersAndNamespaces() {
        SequencerProperties prod = bind("prod");
        String prodUrl = prod.getIngress().getNats().getUrl();
        String prodNamespace = prod.getLeadership().getLeaseNamespace();
        context.close();

        SequencerProperties prodDr = bind("prod-dr");
        String prodDrUrl = prodDr.getIngress().getNats().getUrl();
        String prodDrNamespace = prodDr.getLeadership().getLeaseNamespace();

        assertThat(prodUrl).isNotEqualTo(prodDrUrl);
        assertThat(prodNamespace).isNotEqualTo(prodDrNamespace);
    }

    @Test
    void everyEnvironmentProfileTagsItsOwnMetricsAndInfoDistinctly() {
        for (String profile : new String[] {"local", "dev", "uat", "prod", "prod-dr"}) {
            bind(profile);
            Environment env = context.getEnvironment();
            assertThat(env.getProperty("management.metrics.tags.environment")).isEqualTo(profile);
            assertThat(env.getProperty("info.environment")).isEqualTo(profile);
            context.close();
            context = null;
        }
    }
}
