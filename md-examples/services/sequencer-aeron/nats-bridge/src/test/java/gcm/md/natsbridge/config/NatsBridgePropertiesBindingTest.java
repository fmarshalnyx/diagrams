package gcm.md.natsbridge.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Confirms {@code application.yml} binds onto {@link NatsBridgeProperties} — kebab-case keys and
 * nested objects — without standing up the full service (NATS/Aeron beans require real
 * infrastructure).
 */
class NatsBridgePropertiesBindingTest {

    @EnableConfigurationProperties(NatsBridgeProperties.class)
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

    @Test
    void bindsEveryDefaultFromApplicationYml() {
        context = new SpringApplicationBuilder(PropertiesOnlyConfig.class)
                .web(WebApplicationType.NONE)
                .run();
        NatsBridgeProperties properties = context.getBean(NatsBridgeProperties.class);

        assertThat(properties.getNats().getUrl()).isEqualTo("nats://localhost:4222");
        assertThat(properties.getNats().getStream()).isEqualTo("MD_SEQUENCED");
        assertThat(properties.getNats().getCheckpointKvBucket()).isEqualTo("bridge-checkpoint");
        assertThat(properties.getNats().getCheckpointIntervalMessages()).isEqualTo(1000);
        assertThat(properties.getCluster().getSubscriptionChannel()).isEqualTo("aeron:udp?control-mode=manual");
        assertThat(properties.getCluster().getEgressStreamId()).isEqualTo(1);
        assertThat(properties.getStamping().getSequenceIdOffset()).isEqualTo(8);
        assertThat(properties.getBatching().isEnabled()).isTrue();
        assertThat(properties.getBatching().getBoundaryTemplateId()).isEqualTo(6);
    }
}
