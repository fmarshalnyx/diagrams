package gcm.md.sequencer.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Confirms {@code application.yml} actually binds onto {@link SequencerProperties} (project spec
 * §12) — kebab-case keys, nested objects, and the {@code timestamp-template-ids} list — without
 * standing up the full service (NATS/Kubernetes beans require real infrastructure).
 */
class SequencerPropertiesBindingTest {

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

    @Test
    void bindsEveryDefaultFromApplicationYml() {
        context = new SpringApplicationBuilder(PropertiesOnlyConfig.class)
                .web(WebApplicationType.NONE)
                .run();
        SequencerProperties properties = context.getBean(SequencerProperties.class);

        assertThat(properties.getStamping().getProfile()).isEqualTo("v4");
        assertThat(properties.getStamping().getSequenceIdOffset()).isEqualTo(8);
        assertThat(properties.getStamping().getSequenceTimestampOffset()).isEqualTo(32);
        assertThat(properties.getStamping().getSchemaId()).isEqualTo(100);
        assertThat(properties.getStamping().getTimestampTemplateIds()).containsExactly(1, 9);
        assertThat(properties.getStamping().getEventEnrichment().isEnabled()).isTrue();
        assertThat(properties.getStamping().getEventEnrichment().getBoundaryTemplateId()).isEqualTo(6);

        assertThat(properties.getAllocator().getBlockSize()).isEqualTo(1_000_000L);
        assertThat(properties.getAllocator().getKvBucket()).isEqualTo("sequencer-lease");

        assertThat(properties.getIngress().getNats().getMode()).isEqualTo("jetstream");
        assertThat(properties.getIngress().getNats().getConsumer()).isEqualTo("sequencer");

        assertThat(properties.getEgress().getType()).isEqualTo("jetstream");
        assertThat(properties.getEgress().getBackpressure()).isEqualTo("block");
        assertThat(properties.getEgress().getBatching().isEnabled()).isTrue();
        assertThat(properties.getEgress().getBatching().getMaxMessages()).isEqualTo(100);
        assertThat(properties.getEgress().getJetstream().getMaxInFlight()).isEqualTo(8192);

        assertThat(properties.getHeartbeat().getSourceId()).isEqualTo("SEQR");
        assertThat(properties.getLeadership().getLeaseName()).isEqualTo("gcm-md-sequencer");
    }
}
