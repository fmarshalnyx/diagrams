package gcm.md.linehandlertemplate.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Confirms {@code application.yml} binds onto {@link LineHandlerProperties} — kebab-case keys and
 * nested objects — without standing up the full service (NATS/Aeron beans require real
 * infrastructure).
 */
class LineHandlerPropertiesBindingTest {

    @EnableConfigurationProperties(LineHandlerProperties.class)
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
        LineHandlerProperties properties = context.getBean(LineHandlerProperties.class);

        assertThat(properties.getIngressTransport()).isEqualTo("aeron");
        assertThat(properties.getUpstream().getUrl()).isEqualTo("nats://localhost:4222");
        assertThat(properties.getUpstream().getStream()).isEqualTo("MOCK_UPSTREAM");
        assertThat(properties.getUpstream().getSubject()).isEqualTo("upstream.mock.marketdata");
        assertThat(properties.getUpstream().getDurableConsumerName()).isEqualTo("line-handler-template");
        assertThat(properties.getUpstream().getMaxFetchBatch()).isEqualTo(100);
        assertThat(properties.getAeron().getIngressChannel()).isEqualTo("aeron:udp?term-length=64k");
        assertThat(properties.getAeron().getIngressEndpoints()).isEqualTo("0=localhost:9010");
        assertThat(properties.getNatsIngress().getSubject()).isEqualTo("MD_RAW");
        assertThat(properties.getNatsIngress().getMaxInFlight()).isEqualTo(10_000);
        assertThat(properties.getStamping().getSourceSeqNumOffset()).isEqualTo(64);
        assertThat(properties.getOffer().getRetryParkNanos()).isEqualTo(1_000_000);
        assertThat(properties.getOffer().getWarnEveryNAttempts()).isEqualTo(100);
    }
}
