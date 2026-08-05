package gcm.md.mockupstreamsource.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Confirms {@code application.yml} binds onto {@link MockUpstreamSourceProperties} without
 * standing up the full service (NATS beans require real infrastructure).
 */
class MockUpstreamSourcePropertiesBindingTest {

    @EnableConfigurationProperties(MockUpstreamSourceProperties.class)
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
        MockUpstreamSourceProperties properties = context.getBean(MockUpstreamSourceProperties.class);

        assertThat(properties.getUrl()).isEqualTo("nats://localhost:4222");
        assertThat(properties.getStream()).isEqualTo("MOCK_UPSTREAM");
        assertThat(properties.getSubject()).isEqualTo("upstream.mock.marketdata");
        assertThat(properties.getEgressSubject()).isEqualTo("md.sequenced");
        assertThat(properties.getRate()).isEqualTo(100_000L);
        assertThat(properties.getPattern()).isEqualTo("steady");
        assertThat(properties.getGapProbability()).isZero();
        assertThat(properties.getDuplicateProbability()).isZero();
        assertThat(properties.getSeed()).isZero();
        assertThat(properties.isBatched()).isFalse();
    }
}
