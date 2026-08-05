package gcm.md.mockupstreamsource;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the mock-upstream-source service (implementation-steps.md Milestone 5B):
 * continuously generates synthetic upstream traffic and continuously verifies contiguity/
 * no-duplicates on the sequencer's observed final egress. Persistent and independently
 * deployable from {@code line-handler-template} — see this module's README.
 */
@SpringBootApplication
public class MockUpstreamSourceApplication {

    public static void main(String[] args) {
        SpringApplication.run(MockUpstreamSourceApplication.class, args);
    }
}
