package gcm.md.sequencer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the GCM-MD sequencer service. See {@code SEQUENCER-PROJECT.md} for the full
 * design spec: assigns a globally monotonic sequenceId to every ingested SBE market-data
 * message and republishes the stamped stream as the canonical, totally-ordered record.
 */
@SpringBootApplication
public class SequencerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SequencerApplication.class, args);
    }
}
