package gcm.md.linehandlertemplate;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the line-handler-template reference service (implementation-steps.md
 * Milestone 5B): consumes a mock upstream feed and relays it into the sequencer via whichever
 * {@code IngressTransport} {@code line-handler.ingress-transport} selects. See this module's
 * README for what a real line handler should copy versus keep as-is.
 */
@SpringBootApplication
public class LineHandlerTemplateApplication {

    public static void main(String[] args) {
        SpringApplication.run(LineHandlerTemplateApplication.class, args);
    }
}
