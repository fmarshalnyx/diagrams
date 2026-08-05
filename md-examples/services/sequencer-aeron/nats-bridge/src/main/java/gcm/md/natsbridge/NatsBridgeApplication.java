package gcm.md.natsbridge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the nats-bridge service (design §9): subscribes to the phase-2 Aeron cluster's
 * sequenced egress and republishes it to {@code MD_SEQUENCED}, so existing JetStream/WebSocket
 * consumers see no change whatsoever.
 */
@SpringBootApplication
public class NatsBridgeApplication {

    public static void main(String[] args) {
        SpringApplication.run(NatsBridgeApplication.class, args);
    }
}
