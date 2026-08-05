package gcm.md.sequencer.core;

import gcm.md.sequencer.config.SequencerProperties;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfig;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElector;
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Kubernetes Lease-based single-writer enforcement (project spec §10). Exactly one replica
 * holds the lease at a time; the standby blocks in {@link #startAndAwaitLeadership} until it
 * either acquires the lease itself or the process is stopped. {@link #isLeader()} is also the
 * fencing check consulted by {@link SequenceAllocator} before every KV block-lease write.
 */
public final class LeaderElection {

    private final KubernetesClient client;
    private final SequencerProperties.Leadership config;

    private volatile boolean leader = false;
    private volatile LeaderElector elector;

    /** Creates the leader-election coordinator against the given Kubernetes client and config. */
    public LeaderElection(KubernetesClient client, SequencerProperties properties) {
        this.client = client;
        this.config = properties.getLeadership();
    }

    /**
     * Starts the leader-election loop against the configured Lease object and blocks until this
     * instance acquires leadership at least once. {@code onStartLeading} and {@code onStopLeading}
     * are invoked on subsequent transitions for the lifetime of the process (startup ordering
     * step 2 and lease-loss handling, §10).
     */
    public void startAndAwaitLeadership(Runnable onStartLeading, Runnable onStopLeading) {
        String identity = System.getenv().getOrDefault("HOSTNAME", UUID.randomUUID().toString());
        LeaseLock lock = new LeaseLock(config.getLeaseNamespace(), config.getLeaseName(), identity);
        CountDownLatch acquiredOnce = new CountDownLatch(1);
        LeaderCallbacks callbacks = new LeaderCallbacks(
                () -> {
                    leader = true;
                    onStartLeading.run();
                    acquiredOnce.countDown();
                },
                () -> {
                    leader = false;
                    onStopLeading.run();
                },
                newLeaderIdentity -> { /* informational only; isLeader() drives all decisions */ });

        LeaderElectionConfig electionConfig = new LeaderElectionConfig(
                lock,
                Duration.ofSeconds(config.getLeaseDurationSeconds()),
                Duration.ofSeconds(config.getRenewDeadlineSeconds()),
                Duration.ofSeconds(config.getRetryPeriodSeconds()),
                callbacks,
                true,
                config.getLeaseName());

        this.elector = client.leaderElector().withConfig(electionConfig).build();
        elector.start();

        try {
            acquiredOnce.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted awaiting initial leadership acquisition", e);
        }
    }

    /** Returns whether this instance currently holds the leadership lease. */
    public boolean isLeader() {
        return leader;
    }

    /** Releases leadership (if held) and stops the election loop. Idempotent. */
    public void stop() {
        LeaderElector current = this.elector;
        if (current != null) {
            current.release();
        }
    }
}
