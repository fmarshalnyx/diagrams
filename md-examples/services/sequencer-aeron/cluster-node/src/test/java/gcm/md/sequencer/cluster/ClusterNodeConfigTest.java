package gcm.md.sequencer.cluster;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pure record/string-building logic — no Aeron, no filesystem, no network — so unlike the rest of
 * this module's cluster-touching classes, these factories are fully unit-testable without a live
 * driver. Covers the port-arithmetic contracts {@link InProcessCluster} (in {@code
 * integration-tests}) and {@code TestIngressClients} rely on, so a regression here would silently
 * break every {@code *IT} class's ability to even connect.
 */
class ClusterNodeConfigTest {

    @Test
    void localSingleMemberUsesLoopbackAndTheDocumentedPortScheme() {
        ClusterNodeConfig config = ClusterNodeConfig.localSingleMember();

        assertThat(config.clusterMemberId()).isZero();
        assertThat(config.clusterMembers())
                .isEqualTo("0,localhost:9010,localhost:9020,localhost:9030,localhost:9040,localhost:9050|");
        assertThat(config.archiveControlChannel()).isEqualTo("aeron:udp?endpoint=localhost:9050");
        assertThat(config.archiveControlResponseChannel()).isEqualTo("aeron:udp?endpoint=localhost:9051");
        assertThat(config.replicationChannel()).isEqualTo("aeron:udp?endpoint=localhost:9060");
    }

    @Test
    void ingressAndLogChannelsAreEndpointLessTemplates() {
        // Design §5.2 / CLAUDE.md's hard-won lesson: an explicit endpoint here silently leaves the
        // ingress port unbound, since the real bind endpoint must come from clusterMembers instead.
        ClusterNodeConfig config = ClusterNodeConfig.localSingleMember();

        assertThat(config.ingressChannel()).doesNotContain("endpoint=");
        assertThat(config.logChannel()).doesNotContain("endpoint=");
    }

    @Test
    void singleMemberIsAddressableAtAnArbitraryHost() {
        String host = "gcm-md-cluster-node-0.gcm-md-cluster-node.market-data.svc.cluster.local";
        ClusterNodeConfig config = ClusterNodeConfig.singleMember(host, "/data", List.of());

        assertThat(config.clusterMembers())
                .isEqualTo("0," + host + ":9010," + host + ":9020," + host + ":9030," + host + ":9040," + host + ":9050|");
        assertThat(config.aeronDirectoryName()).isEqualTo("/data/aeron");
        assertThat(config.clusterDirectoryName()).isEqualTo("/data/cluster");
        assertThat(config.archiveDirectoryName()).isEqualTo("/data/archive");
    }

    @Test
    void kubernetesMemberUsesTheSuppliedMemberIdAndMembershipString() {
        String membersString = "0,host-0:9010,host-0:9020,host-0:9030,host-0:9040,host-0:9050|"
                + "1,host-1:9010,host-1:9020,host-1:9030,host-1:9040,host-1:9050|"
                + "2,host-2:9010,host-2:9020,host-2:9030,host-2:9040,host-2:9050|";
        ClusterNodeConfig config = ClusterNodeConfig.kubernetesMember(1, membersString, "host-1", "/data", List.of());

        assertThat(config.clusterMemberId()).isEqualTo(1);
        assertThat(config.clusterMembers()).isEqualTo(membersString);
        assertThat(config.aeronDirectoryName()).isEqualTo("/data/aeron");
        assertThat(config.clusterDirectoryName()).isEqualTo("/data/cluster");
        assertThat(config.archiveDirectoryName()).isEqualTo("/data/archive");
        assertThat(config.archiveControlChannel()).isEqualTo("aeron:udp?endpoint=host-1:9050");
        assertThat(config.archiveControlResponseChannel()).isEqualTo("aeron:udp?endpoint=host-1:9051");
        assertThat(config.replicationChannel()).isEqualTo("aeron:udp?endpoint=host-1:9060");
    }

    @Test
    void kubernetesMemberIngressAndLogChannelsAreEndpointLessTemplates() {
        ClusterNodeConfig config = ClusterNodeConfig.kubernetesMember(0, "0,host-0:9010|", "host-0", "/data", List.of());

        assertThat(config.ingressChannel()).doesNotContain("endpoint=");
        assertThat(config.logChannel()).doesNotContain("endpoint=");
    }

    @Test
    void localMultiMemberProducesOneConfigPerMemberWithDistinctIds() {
        List<ClusterNodeConfig> configs = ClusterNodeConfig.localMultiMember(3, "/data", List.of());

        assertThat(configs).hasSize(3);
        assertThat(configs.get(0).clusterMemberId()).isZero();
        assertThat(configs.get(1).clusterMemberId()).isEqualTo(1);
        assertThat(configs.get(2).clusterMemberId()).isEqualTo(2);
    }

    @Test
    void everyLocalMultiMemberConfigSharesTheIdenticalMembershipView() {
        // A correctness property worth pinning down explicitly: every member must agree on the
        // same clusterMembers string, or Raft membership itself is inconsistent from the start.
        List<ClusterNodeConfig> configs = ClusterNodeConfig.localMultiMember(3, "/data", List.of());

        String expected = configs.get(0).clusterMembers();
        assertThat(configs).extracting(ClusterNodeConfig::clusterMembers).containsOnly(expected);
        assertThat(expected)
                .isEqualTo("0,localhost:9010,localhost:9020,localhost:9030,localhost:9040,localhost:9050|"
                        + "1,localhost:9110,localhost:9120,localhost:9130,localhost:9140,localhost:9150|"
                        + "2,localhost:9210,localhost:9220,localhost:9230,localhost:9240,localhost:9250|");
    }

    @Test
    void localMultiMemberPortSchemeMatchesTheDocumentedOffsetsForANonZeroMember() {
        // member 1's base is 9010 + 1*100 = 9110 (see ClusterNodeConfig's own Javadoc).
        ClusterNodeConfig member1 = ClusterNodeConfig.localMultiMember(3, "/data", List.of()).get(1);

        assertThat(member1.archiveControlChannel()).isEqualTo("aeron:udp?endpoint=localhost:9150"); // base+40
        assertThat(member1.archiveControlResponseChannel()).isEqualTo("aeron:udp?endpoint=localhost:9151"); // base+41
        assertThat(member1.replicationChannel()).isEqualTo("aeron:udp?endpoint=localhost:9170"); // base+60
        assertThat(member1.aeronDirectoryName()).isEqualTo("/data/member1/aeron");
        assertThat(member1.clusterDirectoryName()).isEqualTo("/data/member1/cluster");
        assertThat(member1.archiveDirectoryName()).isEqualTo("/data/member1/archive");
    }

    @Test
    void localMultiMemberDataDirectoriesAreDistinctPerMember() {
        List<ClusterNodeConfig> configs = ClusterNodeConfig.localMultiMember(3, "/data", List.of());

        assertThat(configs).extracting(ClusterNodeConfig::clusterDirectoryName).doesNotHaveDuplicates();
        assertThat(configs).extracting(ClusterNodeConfig::aeronDirectoryName).doesNotHaveDuplicates();
        assertThat(configs).extracting(ClusterNodeConfig::archiveDirectoryName).doesNotHaveDuplicates();
    }
}
