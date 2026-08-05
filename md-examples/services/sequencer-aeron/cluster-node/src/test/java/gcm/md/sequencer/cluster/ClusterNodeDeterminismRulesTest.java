package gcm.md.sequencer.cluster;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Design §12.3: {@code SequencerClusteredService} (and this package generally) may not
 * reference {@code System.nanoTime}, {@code System.currentTimeMillis}, {@code java.util.Random},
 * or {@code java.time.Clock.system*} — every timestamp used for stamping must come from the
 * cluster (design §5.1: {@code OffsetEpochNanoClock} is explicitly forbidden inside the service,
 * only permitted at the consensus-module level in {@code ClusterNodeLauncher}), or replicas
 * would diverge. The identical rule for {@code libs/sequencer-core} lives in
 * {@code libs/architecture-tests} (this package can't be scanned from there — a services/*
 * class that module may not depend on).
 *
 * <p>Currently {@code @Disabled}: ArchUnit cannot parse this reactor's Java 25 class files — see
 * {@code libs/architecture-tests}' {@code ModuleBoundaryTest} for the full confidence note.
 */
class ClusterNodeDeterminismRulesTest {

    private static final JavaClasses CLUSTER_NODE_CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("gcm.md.sequencer.cluster");

    @Test
    @Disabled("ArchUnit cannot parse Java 25 class files as of archunit 1.4.2 (latest)")
    void clusterNodeMustNotReferenceRandomSources() {
        ArchRule rule = noClasses().should().dependOnClassesThat().belongToAnyOf(java.util.Random.class);
        rule.check(CLUSTER_NODE_CLASSES);
    }

    @Test
    @Disabled("ArchUnit cannot parse Java 25 class files as of archunit 1.4.2 (latest)")
    void clusterNodeMustNotCallSystemNanoTimeOrCurrentTimeMillis() {
        ArchRule rule = noClasses().should().callMethod(System.class, "nanoTime")
                .orShould().callMethod(System.class, "currentTimeMillis");
        rule.check(CLUSTER_NODE_CLASSES);
    }

    @Test
    @Disabled("ArchUnit cannot parse Java 25 class files as of archunit 1.4.2 (latest)")
    void onlyClusterNodeLauncherMayReferenceOffsetEpochNanoClock() {
        // Design §5.1: OffsetEpochNanoClock is only permitted "at the consensus-module level"
        // (ClusterNodeLauncher wiring it into ConsensusModule.Context) — forbidden inside
        // SequencerClusteredService and everything it owns.
        ArchRule rule = noClasses().that().haveSimpleNameNotEndingWith("ClusterNodeLauncher")
                .should().dependOnClassesThat().haveFullyQualifiedName("org.agrona.concurrent.OffsetEpochNanoClock");
        rule.check(CLUSTER_NODE_CLASSES);
    }
}
