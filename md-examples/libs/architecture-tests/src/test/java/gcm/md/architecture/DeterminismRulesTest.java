package gcm.md.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Design §12.3: {@code libs/sequencer-core} may not reference {@code System.nanoTime},
 * {@code System.currentTimeMillis}, {@code java.util.Random}, or {@code java.time.Clock.system*}
 * — every timestamp must come from the caller (design §4: "a pure function of (current state,
 * input buffer, supplied time)"), never a local clock, or replicas would diverge. The identical
 * rule for {@code SequencerClusteredService} lives in {@code cluster-node}'s own test suite (a
 * services/* class this module may not depend on — see {@link ModuleBoundaryTest}).
 *
 * <p>Currently {@code @Disabled} for the same reason as {@link ModuleBoundaryTest}: ArchUnit
 * cannot parse this reactor's Java 25 class files. {@link DeterminismTest} in
 * {@code libs/sequencer-core} covers the behavioral half of §12.3 (replay/snapshot equivalence)
 * and runs today; this class covers the static-analysis half and is blocked purely on the
 * ArchUnit/JDK25 compatibility gap.
 */
class DeterminismRulesTest {

    private static final JavaClasses SEQUENCER_CORE_CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("gcm.md.sequencer.stamping");

    @Test
    @Disabled("ArchUnit cannot parse Java 25 class files as of archunit 1.4.2 (latest); see class Javadoc")
    void sequencerCoreMustNotReferenceWallClockOrRandomSources() {
        ArchRule rule = noClasses().should().dependOnClassesThat()
                .belongToAnyOf(java.util.Random.class)
                .orShould().dependOnClassesThat()
                .haveFullyQualifiedName("java.time.Clock");
        rule.check(SEQUENCER_CORE_CLASSES);
    }

    @Test
    @Disabled("ArchUnit cannot parse Java 25 class files as of archunit 1.4.2 (latest); see class Javadoc")
    void sequencerCoreMustNotCallSystemNanoTimeOrCurrentTimeMillis() {
        ArchRule rule = noClasses().should().callMethod(System.class, "nanoTime")
                .orShould().callMethod(System.class, "currentTimeMillis");
        rule.check(SEQUENCER_CORE_CLASSES);
    }
}
