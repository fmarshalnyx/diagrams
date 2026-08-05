package gcm.md.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Design §3.3: {@code libs/*} must not depend on {@code services/*} or on Spring. The
 * maven-enforcer rules in {@code build/gcm-md-parent} ban this at the Maven-module level;
 * these rules ban it at the source-import level, so the constraint survives even if a
 * future refactor collapses module boundaries or a dependency is pulled in transitively
 * on the test classpath only.
 *
 * <p>Stubbed here with dependency-direction rules only, per the Milestone A0 implementation
 * step. The §12.3 wall-clock/randomness ban lives alongside this in {@link DeterminismRulesTest}
 * (same module, for {@code sequencer-core} only — the clustered service's identical ban lives
 * in {@code cluster-node} itself, a services/* module this one may not depend on).
 *
 * <p><b>Currently {@code @Disabled}:</b> ArchUnit's {@link ClassFileImporter} cannot parse
 * this reactor's Java 25 class files — confirmed against archunit-junit5 1.3.0 and the
 * latest published 1.4.2 (Nov 2025), both silently import zero classes for a Java-25-compiled
 * package (its bundled ASM doesn't recognize the class file major version), which makes every
 * {@code noClasses()} rule here vacuously fail ArchUnit's empty-should check. Re-enable once a
 * released archunit version parses Java 25 bytecode; until then, the maven-enforcer rules in
 * build/gcm-md-parent are the sole enforcement of this boundary.
 */
class ModuleBoundaryTest {

    private static final JavaClasses NATS_EGRESS_CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("gcm.md.sequencer.egress");

    private static final JavaClasses MD_MODELS_SBE_CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("com.usb.gcm.md.sbe");

    @Test
    @Disabled("ArchUnit cannot parse Java 25 class files as of archunit 1.4.2 (latest); see class Javadoc")
    void natsEgressMustNotDependOnSpring() {
        ArchRule rule = noClasses().should().dependOnClassesThat()
                .resideInAPackage("org.springframework..");
        rule.check(NATS_EGRESS_CLASSES);
    }

    @Test
    @Disabled("ArchUnit cannot parse Java 25 class files as of archunit 1.4.2 (latest); see class Javadoc")
    void natsEgressMustNotDependOnServices() {
        ArchRule rule = noClasses().should().dependOnClassesThat()
                .resideInAnyPackage("gcm.md.sequencer.core..", "gcm.md.sequencer.ingress..",
                        "gcm.md.sequencer.heartbeat..", "gcm.md.sequencer.health..",
                        "gcm.md.sequencer.metrics..", "gcm.md.sequencer.config..");
        rule.check(NATS_EGRESS_CLASSES);
    }

    @Test
    @Disabled("ArchUnit cannot parse Java 25 class files as of archunit 1.4.2 (latest); see class Javadoc")
    void mdModelsSbeMustNotDependOnSpring() {
        ArchRule rule = noClasses().should().dependOnClassesThat()
                .resideInAPackage("org.springframework..");
        rule.check(MD_MODELS_SBE_CLASSES);
    }
}
