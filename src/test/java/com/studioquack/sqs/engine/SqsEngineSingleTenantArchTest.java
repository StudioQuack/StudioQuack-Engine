package com.studioquack.sqs.engine;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import java.util.Set;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noFields;

/**
 * Enforces the daemon's single-tenant engine boundary. Here there is NO tenant: the daemon serves
 * one environment, so the raw Activiti engine services may be injected ONLY by classes inside
 * {@code com.studioquack.sqs.engine} ({@link SqsEngine} is the single facade).
 */
@AnalyzeClasses(packages = "com.studioquack.sqs", importOptions = ImportOption.DoNotIncludeTests.class)
class SqsEngineSingleTenantArchTest {

    private static final Set<String> RAW_ENGINE_SERVICES = Set.of(
            "org.activiti.engine.RuntimeService",
            "org.activiti.engine.RepositoryService",
            "org.activiti.engine.HistoryService",
            "org.activiti.engine.ManagementService",
            "org.activiti.engine.TaskService");

    private static final DescribedPredicate<JavaClass> A_RAW_ENGINE_SERVICE =
            new DescribedPredicate<>("a raw Activiti engine service (inject SqsEngine instead)") {
                @Override
                public boolean test(JavaClass javaClass) {
                    return RAW_ENGINE_SERVICES.contains(javaClass.getName());
                }
            };

    @ArchTest
    static final ArchRule raw_engine_services_confined_to_sqs_engine =
            noFields()
                    .that().areDeclaredInClassesThat()
                    .resideOutsideOfPackage("com.studioquack.sqs.engine..")
                    .should().haveRawType(A_RAW_ENGINE_SERVICE)
                    .because("the daemon is single-tenant: every Activiti engine call must go through "
                            + "SqsEngine (com.studioquack.sqs.engine), never a raw service elsewhere");
}
