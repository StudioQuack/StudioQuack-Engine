package com.studioquack.sqs.engine.update;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.ManagementService;
import org.activiti.engine.RuntimeService;
import org.activiti.engine.impl.interceptor.CommandContext;
import org.activiti.engine.impl.persistence.entity.ExecutionEntity;
import org.activiti.engine.runtime.Execution;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Continues tokens that were parked between nodes by the previous process.
 * Runs after the engine is up. Intake is open again, so the next node starts.
 */
@Slf4j
@Component
@Order(0)
@RequiredArgsConstructor
public class UpdateResumeOnBoot implements ApplicationRunner {

    private final RuntimeService runtimeService;
    private final ManagementService managementService;

    @Override
    public void run(ApplicationArguments args) {
        List<Execution> parked;
        try {
            parked = runtimeService.createExecutionQuery()
                    .variableValueEquals(UpdatePark.VARIABLE, Boolean.TRUE)
                    .list();
        } catch (RuntimeException ex) {
            log.warn("update-resume: parked-token query failed: {}", ex.toString());
            return;
        }
        for (Execution execution : parked) {
            if (execution == null || execution.getId() == null) {
                continue;
            }
            try {
                resume(execution.getId());
                log.info("update-resume: continued token {}", execution.getId());
            } catch (RuntimeException ex) {
                log.warn("update-resume: token {} stayed parked: {}", execution.getId(), ex.toString());
            }
        }
    }

    private void resume(String executionId) {
        managementService.executeCommand(commandContext -> {
            continueParked(commandContext, executionId);
            return null;
        });
    }

    static void continueParked(CommandContext commandContext, String executionId) {
        ExecutionEntity execution = commandContext.getExecutionEntityManager().findById(executionId);
        if (execution == null || execution.isEnded()) {
            return;
        }
        execution.removeVariableLocal(UpdatePark.VARIABLE);
        commandContext.getAgenda().planTakeOutgoingSequenceFlowsOperation(execution, true);
    }
}
