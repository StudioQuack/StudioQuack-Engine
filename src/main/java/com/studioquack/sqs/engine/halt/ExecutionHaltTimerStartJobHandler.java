package com.studioquack.sqs.engine.halt;

import com.studioquack.sqs.engine.timer.TimerStartRunLauncher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.impl.interceptor.CommandContext;
import org.activiti.engine.impl.jobexecutor.JobHandler;
import org.activiti.engine.impl.jobexecutor.TimerStartEventJobHandler;
import org.activiti.engine.impl.persistence.entity.ExecutionEntity;
import org.activiti.engine.impl.persistence.entity.JobEntity;
import org.springframework.stereotype.Component;

/**
 * Replaces Activiti's {@code timer-start-event} handler. While the engine is halted (offline or limits
 * exceeded) the job returns without starting a process; a repeating timer still reschedules because
 * that lives on the job, not in the handler. Job acquisition is normally paused while halted, so this
 * only catches a fire that was already acquired when the mode changed. An admitted fire is started by
 * {@link TimerStartRunLauncher} as a run with its own run id.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ExecutionHaltTimerStartJobHandler implements JobHandler {

    private final ExecutionHaltGuard haltGuard;
    private final TimerStartRunLauncher launcher;

    @Override
    public String getType() {
        return TimerStartEventJobHandler.TYPE;
    }

    @Override
    public void execute(JobEntity job, String configuration, ExecutionEntity execution, CommandContext commandContext) {
        ExecutionHaltGuard.Mode mode = haltGuard.mode();
        if (mode != ExecutionHaltGuard.Mode.ACTIVE) {
            log.info("execution-halt: timer start skipped, engine is {} (this occurrence does not start)", mode);
            return;
        }
        launcher.start(job, configuration, commandContext);
    }
}
