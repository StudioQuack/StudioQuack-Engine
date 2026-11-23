package com.studioquack.sqs.engine.delegate;

import com.studioquack.sqs.usertask.ParkedUserTask;
import com.studioquack.sqs.usertask.UserTaskApprovalReporter;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.DelegateTask;
import org.activiti.engine.delegate.TaskListener;
import org.activiti.engine.impl.cfg.TransactionContext;
import org.activiti.engine.impl.cfg.TransactionState;
import org.activiti.engine.impl.context.Context;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

/**
 * {@code create} task listener on every {@code user.task}: the token just parked on an approval.
 *
 * <p>Runs inside the engine command, so it only captures the task's identifiers and defers the
 * report to {@link UserTaskApprovalReporter} until the engine transaction COMMITS. A report sent
 * from inside the transaction could announce a task whose transaction later rolls back, and a
 * network call there would hold the engine's connection for the round-trip.
 *
 * <p>Never throws: the wait state is the engine's to keep even when the control plane cannot be told
 * about it yet — the reporter's sweep re-reports any parked task that was never acknowledged.
 */
@Slf4j
@Component("userTaskApprovalListener")
public class UserTaskApprovalListener implements TaskListener {

    private final ObjectProvider<UserTaskApprovalReporter> reporterProvider;

    public UserTaskApprovalListener(ObjectProvider<UserTaskApprovalReporter> reporterProvider) {
        this.reporterProvider = reporterProvider;
    }

    @Override
    public void notify(DelegateTask task) {
        try {
            UserTaskApprovalReporter reporter = reporterProvider.getIfAvailable();
            if (reporter == null) {
                return;
            }
            ParkedUserTask parked = new ParkedUserTask(task.getId(), task.getProcessInstanceId(),
                    task.getProcessDefinitionId(), task.getTaskDefinitionKey(), task.getName(),
                    task.getExecution() == null ? null : task.getExecution().getProcessInstanceBusinessKey());
            TransactionContext tx = Context.getTransactionContext();
            if (tx == null) {
                reporter.onParked(parked);
                return;
            }
            tx.addTransactionListener(TransactionState.COMMITTED, ignored -> reporter.onParked(parked));
        } catch (RuntimeException ex) {
            log.warn("user-task.approval: could not schedule the park report task={}: {}",
                    task.getId(), ex.toString());
        }
    }
}
