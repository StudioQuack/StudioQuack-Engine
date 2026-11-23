package com.studioquack.sqs.usertask;

/**
 * Identifiers of a {@code user.task} the token just parked on, captured inside the engine command
 * by {@code UserTaskApprovalListener} (no payload, no PHI — the reporter reads the rest after
 * commit).
 *
 * @param taskId The Activiti task id — unique per visit, so a loop back onto the same node is a
 * NEW approval, never a re-answer of the old one
 * @param processInstanceId
 * @param processDefinitionId
 * @param taskDefinitionKey
 * @param name
 * @param businessKey The run's businessKey; {@code null} for a keyless call-activity child
 */
public record ParkedUserTask(String taskId, String processInstanceId, String processDefinitionId,
                             String taskDefinitionKey, String name, String businessKey) {
}
