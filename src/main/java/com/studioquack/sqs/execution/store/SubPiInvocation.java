package com.studioquack.sqs.execution.store;

/**
 * One recorded function-port {@code studioquack.execute} invocation. The store returns these
 * ordered by persisted id ascending.
 */
public record SubPiInvocation(String childExecutionId, String callerNodeId, String subPiRootNodeId) {
}
