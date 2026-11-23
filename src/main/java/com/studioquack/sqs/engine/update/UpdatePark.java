package com.studioquack.sqs.engine.update;

import org.activiti.engine.ActivitiEngineAgenda;
import org.activiti.engine.impl.interceptor.CommandContext;
import org.activiti.engine.impl.persistence.entity.ExecutionEntity;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;

/**
 * Stops one token after the activity that just finished, before the outgoing
 * sequence flow is taken. One hook for every node; the BPMN is not edited.
 */
public final class UpdatePark {

    /** Set on the parked execution so the next boot continues that token. */
    public static final String VARIABLE = "__sq_update_park";

    private UpdatePark() {
    }

    /**
     * @return true when the outgoing flow was unscheduled and the variable was set
     */
    public static boolean park(CommandContext commandContext, String executionId) {
        if (commandContext == null || executionId == null) {
            return false;
        }
        ExecutionEntity execution = commandContext.getExecutionEntityManager().findById(executionId);
        if (execution == null || execution.isEnded()) {
            return false;
        }
        ActivitiEngineAgenda agenda = commandContext.getAgenda();
        if (!dropTakeOutgoing(agenda, executionId)) {
            return false;
        }
        execution.setVariableLocal(VARIABLE, Boolean.TRUE);
        return true;
    }

    /**
     * Removes planned take-outgoing operations for this execution. The current
     * operation (the listener) is already running and is not in the queue, so
     * the completion commits and the next node does not start.
     */
    static boolean dropTakeOutgoing(ActivitiEngineAgenda agenda, String executionId) {
        if (agenda == null) {
            return false;
        }
        Collection<?> operations = operations(agenda);
        if (operations == null) {
            return false;
        }
        boolean removed = false;
        Iterator<?> it = operations.iterator();
        while (it.hasNext()) {
            Object op = it.next();
            if (op == null) {
                continue;
            }
            String name = op.getClass().getSimpleName();
            if (!name.contains("TakeOutgoing") && !name.contains("ContinueProcess")) {
                continue;
            }
            if (executionId.equals(executionIdOf(op))) {
                it.remove();
                removed = true;
            }
        }
        return removed;
    }

    private static Collection<?> operations(ActivitiEngineAgenda agenda) {
        Class<?> type = agenda.getClass();
        while (type != null) {
            try {
                Field field = type.getDeclaredField("operations");
                field.setAccessible(true);
                Object value = field.get(agenda);
                if (value instanceof Collection<?> collection) {
                    return collection;
                }
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            } catch (ReflectiveOperationException | RuntimeException ignored) {
                return null;
            }
        }
        return new ArrayDeque<>();
    }

    private static String executionIdOf(Object operation) {
        Class<?> type = operation.getClass();
        while (type != null) {
            try {
                Field field = type.getDeclaredField("execution");
                field.setAccessible(true);
                Object value = field.get(operation);
                if (value instanceof ExecutionEntity entity) {
                    return entity.getId();
                }
                return null;
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            } catch (ReflectiveOperationException | RuntimeException ignored) {
                return null;
            }
        }
        return null;
    }
}
