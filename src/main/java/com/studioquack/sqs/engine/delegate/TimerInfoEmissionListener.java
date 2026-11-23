package com.studioquack.sqs.engine.delegate;

import com.studioquack.common.dto.flow.InOutValueDto;
import com.studioquack.common.dto.flow.ProcessContextRef;
import com.studioquack.sqs.engine.SqsEngine;
import com.studioquack.sqs.execution.context.ExecutionContext;
import com.studioquack.sqs.execution.store.ExecutionContextStore;
import lombok.extern.slf4j.Slf4j;
import org.activiti.bpmn.model.BpmnModel;
import org.activiti.bpmn.model.FlowElement;
import org.activiti.bpmn.model.FlowNode;
import org.activiti.bpmn.model.SequenceFlow;
import org.activiti.bpmn.model.TimerEventDefinition;
import org.activiti.engine.delegate.DelegateExecution;
import org.activiti.engine.delegate.ExecutionListener;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Runtime bridge for {@code timer.intermediate.catch.event} and {@code timer.start.event} — emits the
 * template-promised {@code timer_info} OUT envelope ({@code firedAt}, {@code kind}, {@code due}).
 * Attached (event={@code end}) by {@code ActivitiTimerCatchStrategy} and
 * {@code ActivitiTimerStartStrategy}; fires exactly when the token LEAVES the event, i.e. when the
 * timer job has fired (never at park time). A timer-started run has its own run id
 * ({@code TimerStartRunLauncher}), which is the store key downstream nodes read the row under.
 *
 * <p>The envelope is fanned out as ONE sealed output row per downstream node — the same row shape
 * {@code MessageSenderExecutionListener#writeEmitResult} and the C1 message/signal envelope use —
 * so the next node's ordinary input scan reads {@code timer_info.firedAt} like any other
 * producer's port value. {@code kind} and {@code due} are read off the DEPLOYED model's
 * {@link TimerEventDefinition}, so a bounded cycle reports the EFFECTIVE expression
 * ({@code R10/PT1M}), not the authored one.
 *
 * <p><b>Best-effort.</b> The timer already fired when this runs — a fan-out failure must never
 * roll the timer job back (the async executor would retry it and double-fire the token), so a
 * failure is logged and the run continues without the info row, exactly like
 * {@code writeEmitResult}.
 */
@Slf4j
@Component("timerInfoEmissionListener")
public class TimerInfoEmissionListener implements ExecutionListener {

    /** Activity id prefix the strategies mint: designer id {@code X} → activity id {@code node_X}. */
    private static final String NODE_ID_PREFIX = "node_";
    /** OUT port value the timer catch/start templates declare. */
    private static final String TIMER_INFO_PORT = "timer_info";

    private final ObjectProvider<SqsEngine> sqsEngineProvider;
    private final ObjectProvider<ExecutionContextStore> executionContextStoreProvider;

    public TimerInfoEmissionListener(ObjectProvider<SqsEngine> sqsEngineProvider,
                                     ObjectProvider<ExecutionContextStore> executionContextStoreProvider) {
        this.sqsEngineProvider = sqsEngineProvider;
        this.executionContextStoreProvider = executionContextStoreProvider;
    }

    @Override
    public void notify(DelegateExecution execution) {
        SqsEngine engine = sqsEngineProvider.getIfAvailable();
        ExecutionContextStore contextStore = executionContextStoreProvider.getIfAvailable();
        if (engine == null || contextStore == null) {
            return;
        }
        String activityId = execution.getCurrentActivityId();
        String nodeId = StringUtils.removeStart(activityId, NODE_ID_PREFIX);
        try {
            String businessKey = execution.getProcessInstanceBusinessKey();
            if (StringUtils.isBlank(businessKey)) {
                // A run without a run id has no store key a downstream scan could read the row
                // under — nothing to write.
                return;
            }
            BpmnModel model = engine.readDeployedModel(execution.getProcessDefinitionId());
            if (model == null) {
                log.warn("timerInfoEmission: deployed model unreadable for {} — timer_info lost for {}",
                        execution.getProcessDefinitionId(), nodeId);
                return;
            }
            FlowElement fe = model.getFlowElement(activityId);
            if (!(fe instanceof FlowNode flowNode) || flowNode.getOutgoingFlows() == null) {
                return;
            }
            List<String> downstreamIds = flowNode.getOutgoingFlows().stream()
                    .map(SequenceFlow::getTargetRef)
                    .filter(StringUtils::isNotBlank)
                    .map(target -> StringUtils.removeStart(target, NODE_ID_PREFIX))
                    .distinct()
                    .collect(Collectors.toList());
            if (downstreamIds.isEmpty()) {
                return;
            }

            Map<String, Object> timerInfo = new LinkedHashMap<>();
            timerInfo.put("firedAt", OffsetDateTime.now(ZoneOffset.UTC).toString());
            timerInfo.put("kind", kindOf(fe));
            timerInfo.put("due", dueOf(fe));

            ExecutionContext ctx = new ExecutionContext();
            ctx.setWorkflowExecutionId(businessKey);
            ctx.setCurrentNodeId(nodeId);
            ProcessContextRef writeRef = contextStore.snapshot(ctx, null);

            List<InOutValueDto> outputs = new ArrayList<>(downstreamIds.size());
            for (String downstreamId : downstreamIds) {
                InOutValueDto dto = new InOutValueDto();
                dto.setNodeId(nodeId);
                dto.setPortId(TIMER_INFO_PORT);
                dto.setPortValue(TIMER_INFO_PORT);
                dto.setTargetNodeId(downstreamId);
                dto.setTargetPortId(TIMER_INFO_PORT);
                dto.setTargetPortValue(TIMER_INFO_PORT);
                dto.setValue(timerInfo);
                outputs.add(dto);
            }
            contextStore.saveOutputs(writeRef, outputs);
            log.info("timerInfo.emitted node={} kind={} due={} downstream={}",
                    nodeId, timerInfo.get("kind"), timerInfo.get("due"), downstreamIds.size());
        } catch (Exception ex) {
            log.warn("timerInfoEmission: fan-out failed for activity={}: {}", activityId, ex.toString());
        }
    }

    /** {@code date} / {@code duration} / {@code cycle} — whichever expression the definition sets. */
    private static String kindOf(FlowElement fe) {
        TimerEventDefinition timer = timerOf(fe);
        if (timer == null) {
            return null;
        }
        if (StringUtils.isNotBlank(timer.getTimeDate())) {
            return "date";
        }
        if (StringUtils.isNotBlank(timer.getTimeDuration())) {
            return "duration";
        }
        if (StringUtils.isNotBlank(timer.getTimeCycle())) {
            return "cycle";
        }
        return null;
    }

    /** The EFFECTIVE due expression on the deployed definition (a bounded cycle reports the bound form). */
    private static String dueOf(FlowElement fe) {
        TimerEventDefinition timer = timerOf(fe);
        if (timer == null) {
            return null;
        }
        if (StringUtils.isNotBlank(timer.getTimeDate())) {
            return timer.getTimeDate();
        }
        if (StringUtils.isNotBlank(timer.getTimeDuration())) {
            return timer.getTimeDuration();
        }
        return timer.getTimeCycle();
    }

    private static TimerEventDefinition timerOf(FlowElement fe) {
        if (!(fe instanceof org.activiti.bpmn.model.Event event)) {
            return null;
        }
        return event.getEventDefinitions().stream()
                .filter(ed -> ed instanceof TimerEventDefinition)
                .map(ed -> (TimerEventDefinition) ed)
                .findFirst()
                .orElse(null);
    }
}
