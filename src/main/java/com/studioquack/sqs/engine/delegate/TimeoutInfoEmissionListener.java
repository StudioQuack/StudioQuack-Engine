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

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Runtime bridge for the message-catch timeout branch — emits the template-promised
 * {@code timeout} OUT envelope ({@code timedOut}, {@code waitedMs}, {@code lastCheckedAt}).
 * Attached (event={@code end}) by {@code ActivitiMessageCatchStrategy} to the synthesized
 * timer catch ({@code node_<id>_timeout}) of the {@code onTimeoutBehavior=route} shape; fires
 * exactly when the timer arm wins the event choice, never at park time.
 *
 * <p>Rows are stamped with the AUTHORED catch node's id and the {@code timeout} port value, so
 * a DuckTape edge leaving the node's Timeout port resolves like any other producer's output.
 * {@code waitedMs} is the EFFECTIVE armed wait, read back off the deployed
 * {@link TimerEventDefinition}.
 *
 * <p><b>Best-effort.</b> The timer already fired when this runs — a fan-out failure must never
 * roll the timer job back (the async executor would retry it and double-fire the token), so a
 * failure is logged and the run continues without the envelope, exactly like
 * {@code TimerInfoEmissionListener}.
 */
@Slf4j
@Component("timeoutInfoEmissionListener")
public class TimeoutInfoEmissionListener implements ExecutionListener {

    /** Activity id prefix the strategies mint: designer id {@code X} → activity id {@code node_X}. */
    private static final String NODE_ID_PREFIX = "node_";
    /** Element id suffix of the synthesized timeout arm ({@code node_<id>_timeout}). */
    private static final String TIMEOUT_ARM_SUFFIX = "_timeout";
    /** OUT port value the message-catch template declares for the timeout branch. */
    private static final String TIMEOUT_PORT = "timeout";

    private final ObjectProvider<SqsEngine> sqsEngineProvider;
    private final ObjectProvider<ExecutionContextStore> executionContextStoreProvider;

    public TimeoutInfoEmissionListener(ObjectProvider<SqsEngine> sqsEngineProvider,
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
        String nodeId = StringUtils.removeEnd(StringUtils.removeStart(activityId, NODE_ID_PREFIX),
                TIMEOUT_ARM_SUFFIX);
        try {
            String businessKey = execution.getProcessInstanceBusinessKey();
            if (StringUtils.isBlank(businessKey)) {
                return;
            }
            BpmnModel model = engine.readDeployedModel(execution.getProcessDefinitionId());
            if (model == null) {
                log.warn("timeoutInfoEmission: deployed model unreadable for {} — timeout envelope lost for {}",
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

            Map<String, Object> timeoutInfo = new LinkedHashMap<>();
            timeoutInfo.put("timedOut", true);
            timeoutInfo.put("waitedMs", waitedMsOf(fe));
            timeoutInfo.put("lastCheckedAt", OffsetDateTime.now(ZoneOffset.UTC).toString());

            ExecutionContext ctx = new ExecutionContext();
            ctx.setWorkflowExecutionId(businessKey);
            ctx.setCurrentNodeId(nodeId);
            ProcessContextRef writeRef = contextStore.snapshot(ctx, null);

            List<InOutValueDto> outputs = new ArrayList<>(downstreamIds.size());
            for (String downstreamId : downstreamIds) {
                InOutValueDto dto = new InOutValueDto();
                dto.setNodeId(nodeId);
                dto.setPortId(TIMEOUT_PORT);
                dto.setPortValue(TIMEOUT_PORT);
                dto.setTargetNodeId(downstreamId);
                dto.setTargetPortId(TIMEOUT_PORT);
                dto.setTargetPortValue(TIMEOUT_PORT);
                dto.setValue(timeoutInfo);
                outputs.add(dto);
            }
            contextStore.saveOutputs(writeRef, outputs);
            log.info("timeoutInfo.emitted node={} waitedMs={} downstream={}",
                    nodeId, timeoutInfo.get("waitedMs"), downstreamIds.size());
        } catch (Exception ex) {
            log.warn("timeoutInfoEmission: fan-out failed for activity={}: {}", activityId, ex.toString());
        }
    }

    /** The effective armed wait in ms, parsed back off the deployed duration ({@code PT5S}). */
    private static Long waitedMsOf(FlowElement fe) {
        if (!(fe instanceof org.activiti.bpmn.model.Event event)) {
            return null;
        }
        return event.getEventDefinitions().stream()
                .filter(ed -> ed instanceof TimerEventDefinition)
                .map(ed -> (TimerEventDefinition) ed)
                .map(TimerEventDefinition::getTimeDuration)
                .filter(StringUtils::isNotBlank)
                .map(duration -> {
                    try {
                        return Duration.parse(duration).toMillis();
                    } catch (Exception ex) {
                        return null;
                    }
                })
                .findFirst()
                .orElse(null);
    }
}
