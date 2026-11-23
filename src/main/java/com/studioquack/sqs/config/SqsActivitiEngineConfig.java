package com.studioquack.sqs.config;

import com.studioquack.sqs.engine.listener.BpmnNodeLogListener;
import com.studioquack.sqs.engine.listener.InheritedErrorSentinelScrubber;
import com.studioquack.sqs.engine.listener.SqStepCancellationListener;
import com.studioquack.sqs.engine.listener.SqsExecutionStatusListener;
import com.studioquack.sqs.engine.quota.NodeExecutionCountListener;
import com.studioquack.sqs.engine.update.UpdateParkListener;
import com.studioquack.sqs.engine.halt.ExecutionHaltTimerStartJobHandler;
import lombok.extern.slf4j.Slf4j;
import org.activiti.engine.delegate.event.ActivitiEventListener;
import org.activiti.engine.impl.history.HistoryLevel;
import org.activiti.engine.impl.persistence.StrongUuidGenerator;
import org.activiti.spring.SpringExpressionManager;
import org.activiti.spring.boot.ProcessEngineConfigurationConfigurer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tunes the embedded Activiti {@code ProcessEngine} for the client daemon. This is the
 * single-tenant counterpart of the control plane: because one daemon serves exactly one
 * environment, there is no tenant scoping — deploy/start/query run without a {@code tenantId}, and
 * tenant filters are dropped.
 */
@Slf4j
@Configuration
public class SqsActivitiEngineConfig {

    @Value("${sqs.activiti.async-executor.core-pool-size:8}")
    private int asyncCorePoolSize;

    @Value("${sqs.activiti.async-executor.max-pool-size:32}")
    private int asyncMaxPoolSize;

    @Value("${sqs.activiti.failed-job-retries:0}")
    private int failedJobRetries;

    /** With the halt guard on, the guard starts job acquisition on the first heartbeat (a cold boot is OFFLINE). */
    @Value("${sqs.execution-halt.enabled:${sqs.channel.enabled:false}}")
    private boolean executionHaltEnabled;

    /**
     * Runs every pooled task with the fat-jar classloader as its thread-context classloader.
     * Activiti's async-executor workers evaluate {@code ${scriptDispatchDelegate}}, which calls
     * {@code jakarta.el.ExpressionFactory.newInstance} and resolves
     * {@code org.apache.el.ExpressionFactoryImpl} through the TCCL.
     */
    @Bean
    public org.springframework.boot.task.ThreadPoolTaskExecutorCustomizer sqsFatJarTcclCustomizer() {
        ClassLoader appClassLoader = SqsActivitiEngineConfig.class.getClassLoader();
        return executor -> executor.setTaskDecorator(task -> () -> {
            Thread current = Thread.currentThread();
            ClassLoader previous = current.getContextClassLoader();
            current.setContextClassLoader(appClassLoader);
            try {
                task.run();
            } finally {
                current.setContextClassLoader(previous);
            }
        });
    }

    /**
     * Bean names the generator emits as {@code ${…}} on service tasks / listeners.
     * Anything else — including {@code channelProperties}, {@code osSecretManager},
     * {@code daemonProcessRunner} — must not be reachable from gateway JUEL.
     */
    static final String[] ALLOWED_EXPRESSION_BEANS = {
            "scriptDispatchDelegate",
            "subProcessCompletionListener",
            "messageSenderExecutionListener",
            "messageReceiverCorrelationListener",
            "signalSenderExecutionListener",
            "escalationSenderExecutionListener",
            "sqBoundaryEventDelegate",
            "sqErrorGateDelegate",
            "sqFailureHoldPolicy",
            "sqFailureHoldDelegate",
            "failureHoldSentinelListener",
            "doclingDelegate",
            "timerInfoEmissionListener",
            "timeoutInfoEmissionListener",
            "linkJumpExecutionListener",
            "callActivityIoListener",
            "callActivityChildContextListener",
            "startContextPublishListener",
            "callActivityChildFailureListener",
            "callActivityFailurePropagationListener",
            "sharedErrorEndSentinelListener",
            "userTaskApprovalListener"
    };

    @Bean
    public ProcessEngineConfigurationConfigurer sqsActivitiEngineConfigurer(
            SqsExecutionStatusListener statusListener,
            BpmnNodeLogListener bpmnNodeLogListener,
            NodeExecutionCountListener nodeExecutionCountListener,
            UpdateParkListener updateParkListener,
            ExecutionHaltTimerStartJobHandler executionHaltTimerStartJobHandler,
            SqStepCancellationListener stepCancellationListener,
            InheritedErrorSentinelScrubber inheritedErrorSentinelScrubber,
            ApplicationContext applicationContext) {
        return cfg -> {
            cfg.setAsyncExecutorActivate(!executionHaltEnabled);
            cfg.setHistoryLevel(HistoryLevel.FULL);
            cfg.setIdGenerator(new StrongUuidGenerator());
            cfg.setAsyncExecutorCorePoolSize(asyncCorePoolSize);
            cfg.setAsyncExecutorMaxPoolSize(asyncMaxPoolSize);
            cfg.setAsyncExecutorNumberOfRetries(failedJobRetries);
            // APPEND status, bpmn node-log, step-cancel, then the sentinel scrubber — never
            // clobber Activiti's own listeners.
            List<ActivitiEventListener> listeners = new ArrayList<>();
            if (cfg.getEventListeners() != null) {
                listeners.addAll(cfg.getEventListeners());
            }
            listeners.add(statusListener);
            listeners.add(bpmnNodeLogListener);
            listeners.add(nodeExecutionCountListener);
            listeners.add(updateParkListener);
            listeners.add(stepCancellationListener);
            listeners.add(inheritedErrorSentinelScrubber);
            cfg.setEventListeners(listeners);
            cfg.setCustomJobHandlers(List.of(executionHaltTimerStartJobHandler));
            cfg.setExpressionManager(new SpringExpressionManager(
                    applicationContext, new AllowedExpressionBeans(applicationContext)));
            log.info("SQS Activiti engine tuned (single-tenant): asyncExecutor={} historyLevel=FULL "
                            + "idGenerator=StrongUuid corePool={} maxPool={} failedJobRetries={} "
                            + "statusListener=on bpmnNodeLogListener=on stepCancellationListener=on "
                            + "inheritedErrorSentinelScrubber=on expressionBeans={}",
                    executionHaltEnabled ? "on-first-heartbeat" : "on",
                    asyncCorePoolSize, asyncMaxPoolSize, failedJobRetries,
                    List.of(ALLOWED_EXPRESSION_BEANS));
        };
    }

    /**
     * Fail-closed bean map for gateway JUEL: only {@link #ALLOWED_EXPRESSION_BEANS} resolve, and each
     * bean is fetched from the context LAZILY at expression-evaluation time. {@code containsKey} never
     * instantiates (definition-registry check only), so the EL resolver's membership probes stay
     * cheap.
     */
    static final class AllowedExpressionBeans extends java.util.AbstractMap<Object, Object> {

        private final ApplicationContext applicationContext;

        AllowedExpressionBeans(ApplicationContext applicationContext) {
            this.applicationContext = applicationContext;
        }

        @Override
        public boolean containsKey(Object key) {
            return key instanceof String name
                    && List.of(ALLOWED_EXPRESSION_BEANS).contains(name)
                    && applicationContext.containsBean(name);
        }

        @Override
        public Object get(Object key) {
            return containsKey(key) ? applicationContext.getBean((String) key) : null;
        }

        /**
         * Tooling-only path (EL feature descriptors) — the runtime resolver
         * ({@code ReadOnlyMapELResolver}) only ever calls {@link #containsKey} / {@link #get}.
         * Resolving entries here would instantiate, so this stays off the hot path by contract.
         */
        @Override
        public Set<Map.Entry<Object, Object>> entrySet() {
            Set<Map.Entry<Object, Object>> entries = new java.util.LinkedHashSet<>();
            for (String name : ALLOWED_EXPRESSION_BEANS) {
                if (applicationContext.containsBean(name)) {
                    entries.add(new java.util.AbstractMap.SimpleEntry<>(name, applicationContext.getBean(name)));
                }
            }
            return entries;
        }
    }
}
