/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTask;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_THREAD_POOL;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;

/**
 * Abstract retryable workflow step
 */
public abstract class AbstractRetryableWorkflowStep implements WorkflowStep {
    private static final Logger logger = LogManager.getLogger(AbstractRetryableWorkflowStep.class);
    private TimeValue retryDuration;
    private final MachineLearningNodeClient mlClient;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private ThreadPool threadPool;

    /**
     * Instantiates a new Retryable workflow step
     * @param threadPool The OpenSearch thread pool
     * @param mlClient machine learning client
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     * @param flowFrameworkSettings settings of flow framework
     */
    protected AbstractRetryableWorkflowStep(
        ThreadPool threadPool,
        MachineLearningNodeClient mlClient,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings
    ) {
        this.threadPool = threadPool;
        this.retryDuration = flowFrameworkSettings.getRetryDuration();
        this.mlClient = mlClient;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
    }

    /**
     * Retryable get ml task
     * @param currentNodeInputs the current Node Inputs
     * @param nodeId the workflow node id
     * @param future the workflow step future
     * @param taskId the ml task id
     * @param workflowStep the workflow step which requires a retry get ml task functionality
     * @param tenantId the tenant ID
     * @param mlTaskListener the ML Task Listener
     */
    protected void retryableGetMlTask(
        WorkflowData currentNodeInputs,
        String nodeId,
        PlainActionFuture<WorkflowData> future,
        String taskId,
        String workflowStep,
        String tenantId,
        ActionListener<WorkflowData> mlTaskListener
    ) {
        CompletableFuture.runAsync(() -> {
            do {
                mlClient.getTask(taskId, ActionListener.wrap(response -> {
                    String resourceName = getResourceByWorkflowStep(getName());
                    String id = getResourceId(response);
                    switch (response.getState()) {
                        case COMPLETED:
                            logger.info("{} successful for {} and {} {}", workflowStep, currentNodeInputs, resourceName, id);
                            ActionListener<WorkflowData> resourceListener = ActionListener.wrap(r -> {
                                Map<String, Object> content = new HashMap<>(r.getContent());
                                content.put(REGISTER_MODEL_STATUS, response.getState().toString());
                                mlTaskListener.onResponse(new WorkflowData(content, r.getWorkflowId(), r.getNodeId()));
                            }, mlTaskListener::onFailure);
                            flowFrameworkIndicesHandler.addResourceToStateIndex(
                                currentNodeInputs,
                                nodeId,
                                getName(),
                                id,
                                tenantId,
                                resourceListener
                            );
                            break;
                        case FAILED:
                        case COMPLETED_WITH_ERROR:
                            String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage(
                                "{} failed with error : {}",
                                workflowStep,
                                response.getError()
                            ).getFormattedMessage();
                            logger.error(errorMessage);
                            mlTaskListener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
                            break;
                        case CANCELLED:
                            errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("{} task was cancelled.", workflowStep)
                                .getFormattedMessage();
                            logger.error(errorMessage);
                            mlTaskListener.onFailure(new FlowFrameworkException(errorMessage, RestStatus.REQUEST_TIMEOUT));
                            break;
                        default:
                            // Task started or running, do nothing
                    }
                }, exception -> {
                    String errorMessage = ParameterizedMessageFactory.INSTANCE.newMessage("{} failed", workflowStep).getFormattedMessage();
                    logger.error(errorMessage, exception);
                    mlTaskListener.onFailure(new WorkflowStepException(errorMessage, RestStatus.BAD_REQUEST));
                }));
                try {
                    Thread.sleep(this.retryDuration.getMillis());
                } catch (InterruptedException e) {
                    FutureUtils.cancel(future);
                    Thread.currentThread().interrupt();
                }
            } while (!future.isDone());
        }, threadPool.executor(WORKFLOW_THREAD_POOL));
    }

    /**
     * Returns the resourceId associated with the task
     * @param response The Task response
     * @return the resource ID, such as a model id
     */
    protected abstract String getResourceId(MLTask response);
}
