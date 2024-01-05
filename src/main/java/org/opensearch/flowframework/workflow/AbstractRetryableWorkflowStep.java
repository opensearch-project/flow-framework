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
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTask;
import org.opensearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.flowframework.common.CommonValue.PROVISION_THREAD_POOL;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;

/**
 * Abstract retryable workflow step
 */
public abstract class AbstractRetryableWorkflowStep implements WorkflowStep {
    private static final Logger logger = LogManager.getLogger(AbstractRetryableWorkflowStep.class);
    /** The maximum number of transport request retries */
    protected volatile Integer maxRetry;
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
        this.maxRetry = flowFrameworkSettings.getMaxRetry();
        this.mlClient = mlClient;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
    }

    /**
     * Retryable get ml task
     * @param workflowId the workflow id
     * @param nodeId the workflow node id
     * @param future the workflow step future
     * @param taskId the ml task id
     * @param workflowStep the workflow step which requires a retry get ml task functionality
     */
    protected void retryableGetMlTask(
        String workflowId,
        String nodeId,
        CompletableFuture<WorkflowData> future,
        String taskId,
        String workflowStep
    ) {
        AtomicInteger retries = new AtomicInteger();
        CompletableFuture.runAsync(() -> {
            while (retries.getAndIncrement() < this.maxRetry && !future.isDone()) {
                mlClient.getTask(taskId, ActionListener.wrap(response -> {
                    switch (response.getState()) {
                        case COMPLETED:
                            try {
                                String resourceName = getResourceByWorkflowStep(getName());
                                String id = getResourceId(response);
                                logger.info("{} successful for {} and {} {}", workflowStep, workflowId, resourceName, id);
                                flowFrameworkIndicesHandler.updateResourceInStateIndex(
                                    workflowId,
                                    nodeId,
                                    getName(),
                                    id,
                                    ActionListener.wrap(updateResponse -> {
                                        logger.info("successfully updated resources created in state index: {}", updateResponse.getIndex());
                                        future.complete(
                                            new WorkflowData(
                                                Map.ofEntries(
                                                    Map.entry(resourceName, id),
                                                    Map.entry(REGISTER_MODEL_STATUS, response.getState().name())
                                                ),
                                                workflowId,
                                                nodeId
                                            )
                                        );
                                    }, exception -> {
                                        logger.error("Failed to update new created resource", exception);
                                        future.completeExceptionally(
                                            new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception))
                                        );
                                    })
                                );
                            } catch (Exception e) {
                                logger.error("Failed to parse and update new created resource", e);
                                future.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
                            }
                            break;
                        case FAILED:
                        case COMPLETED_WITH_ERROR:
                            String errorMessage = workflowStep + " failed with error : " + response.getError();
                            logger.error(errorMessage);
                            future.completeExceptionally(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
                            break;
                        case CANCELLED:
                            errorMessage = workflowStep + " task was cancelled.";
                            logger.error(errorMessage);
                            future.completeExceptionally(new FlowFrameworkException(errorMessage, RestStatus.REQUEST_TIMEOUT));
                            break;
                        default:
                            // Task started or running, do nothing
                    }
                }, exception -> {
                    String errorMessage = workflowStep + " failed with error : " + exception.getMessage();
                    logger.error(errorMessage);
                    future.completeExceptionally(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
                }));
                // Wait long enough for future to possibly complete
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    FutureUtils.cancel(future);
                    Thread.currentThread().interrupt();
                }
            }
            if (!future.isDone()) {
                String errorMessage = workflowStep + " did not complete after " + maxRetry + " retries";
                logger.error(errorMessage);
                future.completeExceptionally(new FlowFrameworkException(errorMessage, RestStatus.REQUEST_TIMEOUT));
            }
        }, threadPool.executor(PROVISION_THREAD_POOL));
    }

    /**
     * Returns the resourceId associated with the task
     * @param response The Task response
     * @return the resource ID, such as a model id
     */
    protected abstract String getResourceId(MLTask response);
}
