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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.common.WorkflowResources;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTask;
import org.opensearch.ml.common.MLTaskState;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.FlowFrameworkSettings.MAX_GET_TASK_REQUEST_RETRY;

/**
 * Abstract retryable workflow step
 */
public abstract class AbstractRetryableWorkflowStep implements WorkflowStep {
    private static final Logger logger = LogManager.getLogger(AbstractRetryableWorkflowStep.class);
    /** The maximum number of transport request retries */
    protected volatile Integer maxRetry;
    private final MachineLearningNodeClient mlClient;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /**
     * Instantiates a new Retryable workflow step
     * @param settings Environment settings
     * @param clusterService the cluster service
     * @param mlClient machine learning client
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public AbstractRetryableWorkflowStep(
        Settings settings,
        ClusterService clusterService,
        MachineLearningNodeClient mlClient,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler
    ) {
        this.maxRetry = MAX_GET_TASK_REQUEST_RETRY.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_GET_TASK_REQUEST_RETRY, it -> maxRetry = it);
        this.mlClient = mlClient;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
    }

    /**
     * Completes the future for either deploy or register local model step
     * @param resourceName resource name for the given step
     * @param nodeId node ID of the given step
     * @param workflowId workflow ID of the given workflow
     * @param response Response from ml commons get Task API
     * @param future CompletableFuture of the given step
     */
    public void completeFuture(String resourceName, String nodeId, String workflowId, MLTask response, CompletableFuture future) {
        future.complete(
            new WorkflowData(
                Map.ofEntries(Map.entry(resourceName, response.getModelId()), Map.entry(REGISTER_MODEL_STATUS, response.getState().name())),
                workflowId,
                nodeId
            )
        );
    }

    /**
     * Retryable get ml task
     * @param workflowId the workflow id
     * @param nodeId the workflow node id
     * @param future the workflow step future
     * @param taskId the ml task id
     * @param retries the current number of request retries
     * @param workflowStep the workflow step which requires a retry get ml task functionality
     */
    void retryableGetMlTask(
        String workflowId,
        String nodeId,
        CompletableFuture<WorkflowData> future,
        String taskId,
        int retries,
        String workflowStep
    ) {
        mlClient.getTask(taskId, ActionListener.wrap(response -> {
            MLTaskState currentState = response.getState();
            if (currentState != MLTaskState.COMPLETED) {
                if (Stream.of(MLTaskState.FAILED, MLTaskState.COMPLETED_WITH_ERROR).anyMatch(x -> x == currentState)) {
                    // Model registration failed or completed with errors
                    String errorMessage = workflowStep + " failed with error : " + response.getError();
                    logger.error(errorMessage);
                    future.completeExceptionally(new FlowFrameworkException(errorMessage, RestStatus.BAD_REQUEST));
                } else {
                    // Task still in progress, attempt retry
                    throw new IllegalStateException(workflowStep + " is not yet completed");
                }
            } else {
                try {
                    logger.info(workflowStep + " successful for {} and modelId {}", workflowId, response.getModelId());
                    String resourceName = WorkflowResources.getResourceByWorkflowStep(getName());
                    if (getName().equals(WorkflowResources.DEPLOY_MODEL.getWorkflowStep())) {
                        completeFuture(resourceName, nodeId, workflowId, response, future);
                    } else {
                        flowFrameworkIndicesHandler.updateResourceInStateIndex(
                            workflowId,
                            nodeId,
                            getName(),
                            response.getTaskId(),
                            ActionListener.wrap(updateResponse -> {
                                logger.info("successfully updated resources created in state index: {}", updateResponse.getIndex());
                                completeFuture(resourceName, nodeId, workflowId, response, future);
                            }, exception -> {
                                logger.error("Failed to update new created resource", exception);
                                future.completeExceptionally(
                                    new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception))
                                );
                            })
                        );
                    }
                } catch (Exception e) {
                    logger.error("Failed to parse and update new created resource", e);
                    future.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
                }
            }
        }, exception -> {
            if (retries < maxRetry) {
                // Sleep thread prior to retrying request
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                    FutureUtils.cancel(future);
                }
                retryableGetMlTask(workflowId, nodeId, future, taskId, retries + 1, workflowStep);
            } else {
                logger.error("Failed to retrieve" + workflowStep + ",maximum retries exceeded");
                future.completeExceptionally(new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception)));
            }
        }));
    }

}
