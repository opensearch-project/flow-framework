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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTaskState;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static org.opensearch.flowframework.common.CommonValue.MODEL_ID;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.CommonValue.TASK_ID;
import static org.opensearch.flowframework.util.RetryUtils.shouldRetry;

/**
 * Step to retrieve an ML Task
 */
public class GetMLTaskStep extends AbstractRetryableWorkflowStep {

    private static final Logger logger = LogManager.getLogger(GetMLTaskStep.class);
    private MachineLearningNodeClient mlClient;
    static final String NAME = "get_ml_task";

    /**
     * Instantiate this class
     * @param settings the Opensearch settings
     * @param clusterService the OpenSearch cluster service
     * @param mlClient client to instantiate MLClient
     */
    public GetMLTaskStep(Settings settings, ClusterService clusterService, MachineLearningNodeClient mlClient) {
        super(settings, clusterService);
        this.mlClient = mlClient;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {

        CompletableFuture<WorkflowData> getMLTaskFuture = new CompletableFuture<>();

        String taskId = null;

        for (WorkflowData workflowData : data) {
            Map<String, Object> content = workflowData.getContent();
            for (Entry<String, Object> entry : content.entrySet()) {
                switch (entry.getKey()) {
                    case TASK_ID:
                        taskId = (String) content.get(TASK_ID);
                        break;
                    default:
                        break;
                }
            }
        }

        if (taskId == null) {
            logger.error("Failed to retrieve ML Task");
            getMLTaskFuture.completeExceptionally(new FlowFrameworkException("Required fields are not provided", RestStatus.BAD_REQUEST));
        } else {
            retryableGetMlTask(data, getMLTaskFuture, taskId, 0);
        }

        return getMLTaskFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private void retryableGetMlTask(List<WorkflowData> data, CompletableFuture<WorkflowData> getMLTaskFuture, String taskId, int retries) {
        mlClient.getTask(taskId, ActionListener.wrap(response -> {
            if (response.getState() != MLTaskState.COMPLETED) {
                throw new IllegalStateException("MLTask is not yet completed");
            } else {
                logger.info("ML Task retrieval successful");
                getMLTaskFuture.complete(
                    new WorkflowData(
                        Map.ofEntries(
                            Map.entry(MODEL_ID, response.getModelId()),
                            Map.entry(REGISTER_MODEL_STATUS, response.getState().name())
                        ),
                        data.get(0).getWorkflowId()
                    )
                );
            }
        }, exception -> {
            if (shouldRetry(retries, maxRetry)) {
                // Sleep thread prior to retrying request
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                    getMLTaskFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
                }
                final int retryAdd = retries + 1;
                retryableGetMlTask(data, getMLTaskFuture, taskId, retryAdd);
            } else {
                logger.error("Failed to retrieve ML Task, maximum retries exceeded");
                getMLTaskFuture.completeExceptionally(
                    new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception))
                );
            }
        }));
    }

}
