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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTask;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static org.opensearch.flowframework.common.CommonValue.MODEL_ID;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.CommonValue.TASK_ID;

/**
 * Step to retrieve an ML Task
 */
public class GetMLTaskStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(GetMLTaskStep.class);
    private MachineLearningNodeClient mlClient;
    static final String NAME = "get_ml_task";

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     */
    public GetMLTaskStep(MachineLearningNodeClient mlClient) {
        this.mlClient = mlClient;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {

        CompletableFuture<WorkflowData> getMLTaskFuture = new CompletableFuture<>();

        ActionListener<MLTask> actionListener = ActionListener.wrap(response -> {

            // TODO : Add retry capability if response status is not COMPLETED :
            // https://github.com/opensearch-project/flow-framework/issues/158

            logger.info("ML Task retrieval successful");
            getMLTaskFuture.complete(
                new WorkflowData(
                    Map.ofEntries(Map.entry(MODEL_ID, response.getModelId()), Map.entry(REGISTER_MODEL_STATUS, response.getState().name())),
                    data.get(0).getWorkflowId()
                )
            );
        }, exception -> {
            logger.error("Failed to retrieve ML Task");
            getMLTaskFuture.completeExceptionally(new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception)));
        });

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
            mlClient.getTask(taskId, actionListener);
        }

        return getMLTaskFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
