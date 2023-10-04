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
import org.opensearch.common.SuppressForbidden;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTask;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.transport.task.MLTaskGetResponse;

/**
 * Step to get modelID of a registered local model
 */
@SuppressForbidden(reason = "This class is for the future work of registering local model")
public class GetTask {

    private static final Logger logger = LogManager.getLogger(GetTask.class);
    private MachineLearningNodeClient machineLearningNodeClient;
    private String taskId;

    /**
     * Instantiate this class
     * @param machineLearningNodeClient client to instantiate ml-commons APIs
     * @param taskId taskID of the model
     */
    public GetTask(MachineLearningNodeClient machineLearningNodeClient, String taskId) {
        this.machineLearningNodeClient = machineLearningNodeClient;
        this.taskId = taskId;
    }

    /**
     * Invokes get task API of ml-commons
     */
    public void getTask() {

        ActionListener<MLTask> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLTask mlTask) {
                if (mlTask.getState() == MLTaskState.COMPLETED) {
                    logger.info("Model registration successful");
                    MLTaskGetResponse response = MLTaskGetResponse.builder().mlTask(mlTask).build();
                    logger.info("Response from task {}", response);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Model registration failed");
            }
        };

        machineLearningNodeClient.getTask(taskId, actionListener);

    }

}
