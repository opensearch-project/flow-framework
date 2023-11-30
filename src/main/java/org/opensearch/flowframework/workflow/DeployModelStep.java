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
import org.opensearch.ml.common.transport.deploy.MLDeployModelResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.opensearch.flowframework.common.CommonValue.MODEL_ID;

/**
 * Step to deploy a model
 */
public class DeployModelStep implements WorkflowStep {
    private static final Logger logger = LogManager.getLogger(DeployModelStep.class);

    private final MachineLearningNodeClient mlClient;
    static final String NAME = "deploy_model";

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     */
    public DeployModelStep(MachineLearningNodeClient mlClient) {
        this.mlClient = mlClient;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs
    ) {

        CompletableFuture<WorkflowData> deployModelFuture = new CompletableFuture<>();

        ActionListener<MLDeployModelResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLDeployModelResponse mlDeployModelResponse) {
                logger.info("Model deployment state {}", mlDeployModelResponse.getStatus());
                deployModelFuture.complete(
                    new WorkflowData(
                        Map.ofEntries(Map.entry("deploy_model_status", mlDeployModelResponse.getStatus())),
                        currentNodeInputs.getWorkflowId(),
                        currentNodeInputs.getNodeId()
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to deploy model");
                deployModelFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
            }
        };

        String modelId = null;

        // TODO: Recreating the list to get this compiling
        // Need to refactor the below iteration to pull directly from the maps
        List<WorkflowData> data = new ArrayList<>();
        data.add(currentNodeInputs);
        data.addAll(outputs.values());

        for (WorkflowData workflowData : data) {
            if (workflowData.getContent().containsKey(MODEL_ID)) {
                modelId = (String) workflowData.getContent().get(MODEL_ID);
                break;
            }
        }

        if (modelId != null) {
            mlClient.deploy(modelId, actionListener);
        } else {
            deployModelFuture.completeExceptionally(new FlowFrameworkException("Model ID is not provided", RestStatus.BAD_REQUEST));
        }

        return deployModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
