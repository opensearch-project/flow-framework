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
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.client.MLClient;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.transport.deploy.MLDeployModelResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.opensearch.flowframework.common.CommonValue.MODEL_ID;

/**
 * Step to deploy a model
 */
public class DeployModelStep implements WorkflowStep {
    private static final Logger logger = LogManager.getLogger(DeployModelStep.class);

    private Client client;
    static final String NAME = "deploy_model";

    /**
     * Instantiate this class
     * @param client client to instantiate MLClient
     */
    public DeployModelStep(Client client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {

        CompletableFuture<WorkflowData> deployModelFuture = new CompletableFuture<>();

        MachineLearningNodeClient machineLearningNodeClient = MLClient.createMLClient(client);

        ActionListener<MLDeployModelResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLDeployModelResponse mlDeployModelResponse) {
                logger.info("Model deployment state {}", mlDeployModelResponse.getStatus());
                deployModelFuture.complete(
                    new WorkflowData(Map.ofEntries(Map.entry("deploy_model_status", mlDeployModelResponse.getStatus())))
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Model deployment failed");
                deployModelFuture.completeExceptionally(e);
            }
        };

        String modelId = null;

        for (WorkflowData workflowData : data) {
            if (workflowData.getContent().containsKey(MODEL_ID)) {
                modelId = (String) workflowData.getContent().get(MODEL_ID);
                break;
            }
        }
        machineLearningNodeClient.deploy(modelId, actionListener);
        return deployModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
