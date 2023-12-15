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
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.transport.deploy.MLDeployModelResponse;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.opensearch.flowframework.common.CommonValue.MODEL_ID;

/**
 * Step to deploy a model
 */
public class DeployModelStep extends AbstractRetryableWorkflowStep {
    private static final Logger logger = LogManager.getLogger(DeployModelStep.class);

    private final MachineLearningNodeClient mlClient;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    static final String NAME = "deploy_model";

    /**
     * Instantiate this class
     * @param settings The OpenSearch settings
     * @param clusterService The cluster service
     * @param mlClient client to instantiate MLClient
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public DeployModelStep(
        Settings settings,
        ClusterService clusterService,
        MachineLearningNodeClient mlClient,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler
    ) {
        super(settings, clusterService, mlClient, flowFrameworkIndicesHandler);
        this.mlClient = mlClient;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
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
                String taskId = mlDeployModelResponse.getTaskId();

                // Attempt to retrieve the model ID
                retryableGetMlTask(currentNodeInputs.getWorkflowId(), currentNodeId, deployModelFuture, taskId, 0, "Deploy model");
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to deploy model");
                deployModelFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
            }
        };

        Set<String> requiredKeys = Set.of(MODEL_ID);
        Set<String> optionalKeys = Collections.emptySet();

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                requiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs
            );

            String modelId = (String) inputs.get(MODEL_ID);

            mlClient.deploy(modelId, actionListener);
        } catch (FlowFrameworkException e) {
            deployModelFuture.completeExceptionally(e);
        }
        return deployModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
