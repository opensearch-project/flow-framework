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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.common.FlowFrameworkSettings;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLTask;
import org.opensearch.ml.common.transport.deploy.MLDeployModelResponse;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;
import static org.opensearch.flowframework.exception.WorkflowStepException.getSafeException;

/**
 * Step to deploy a model
 */
public class DeployModelStep extends AbstractRetryableWorkflowStep {
    private static final Logger logger = LogManager.getLogger(DeployModelStep.class);

    private final MachineLearningNodeClient mlClient;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "deploy_model";

    /**
     * Instantiate this class
     * @param threadPool The OpenSearch thread pool
     * @param mlClient client to instantiate MLClient
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     * @param flowFrameworkSettings settings of flow framework
     */
    public DeployModelStep(
        ThreadPool threadPool,
        MachineLearningNodeClient mlClient,
        FlowFrameworkIndicesHandler flowFrameworkIndicesHandler,
        FlowFrameworkSettings flowFrameworkSettings
    ) {
        super(threadPool, mlClient, flowFrameworkIndicesHandler, flowFrameworkSettings);
        this.mlClient = mlClient;
    }

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {

        PlainActionFuture<WorkflowData> deployModelFuture = PlainActionFuture.newFuture();

        Set<String> requiredKeys = Set.of(MODEL_ID);
        Set<String> optionalKeys = Collections.emptySet();

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                requiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );

            String modelId = (String) inputs.get(MODEL_ID);

            mlClient.deploy(modelId, new ActionListener<>() {
                @Override
                public void onResponse(MLDeployModelResponse mlDeployModelResponse) {
                    logger.info("Model deployment state {}", mlDeployModelResponse.getStatus());
                    String taskId = mlDeployModelResponse.getTaskId();

                    // Attempt to retrieve the model ID
                    retryableGetMlTask(
                        currentNodeInputs.getWorkflowId(),
                        currentNodeId,
                        deployModelFuture,
                        taskId,
                        "Deploy model",
                        ActionListener.wrap(mlTask -> {
                            // Deployed Model Resource has been updated
                            String resourceName = getResourceByWorkflowStep(getName());
                            String id = getResourceId(mlTask);
                            deployModelFuture.onResponse(
                                new WorkflowData(Map.of(resourceName, id), currentNodeInputs.getWorkflowId(), currentNodeId)
                            );
                        },
                            e -> {
                                deployModelFuture.onFailure(
                                    new FlowFrameworkException("Failed to deploy model", ExceptionsHelper.status(e))
                                );
                            }
                        )
                    );
                }

                @Override
                public void onFailure(Exception ex) {
                    Exception e = getSafeException(ex);
                    String errorMessage = (e == null ? "Failed to deploy model " + modelId : e.getMessage());
                    logger.error(errorMessage, e);
                    deployModelFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
                }
            });
        } catch (FlowFrameworkException e) {
            deployModelFuture.onFailure(e);
        }
        return deployModelFuture;
    }

    @Override
    protected String getResourceId(MLTask response) {
        return response.getModelId();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
