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
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput.MLRegisterModelInputBuilder;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;

import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.DEPLOY_FIELD;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;
import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_GROUP_ID;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;

/**
 * Step to register a remote model
 */
public class RegisterRemoteModelStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(RegisterRemoteModelStep.class);

    private final MachineLearningNodeClient mlClient;

    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "register_remote_model";

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public RegisterRemoteModelStep(MachineLearningNodeClient mlClient, FlowFrameworkIndicesHandler flowFrameworkIndicesHandler) {
        this.mlClient = mlClient;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
    }

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {

        PlainActionFuture<WorkflowData> registerRemoteModelFuture = PlainActionFuture.newFuture();

        Set<String> requiredKeys = Set.of(NAME_FIELD, CONNECTOR_ID);
        Set<String> optionalKeys = Set.of(MODEL_GROUP_ID, DESCRIPTION_FIELD, DEPLOY_FIELD);

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                requiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );

            String modelName = (String) inputs.get(NAME_FIELD);
            String modelGroupId = (String) inputs.get(MODEL_GROUP_ID);
            String description = (String) inputs.get(DESCRIPTION_FIELD);
            String connectorId = (String) inputs.get(CONNECTOR_ID);
            final Boolean deploy = (Boolean) inputs.get(DEPLOY_FIELD);

            MLRegisterModelInputBuilder builder = MLRegisterModelInput.builder()
                .functionName(FunctionName.REMOTE)
                .modelName(modelName)
                .connectorId(connectorId);

            if (modelGroupId != null) {
                builder.modelGroupId(modelGroupId);
            }
            if (description != null) {
                builder.description(description);
            }
            if (deploy != null) {
                builder.deployModel(deploy);
            }
            MLRegisterModelInput mlInput = builder.build();

            mlClient.register(mlInput, new ActionListener<MLRegisterModelResponse>() {
                @Override
                public void onResponse(MLRegisterModelResponse mlRegisterModelResponse) {

                    try {
                        logger.info("Remote Model registration successful");
                        String resourceName = getResourceByWorkflowStep(getName());
                        flowFrameworkIndicesHandler.updateResourceInStateIndex(
                            currentNodeInputs.getWorkflowId(),
                            currentNodeId,
                            getName(),
                            mlRegisterModelResponse.getModelId(),
                            ActionListener.wrap(response -> {
                                // If we deployed, simulate the deploy step has been called
                                if (Boolean.TRUE.equals(deploy)) {
                                    flowFrameworkIndicesHandler.updateResourceInStateIndex(
                                        currentNodeInputs.getWorkflowId(),
                                        currentNodeId,
                                        DeployModelStep.NAME,
                                        mlRegisterModelResponse.getModelId(),
                                        ActionListener.wrap(deployUpdateResponse -> {
                                            completeRegisterFuture(deployUpdateResponse, resourceName, mlRegisterModelResponse);
                                        }, deployUpdateException -> {
                                            String errorMessage = "Failed to update simulated deploy step resource "
                                                + mlRegisterModelResponse.getModelId();
                                            logger.error(errorMessage, deployUpdateException);
                                            registerRemoteModelFuture.onFailure(
                                                new FlowFrameworkException(errorMessage, ExceptionsHelper.status(deployUpdateException))
                                            );
                                        })
                                    );
                                } else {
                                    completeRegisterFuture(response, resourceName, mlRegisterModelResponse);
                                }
                            }, exception -> {
                                String errorMessage = "Failed to update new created "
                                    + currentNodeId
                                    + " resource "
                                    + getName()
                                    + " id "
                                    + mlRegisterModelResponse.getModelId();
                                logger.error(errorMessage, exception);
                                registerRemoteModelFuture.onFailure(
                                    new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception))
                                );
                            })
                        );

                    } catch (Exception e) {
                        String errorMessage = "Failed to parse and update new created resource";
                        logger.error(errorMessage, e);
                        registerRemoteModelFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
                    }
                }

                void completeRegisterFuture(UpdateResponse response, String resourceName, MLRegisterModelResponse mlRegisterModelResponse) {
                    logger.info("successfully updated resources created in state index: {}", response.getIndex());
                    registerRemoteModelFuture.onResponse(
                        new WorkflowData(
                            Map.ofEntries(
                                Map.entry(resourceName, mlRegisterModelResponse.getModelId()),
                                Map.entry(REGISTER_MODEL_STATUS, mlRegisterModelResponse.getStatus())
                            ),
                            currentNodeInputs.getWorkflowId(),
                            currentNodeInputs.getNodeId()
                        )
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    String errorMessage = "Failed to register remote model";
                    logger.error(errorMessage, e);
                    registerRemoteModelFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
                }
            });

        } catch (FlowFrameworkException e) {
            registerRemoteModelFuture.onFailure(e);
        }
        return registerRemoteModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
