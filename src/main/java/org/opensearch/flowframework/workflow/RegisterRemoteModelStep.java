/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import static org.opensearch.flowframework.common.CommonValue.CONNECTOR_ID;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.FUNCTION_NAME;
import static org.opensearch.flowframework.common.CommonValue.MODEL_GROUP_ID;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.common.WorkflowResources;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput.MLRegisterModelInputBuilder;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;

/**
 * Step to register a remote model
 */
public class RegisterRemoteModelStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(RegisterRemoteModelStep.class);

    private final MachineLearningNodeClient mlClient;

    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    static final String NAME = WorkflowResources.REGISTER_REMOTE_MODEL.getWorkflowStep();

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
    public CompletableFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs
    ) {

        CompletableFuture<WorkflowData> registerRemoteModelFuture = new CompletableFuture<>();

        ActionListener<MLRegisterModelResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLRegisterModelResponse mlRegisterModelResponse) {

                try {
                    logger.info("Remote Model registration successful");
                    String resourceName = WorkflowResources.getResourceByWorkflowStep(getName());
                    flowFrameworkIndicesHandler.updateResourceInStateIndex(
                        currentNodeInputs.getWorkflowId(),
                        currentNodeId,
                        getName(),
                        mlRegisterModelResponse.getModelId(),
                        ActionListener.wrap(response -> {
                            logger.info("successfully updated resources created in state index: {}", response.getIndex());
                            registerRemoteModelFuture.complete(
                                new WorkflowData(
                                    Map.ofEntries(
                                        Map.entry(resourceName, mlRegisterModelResponse.getModelId()),
                                        Map.entry(REGISTER_MODEL_STATUS, mlRegisterModelResponse.getStatus())
                                    ),
                                    currentNodeInputs.getWorkflowId(),
                                    currentNodeInputs.getNodeId()
                                )
                            );
                        }, exception -> {
                            logger.error("Failed to update new created resource", exception);
                            registerRemoteModelFuture.completeExceptionally(
                                new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception))
                            );
                        })
                    );

                } catch (Exception e) {
                    logger.error("Failed to parse and update new created resource", e);
                    registerRemoteModelFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to register remote model");
                registerRemoteModelFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
            }
        };

        Set<String> requiredKeys = Set.of(NAME_FIELD, FUNCTION_NAME, CONNECTOR_ID);
        Set<String> optionalKeys = Set.of(MODEL_GROUP_ID, DESCRIPTION_FIELD);

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                requiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs
            );

            String modelName = (String) inputs.get(NAME_FIELD);
            FunctionName functionName = FunctionName.from(((String) inputs.get(FUNCTION_NAME)).toUpperCase(Locale.ROOT));
            String modelGroupId = (String) inputs.get(MODEL_GROUP_ID);
            String description = (String) inputs.get(DESCRIPTION_FIELD);
            String connectorId = (String) inputs.get(CONNECTOR_ID);

            MLRegisterModelInputBuilder builder = MLRegisterModelInput.builder()
                .functionName(functionName)
                .modelName(modelName)
                .connectorId(connectorId);

            if (modelGroupId != null) {
                builder.modelGroupId(modelGroupId);
            }
            if (description != null) {
                builder.description(description);
            }
            MLRegisterModelInput mlInput = builder.build();

            mlClient.register(mlInput, actionListener);

        } catch (FlowFrameworkException e) {
            registerRemoteModelFuture.completeExceptionally(e);
        }
        return registerRemoteModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
