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
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.model.MLModelConfig;
import org.opensearch.ml.common.model.MLModelFormat;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelResponse;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.CommonValue.CONNECTOR_ID;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.FUNCTION_NAME;
import static org.opensearch.flowframework.common.CommonValue.MODEL_CONFIG;
import static org.opensearch.flowframework.common.CommonValue.MODEL_FORMAT;
import static org.opensearch.flowframework.common.CommonValue.MODEL_GROUP_ID;
import static org.opensearch.flowframework.common.CommonValue.MODEL_VERSION;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;

/**
 * Step to register a remote model
 */
public class RegisterModelStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(RegisterModelStep.class);

    private MachineLearningNodeClient mlClient;

    static final String NAME = "register_model";

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     */
    public RegisterModelStep(MachineLearningNodeClient mlClient) {
        this.mlClient = mlClient;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {

        CompletableFuture<WorkflowData> registerModelFuture = new CompletableFuture<>();

        ActionListener<MLRegisterModelResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLRegisterModelResponse mlRegisterModelResponse) {
                logger.info("Model registration successful");
                registerModelFuture.complete(
                    new WorkflowData(
                        Map.ofEntries(
                            Map.entry("model_id", mlRegisterModelResponse.getModelId()),
                            Map.entry("register_model_status", mlRegisterModelResponse.getStatus())
                        )
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to register model");
                registerModelFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
            }
        };

        FunctionName functionName = null;
        String modelName = null;
        String modelVersion = null;
        String modelGroupId = null;
        String connectorId = null;
        String description = null;
        MLModelFormat modelFormat = null;
        MLModelConfig modelConfig = null;

        for (WorkflowData workflowData : data) {
            Map<String, Object> content = workflowData.getContent();

            for (Entry<String, Object> entry : content.entrySet()) {
                switch (entry.getKey()) {
                    case FUNCTION_NAME:
                        functionName = FunctionName.from(((String) content.get(FUNCTION_NAME)).toUpperCase(Locale.ROOT));
                        break;
                    case NAME_FIELD:
                        modelName = (String) content.get(NAME_FIELD);
                        break;
                    case MODEL_VERSION:
                        modelVersion = (String) content.get(MODEL_VERSION);
                        break;
                    case MODEL_GROUP_ID:
                        modelGroupId = (String) content.get(MODEL_GROUP_ID);
                        break;
                    case MODEL_FORMAT:
                        modelFormat = MLModelFormat.from((String) content.get(MODEL_FORMAT));
                        break;
                    case MODEL_CONFIG:
                        modelConfig = (MLModelConfig) content.get(MODEL_CONFIG);
                        break;
                    case DESCRIPTION_FIELD:
                        description = (String) content.get(DESCRIPTION_FIELD);
                        break;
                    case CONNECTOR_ID:
                        connectorId = (String) content.get(CONNECTOR_ID);
                        break;
                    default:
                        break;

                }
            }
        }

        if (Stream.of(functionName, modelName, description, connectorId).allMatch(x -> x != null)) {

            // TODO: Add model Config and type cast correctly
            MLRegisterModelInput mlInput = MLRegisterModelInput.builder()
                .functionName(functionName)
                .modelName(modelName)
                .description(description)
                .connectorId(connectorId)
                .build();

            mlClient.register(mlInput, actionListener);
        } else {
            registerModelFuture.completeExceptionally(
                new FlowFrameworkException("Required fields are not provided", RestStatus.BAD_REQUEST)
            );
        }

        return registerModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }
}
