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
import org.opensearch.flowframework.common.WorkflowResources;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput.MLRegisterModelInputBuilder;
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
import static org.opensearch.flowframework.common.CommonValue.MODEL_GROUP_ID;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.REGISTER_MODEL_STATUS;

/**
 * Step to register a remote model
 */
public class RegisterRemoteModelStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(RegisterRemoteModelStep.class);

    private MachineLearningNodeClient mlClient;
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
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) {

        CompletableFuture<WorkflowData> registerRemoteModelFuture = new CompletableFuture<>();

        ActionListener<MLRegisterModelResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLRegisterModelResponse mlRegisterModelResponse) {
                try {
                    logger.info("Remote Model registration successful");
                    String resourceName = WorkflowResources.getResourceByWorkflowStep(getName());
                    registerRemoteModelFuture.complete(
                        new WorkflowData(
                            Map.ofEntries(
                                Map.entry(resourceName, mlRegisterModelResponse.getModelId()),
                                Map.entry(REGISTER_MODEL_STATUS, mlRegisterModelResponse.getStatus())
                            ),
                            data.get(0).getWorkflowId()
                        )
                    );
                    flowFrameworkIndicesHandler.updateResourceInStateIndex(
                        data.get(0),
                        getName(),
                        mlRegisterModelResponse.getModelId(),
                        registerRemoteModelFuture
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

        String modelName = null;
        FunctionName functionName = null;
        String modelGroupId = null;
        String description = null;
        String connectorId = null;

        // TODO : Handle inline connector configuration : https://github.com/opensearch-project/flow-framework/issues/149

        for (WorkflowData workflowData : data) {

            Map<String, Object> content = workflowData.getContent();
            for (Entry<String, Object> entry : content.entrySet()) {
                switch (entry.getKey()) {
                    case NAME_FIELD:
                        modelName = (String) content.get(NAME_FIELD);
                        break;
                    case FUNCTION_NAME:
                        functionName = FunctionName.from(((String) content.get(FUNCTION_NAME)).toUpperCase(Locale.ROOT));
                        break;
                    case MODEL_GROUP_ID:
                        modelGroupId = (String) content.get(MODEL_GROUP_ID);
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

        if (Stream.of(modelName, functionName, connectorId).allMatch(x -> x != null)) {

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
        } else {
            registerRemoteModelFuture.completeExceptionally(
                new FlowFrameworkException("Required fields are not provided", RestStatus.BAD_REQUEST)
            );
        }

        return registerRemoteModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
