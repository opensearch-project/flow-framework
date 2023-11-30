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
import org.opensearch.ml.common.AccessMode;
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupInput;
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupInput.MLRegisterModelGroupInputBuilder;
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static org.opensearch.flowframework.common.CommonValue.ADD_ALL_BACKEND_ROLES;
import static org.opensearch.flowframework.common.CommonValue.BACKEND_ROLES_FIELD;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.MODEL_ACCESS_MODE;
import static org.opensearch.flowframework.common.CommonValue.MODEL_GROUP_STATUS;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;

/**
 * Step to register a model group
 */
public class ModelGroupStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(ModelGroupStep.class);

    private final MachineLearningNodeClient mlClient;

    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    static final String NAME = WorkflowResources.REGISTER_MODEL_GROUP.getWorkflowStep();

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public ModelGroupStep(MachineLearningNodeClient mlClient, FlowFrameworkIndicesHandler flowFrameworkIndicesHandler) {
        this.mlClient = mlClient;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs
    ) throws IOException {

        CompletableFuture<WorkflowData> registerModelGroupFuture = new CompletableFuture<>();

        ActionListener<MLRegisterModelGroupResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLRegisterModelGroupResponse mlRegisterModelGroupResponse) {
                try {
                    logger.info("Remote Model registration successful");
                    String resourceName = WorkflowResources.getResourceByWorkflowStep(getName());
                    registerModelGroupFuture.complete(
                        new WorkflowData(
                            Map.ofEntries(
                                Map.entry(resourceName, mlRegisterModelGroupResponse.getModelGroupId()),
                                Map.entry(MODEL_GROUP_STATUS, mlRegisterModelGroupResponse.getStatus())
                            ),
                            currentNodeInputs.getWorkflowId(),
                            currentNodeId
                        )
                    );
                    flowFrameworkIndicesHandler.updateResourceInStateIndex(
                        currentNodeInputs.getWorkflowId(),
                        currentNodeId,
                        getName(),
                        mlRegisterModelGroupResponse.getModelGroupId(),
                        registerModelGroupFuture
                    );

                } catch (Exception e) {
                    logger.error("Failed to parse and update new created resource", e);
                    registerModelGroupFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to register model group");
                registerModelGroupFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
            }
        };

        String modelGroupName = null;
        String description = null;
        List<String> backendRoles = new ArrayList<>();
        AccessMode modelAccessMode = null;
        Boolean isAddAllBackendRoles = null;

        // TODO: Recreating the list to get this compiling
        // Need to refactor the below iteration to pull directly from the maps
        List<WorkflowData> data = new ArrayList<>();
        data.add(currentNodeInputs);
        data.addAll(outputs.values());

        for (WorkflowData workflowData : data) {
            Map<String, Object> content = workflowData.getContent();

            for (Entry<String, Object> entry : content.entrySet()) {
                switch (entry.getKey()) {
                    case NAME_FIELD:
                        modelGroupName = (String) content.get(NAME_FIELD);
                        break;
                    case DESCRIPTION_FIELD:
                        description = (String) content.get(DESCRIPTION_FIELD);
                        break;
                    case BACKEND_ROLES_FIELD:
                        backendRoles = getBackendRoles(content);
                        break;
                    case MODEL_ACCESS_MODE:
                        modelAccessMode = (AccessMode) content.get(MODEL_ACCESS_MODE);
                        break;
                    case ADD_ALL_BACKEND_ROLES:
                        isAddAllBackendRoles = (Boolean) content.get(ADD_ALL_BACKEND_ROLES);
                        break;
                    default:
                        break;
                }
            }
        }

        if (modelGroupName == null) {
            registerModelGroupFuture.completeExceptionally(
                new FlowFrameworkException("Model group name is not provided", RestStatus.BAD_REQUEST)
            );
        } else {
            MLRegisterModelGroupInputBuilder builder = MLRegisterModelGroupInput.builder();
            builder.name(modelGroupName);
            if (description != null) {
                builder.description(description);
            }
            if (!backendRoles.isEmpty()) {
                builder.backendRoles(backendRoles);
            }
            if (modelAccessMode != null) {
                builder.modelAccessMode(modelAccessMode);
            }
            if (isAddAllBackendRoles != null) {
                builder.isAddAllBackendRoles(isAddAllBackendRoles);
            }
            MLRegisterModelGroupInput mlInput = builder.build();

            mlClient.registerModelGroup(mlInput, actionListener);
        }

        return registerModelGroupFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @SuppressWarnings("unchecked")
    private List<String> getBackendRoles(Map<String, Object> content) {
        return (List<String>) content.get(BACKEND_ROLES_FIELD);
    }
}
