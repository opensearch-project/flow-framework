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
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION;
import static org.opensearch.flowframework.common.CommonValue.MODEL_ACCESS_MODE;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;

/**
 * Step to register a model group
 */
public class ModelGroupStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(RegisterModelStep.class);

    private MachineLearningNodeClient mlClient;

    static final String NAME = "model_group";

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     */
    public ModelGroupStep(MachineLearningNodeClient mlClient) {
        this.mlClient = mlClient;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) throws IOException {

        CompletableFuture<WorkflowData> registerModelGroupFuture = new CompletableFuture<>();

        ActionListener<MLRegisterModelGroupResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLRegisterModelGroupResponse mlRegisterModelGroupResponse) {
                logger.info("Model group registration successful");
                registerModelGroupFuture.complete(
                    new WorkflowData(
                        Map.ofEntries(
                            Map.entry("model_group_id", mlRegisterModelGroupResponse.getModelGroupId()),
                            Map.entry("model_group_status", mlRegisterModelGroupResponse.getStatus())
                        )
                    )
                );
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
        Boolean isAddAllBackendRoles = false;

        for (WorkflowData workflowData : data) {
            Map<String, Object> content = workflowData.getContent();

            for (Entry<String, Object> entry : content.entrySet()) {
                switch (entry.getKey()) {
                    case NAME_FIELD:
                        modelGroupName = (String) content.get(NAME_FIELD);
                        break;
                    case DESCRIPTION:
                        description = (String) content.get(DESCRIPTION);
                        break;
                    case BACKEND_ROLES_FIELD:
                        backendRoles = (List<String>) content.get(BACKEND_ROLES_FIELD);
                    case MODEL_ACCESS_MODE:
                        modelAccessMode = (AccessMode) content.get(MODEL_ACCESS_MODE);
                    case ADD_ALL_BACKEND_ROLES:
                        isAddAllBackendRoles = (Boolean) content.get(ADD_ALL_BACKEND_ROLES);
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
            if (backendRoles != null && backendRoles.size() > 0) {
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
}
