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
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.AccessMode;
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupInput;
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupInput.MLRegisterModelGroupInputBuilder;
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupResponse;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.ADD_ALL_BACKEND_ROLES;
import static org.opensearch.flowframework.common.CommonValue.BACKEND_ROLES_FIELD;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.MODEL_ACCESS_MODE;
import static org.opensearch.flowframework.common.CommonValue.MODEL_GROUP_STATUS;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;
import static org.opensearch.flowframework.exception.WorkflowStepException.getSafeException;

/**
 * Step to register a model group
 */
public class RegisterModelGroupStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(RegisterModelGroupStep.class);

    private final MachineLearningNodeClient mlClient;

    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "register_model_group";

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public RegisterModelGroupStep(MachineLearningNodeClient mlClient, FlowFrameworkIndicesHandler flowFrameworkIndicesHandler) {
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
        PlainActionFuture<WorkflowData> registerModelGroupFuture = PlainActionFuture.newFuture();

        ActionListener<MLRegisterModelGroupResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLRegisterModelGroupResponse mlRegisterModelGroupResponse) {
                try {
                    logger.info("Model group registration successful");
                    String resourceName = getResourceByWorkflowStep(getName());
                    flowFrameworkIndicesHandler.updateResourceInStateIndex(
                        currentNodeInputs.getWorkflowId(),
                        currentNodeId,
                        getName(),
                        mlRegisterModelGroupResponse.getModelGroupId(),
                        ActionListener.wrap(updateResponse -> {
                            logger.info("successfully updated resources created in state index: {}", updateResponse.getIndex());
                            registerModelGroupFuture.onResponse(
                                new WorkflowData(
                                    Map.ofEntries(
                                        Map.entry(resourceName, mlRegisterModelGroupResponse.getModelGroupId()),
                                        Map.entry(MODEL_GROUP_STATUS, mlRegisterModelGroupResponse.getStatus())
                                    ),
                                    currentNodeInputs.getWorkflowId(),
                                    currentNodeId
                                )
                            );
                        }, exception -> {
                            String errorMessage = "Failed to update new created "
                                + currentNodeId
                                + " resource "
                                + getName()
                                + " id "
                                + mlRegisterModelGroupResponse.getModelGroupId();
                            logger.error(errorMessage, exception);
                            registerModelGroupFuture.onFailure(
                                new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception))
                            );
                        })
                    );

                } catch (Exception e) {
                    String errorMessage = "Failed to parse and update new created resource";
                    logger.error(errorMessage, e);
                    registerModelGroupFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
                }
            }

            @Override
            public void onFailure(Exception ex) {
                Exception e = getSafeException(ex);
                String errorMessage = (e == null ? "Failed to register model group" : e.getMessage());
                logger.error(errorMessage, e);
                registerModelGroupFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
            }
        };

        Set<String> requiredKeys = Set.of(NAME_FIELD);
        Set<String> optionalKeys = Set.of(DESCRIPTION_FIELD, BACKEND_ROLES_FIELD, MODEL_ACCESS_MODE, ADD_ALL_BACKEND_ROLES);

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                requiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );
            String modelGroupName = (String) inputs.get(NAME_FIELD);
            String description = (String) inputs.get(DESCRIPTION_FIELD);
            List<String> backendRoles = getBackendRoles(inputs);
            AccessMode modelAccessMode = null;
            if (inputs.containsKey(MODEL_ACCESS_MODE)) {
                modelAccessMode = AccessMode.from((inputs.get(MODEL_ACCESS_MODE)).toString().toLowerCase(Locale.ROOT));
            }
            Boolean isAddAllBackendRoles = ParseUtils.parseIfExists(inputs, ADD_ALL_BACKEND_ROLES, Boolean.class);

            MLRegisterModelGroupInputBuilder builder = MLRegisterModelGroupInput.builder();
            builder.name(modelGroupName);
            if (description != null) {
                builder.description(description);
            }
            if (!CollectionUtils.isEmpty(backendRoles)) {
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
        } catch (IllegalArgumentException iae) {
            registerModelGroupFuture.onFailure(new WorkflowStepException(iae.getMessage(), RestStatus.BAD_REQUEST));
        } catch (FlowFrameworkException e) {
            registerModelGroupFuture.onFailure(e);
        }

        return registerModelGroupFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @SuppressWarnings("unchecked")
    private List<String> getBackendRoles(Map<String, Object> content) {
        if (content.containsKey(BACKEND_ROLES_FIELD)) {
            return (List<String>) content.get(BACKEND_ROLES_FIELD);
        }
        return Collections.emptyList();
    }
}
