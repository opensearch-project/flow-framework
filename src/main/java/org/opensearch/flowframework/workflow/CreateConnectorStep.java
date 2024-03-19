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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.connector.ConnectorAction;
import org.opensearch.ml.common.connector.ConnectorAction.ActionType;
import org.opensearch.ml.common.transport.connector.MLCreateConnectorInput;
import org.opensearch.ml.common.transport.connector.MLCreateConnectorResponse;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.ACTIONS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.CREDENTIAL_FIELD;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PARAMETERS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROTOCOL_FIELD;
import static org.opensearch.flowframework.common.CommonValue.VERSION_FIELD;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;
import static org.opensearch.flowframework.util.ParseUtils.getStringToStringMap;

/**
 * Step to create a connector for a remote model
 */
public class CreateConnectorStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateConnectorStep.class);

    private MachineLearningNodeClient mlClient;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "create_connector";

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public CreateConnectorStep(MachineLearningNodeClient mlClient, FlowFrameworkIndicesHandler flowFrameworkIndicesHandler) {
        this.mlClient = mlClient;
        this.flowFrameworkIndicesHandler = flowFrameworkIndicesHandler;
    }

    // TODO: need to add retry conflicts here
    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {
        PlainActionFuture<WorkflowData> createConnectorFuture = PlainActionFuture.newFuture();

        ActionListener<MLCreateConnectorResponse> actionListener = new ActionListener<>() {

            @Override
            public void onResponse(MLCreateConnectorResponse mlCreateConnectorResponse) {
                String resourceName = getResourceByWorkflowStep(getName());
                try {
                    logger.info("Created connector successfully");
                    flowFrameworkIndicesHandler.updateResourceInStateIndex(
                        currentNodeInputs.getWorkflowId(),
                        currentNodeId,
                        getName(),
                        mlCreateConnectorResponse.getConnectorId(),
                        ActionListener.wrap(response -> {
                            logger.info("successfully updated resources created in state index: {}", response.getIndex());
                            createConnectorFuture.onResponse(
                                new WorkflowData(
                                    Map.ofEntries(Map.entry(resourceName, mlCreateConnectorResponse.getConnectorId())),
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
                                + mlCreateConnectorResponse.getConnectorId();
                            logger.error(errorMessage, exception);
                            createConnectorFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception)));
                        })
                    );

                } catch (Exception e) {
                    String errorMessage = "Failed to parse and update new created resource";
                    logger.error(errorMessage, e);
                    createConnectorFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
                }
            }

            @Override
            public void onFailure(Exception e) {
                String errorMessage = "Failed to create connector";
                logger.error(errorMessage, e);
                createConnectorFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
            }
        };

        Set<String> requiredKeys = Set.of(
            NAME_FIELD,
            DESCRIPTION_FIELD,
            VERSION_FIELD,
            PROTOCOL_FIELD,
            PARAMETERS_FIELD,
            CREDENTIAL_FIELD,
            ACTIONS_FIELD
        );
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

            String name = (String) inputs.get(NAME_FIELD);
            String description = (String) inputs.get(DESCRIPTION_FIELD);
            String version = (String) inputs.get(VERSION_FIELD);
            String protocol = (String) inputs.get(PROTOCOL_FIELD);
            Map<String, String> parameters;
            Map<String, String> credentials;
            List<ConnectorAction> actions;

            try {
                parameters = getParameterMap(inputs.get(PARAMETERS_FIELD));
                credentials = getStringToStringMap(inputs.get(CREDENTIAL_FIELD), CREDENTIAL_FIELD);
                actions = getConnectorActionList(inputs.get(ACTIONS_FIELD));
                // TODO: check for un-needed substitution? ${{create_connector. and remove field so we don't need almost duplicate templates
            } catch (IllegalArgumentException iae) {
                logger.error("IllegalArgumentException in connector configuration", iae);
                throw new FlowFrameworkException("IllegalArgumentException in connector configuration", RestStatus.BAD_REQUEST);
            } catch (PrivilegedActionException pae) {
                logger.error("PrivilegedActionException in connector configuration", pae);
                throw new FlowFrameworkException("PrivilegedActionException in connector configuration", RestStatus.UNAUTHORIZED);
            }

            MLCreateConnectorInput mlInput = MLCreateConnectorInput.builder()
                .name(name)
                .description(description)
                .version(version)
                .protocol(protocol)
                .parameters(parameters)
                .credential(credentials)
                .actions(actions)
                .build();

            mlClient.createConnector(mlInput, actionListener);
        } catch (FlowFrameworkException e) {
            createConnectorFuture.onFailure(e);
        }
        return createConnectorFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private static Map<String, String> getParameterMap(Object parameterMap) throws PrivilegedActionException {
        Map<String, String> parameters = new HashMap<>();
        for (Entry<String, String> entry : getStringToStringMap(parameterMap, PARAMETERS_FIELD).entrySet()) {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                parameters.put(entry.getKey(), entry.getValue());
                return null;
            });
        }
        return parameters;
    }

    private static List<ConnectorAction> getConnectorActionList(Object array) {
        if (!(array instanceof Map[])) {
            throw new IllegalArgumentException("[" + ACTIONS_FIELD + "] must be an array of key-value maps.");
        }
        List<ConnectorAction> actions = new ArrayList<>();
        for (Map<?, ?> map : (Map<?, ?>[]) array) {
            String actionType = (String) map.get(ConnectorAction.ACTION_TYPE_FIELD);
            if (actionType == null) {
                throw new IllegalArgumentException("[" + ConnectorAction.ACTION_TYPE_FIELD + "] is missing.");
            }
            @SuppressWarnings("unchecked")
            ConnectorAction action = ConnectorAction.builder()
                .actionType(ActionType.valueOf(actionType.toUpperCase(Locale.ROOT)))
                .method((String) map.get(ConnectorAction.METHOD_FIELD))
                .url((String) map.get(ConnectorAction.URL_FIELD))
                .headers((Map<String, String>) map.get(ConnectorAction.HEADERS_FIELD))
                .requestBody((String) map.get(ConnectorAction.REQUEST_BODY_FIELD))
                .preProcessFunction((String) map.get(ConnectorAction.ACTION_PRE_PROCESS_FUNCTION))
                .postProcessFunction((String) map.get(ConnectorAction.ACTION_POST_PROCESS_FUNCTION))
                .build();
            actions.add(action);
        }
        return actions;
    }

}
