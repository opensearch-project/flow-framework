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
import org.opensearch.ml.common.connector.ConnectorAction;
import org.opensearch.ml.common.connector.ConnectorAction.ActionType;
import org.opensearch.ml.common.transport.connector.MLCreateConnectorInput;
import org.opensearch.ml.common.transport.connector.MLCreateConnectorResponse;

import java.io.IOException;
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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.CommonValue.ACTIONS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.CREDENTIALS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PARAMETERS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROTOCOL_FIELD;
import static org.opensearch.flowframework.common.CommonValue.VERSION_FIELD;

/**
 * Step to create a connector for a remote model
 */
public class CreateConnectorStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateConnectorStep.class);

    private MachineLearningNodeClient mlClient;

    static final String NAME = "create_connector";

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     */
    public CreateConnectorStep(MachineLearningNodeClient mlClient) {
        this.mlClient = mlClient;
    }

    @Override
    public CompletableFuture<WorkflowData> execute(List<WorkflowData> data) throws IOException {
        CompletableFuture<WorkflowData> createConnectorFuture = new CompletableFuture<>();

        ActionListener<MLCreateConnectorResponse> actionListener = new ActionListener<>() {

            @Override
            public void onResponse(MLCreateConnectorResponse mlCreateConnectorResponse) {
                logger.info("Created connector successfully");
                // TODO Add the response to Global Context
                createConnectorFuture.complete(
                    new WorkflowData(Map.ofEntries(Map.entry("connector_id", mlCreateConnectorResponse.getConnectorId())))
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to create connector");
                createConnectorFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
            }
        };

        String name = null;
        String description = null;
        String version = null;
        String protocol = null;
        Map<String, String> parameters = Collections.emptyMap();
        Map<String, String> credentials = Collections.emptyMap();
        List<ConnectorAction> actions = Collections.emptyList();

        for (WorkflowData workflowData : data) {
            for (Entry<String, Object> entry : workflowData.getContent().entrySet()) {
                switch (entry.getKey()) {
                    case NAME_FIELD:
                        name = (String) entry.getValue();
                        break;
                    case DESCRIPTION_FIELD:
                        description = (String) entry.getValue();
                        break;
                    case VERSION_FIELD:
                        version = (String) entry.getValue();
                        break;
                    case PROTOCOL_FIELD:
                        protocol = (String) entry.getValue();
                        break;
                    case PARAMETERS_FIELD:
                        parameters = getParameterMap(entry.getValue());
                        break;
                    case CREDENTIALS_FIELD:
                        credentials = getStringToStringMap(entry.getValue(), CREDENTIALS_FIELD);
                        break;
                    case ACTIONS_FIELD:
                        actions = getConnectorActionList(entry.getValue());
                        break;
                }
            }
        }

        if (Stream.of(name, description, version, protocol, parameters, credentials, actions).allMatch(x -> x != null)) {
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
        } else {
            createConnectorFuture.completeExceptionally(
                new FlowFrameworkException("Required fields are not provided", RestStatus.BAD_REQUEST)
            );
        }

        return createConnectorFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> getStringToStringMap(Object map, String fieldName) {
        if (map instanceof Map) {
            return (Map<String, String>) map;
        }
        throw new IllegalArgumentException("[" + fieldName + "] must be a key-value map.");
    }

    private static Map<String, String> getParameterMap(Object parameterMap) {
        Map<String, String> parameters = new HashMap<>();
        for (Entry<String, String> entry : getStringToStringMap(parameterMap, PARAMETERS_FIELD).entrySet()) {
            try {
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    parameters.put(entry.getKey(), entry.getValue());
                    return null;
                });
            } catch (PrivilegedActionException e) {
                throw new RuntimeException(e);
            }
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
