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
import org.opensearch.ml.common.transport.connector.MLCreateConnectorInput;
import org.opensearch.ml.common.transport.connector.MLCreateConnectorResponse;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.CommonValue.ACTIONS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.CREDENTIALS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION;
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
        Map<String, String> parameters = new HashMap<>();
        Map<String, String> credentials = new HashMap<>();
        List<ConnectorAction> actions = null;

        for (WorkflowData workflowData : data) {
            Map<String, Object> content = workflowData.getContent();

            for (Entry<String, Object> entry : content.entrySet()) {
                switch (entry.getKey()) {
                    case NAME_FIELD:
                        name = (String) content.get(NAME_FIELD);
                        break;
                    case DESCRIPTION:
                        description = (String) content.get(DESCRIPTION);
                        break;
                    case VERSION_FIELD:
                        version = (String) content.get(VERSION_FIELD);
                        break;
                    case PROTOCOL_FIELD:
                        protocol = (String) content.get(PROTOCOL_FIELD);
                        break;
                    case PARAMETERS_FIELD:
                        parameters = getParameterMap((Map<String, String>) content.get(PARAMETERS_FIELD));
                        break;
                    case CREDENTIALS_FIELD:
                        credentials = (Map<String, String>) content.get(CREDENTIALS_FIELD);
                        break;
                    case ACTIONS_FIELD:
                        actions = (List<ConnectorAction>) content.get(ACTIONS_FIELD);
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

    private static Map<String, String> getParameterMap(Map<String, String> params) {

        Map<String, String> parameters = new HashMap<>();
        for (String key : params.keySet()) {
            String value = params.get(key);
            try {
                AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                    parameters.put(key, value);
                    return null;
                });
            } catch (PrivilegedActionException e) {
                throw new RuntimeException(e);
            }
        }
        return parameters;
    }

}
