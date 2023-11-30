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
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.model.ResourceCreated;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.connector.ConnectorAction;
import org.opensearch.ml.common.connector.ConnectorAction.ActionType;
import org.opensearch.ml.common.transport.connector.MLCreateConnectorInput;
import org.opensearch.ml.common.transport.connector.MLCreateConnectorResponse;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

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
import static org.opensearch.flowframework.common.CommonValue.CREDENTIAL_FIELD;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PARAMETERS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PROTOCOL_FIELD;
import static org.opensearch.flowframework.common.CommonValue.VERSION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;

/**
 * Step to create a connector for a remote model
 */
public class CreateConnectorStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(CreateConnectorStep.class);

    private MachineLearningNodeClient mlClient;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    static final String NAME = "create_connector";

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
    public CompletableFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs
    ) throws IOException {
        CompletableFuture<WorkflowData> createConnectorFuture = new CompletableFuture<>();

        ActionListener<MLCreateConnectorResponse> actionListener = new ActionListener<>() {

            @Override
            public void onResponse(MLCreateConnectorResponse mlCreateConnectorResponse) {
                String workflowId = currentNodeInputs.getWorkflowId();
                createConnectorFuture.complete(
                    new WorkflowData(
                        Map.ofEntries(Map.entry("connector_id", mlCreateConnectorResponse.getConnectorId())),
                        workflowId,
                        currentNodeInputs.getNodeId()
                    )
                );
                try {
                    logger.info("Created connector successfully");
                    String workflowStepName = getName();
                    ResourceCreated newResource = new ResourceCreated(workflowStepName, mlCreateConnectorResponse.getConnectorId());
                    XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
                    newResource.toXContent(builder, ToXContentObject.EMPTY_PARAMS);

                    // The script to append a new object to the resources_created array
                    Script script = new Script(
                        ScriptType.INLINE,
                        "painless",
                        "ctx._source.resources_created.add(params.newResource)",
                        Collections.singletonMap("newResource", newResource)
                    );

                    flowFrameworkIndicesHandler.updateFlowFrameworkSystemIndexDocWithScript(
                        WORKFLOW_STATE_INDEX,
                        workflowId,
                        script,
                        ActionListener.wrap(updateResponse -> {
                            logger.info("updated resources created of {}", workflowId);
                        }, exception -> {
                            createConnectorFuture.completeExceptionally(
                                new FlowFrameworkException(exception.getMessage(), ExceptionsHelper.status(exception))
                            );
                            logger.error("Failed to update workflow state with newly created resource: {}", exception);
                        })
                    );
                } catch (IOException e) {
                    logger.error("Failed to parse new created resource", e);
                }
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

        // TODO: Recreating the list to get this compiling
        // Need to refactor the below iteration to pull directly from the maps
        List<WorkflowData> data = new ArrayList<>();
        data.add(currentNodeInputs);
        data.addAll(outputs.values());

        try {
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
                        case CREDENTIAL_FIELD:
                            credentials = getStringToStringMap(entry.getValue(), CREDENTIAL_FIELD);
                            break;
                        case ACTIONS_FIELD:
                            actions = getConnectorActionList(entry.getValue());
                            break;
                    }
                }
            }
        } catch (IllegalArgumentException iae) {
            createConnectorFuture.completeExceptionally(new FlowFrameworkException(iae.getMessage(), RestStatus.BAD_REQUEST));
            return createConnectorFuture;
        } catch (PrivilegedActionException pae) {
            createConnectorFuture.completeExceptionally(new FlowFrameworkException(pae.getMessage(), RestStatus.UNAUTHORIZED));
            return createConnectorFuture;
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
