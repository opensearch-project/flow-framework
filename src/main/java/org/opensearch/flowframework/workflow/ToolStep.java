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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.common.agent.MLToolSpec;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.CONFIG_FIELD;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.INCLUDE_OUTPUT_IN_AGENT_RESPONSE;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PARAMETERS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TOOLS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TYPE;
import static org.opensearch.flowframework.common.WorkflowResources.AGENT_ID;
import static org.opensearch.flowframework.common.WorkflowResources.CONNECTOR_ID;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;

/**
 * Step to register a tool for an agent
 */
public class ToolStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(ToolStep.class);
    PlainActionFuture<WorkflowData> toolFuture = PlainActionFuture.newFuture();

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "create_tool";
    /** Required input keys */
    public static final Set<String> REQUIRED_INPUTS = Set.of(TYPE);
    /** Optional input keys */
    public static final Set<String> OPTIONAL_INPUTS = Set.of(
        NAME_FIELD,
        DESCRIPTION_FIELD,
        PARAMETERS_FIELD,
        CONFIG_FIELD,
        INCLUDE_OUTPUT_IN_AGENT_RESPONSE
    );
    /** Provided output keys */
    public static final Set<String> PROVIDED_OUTPUTS = Set.of(TOOLS_FIELD);

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params,
        String tenantId
    ) {
        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                REQUIRED_INPUTS,
                OPTIONAL_INPUTS,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );

            String type = (String) inputs.get(TYPE);
            String name = (String) inputs.get(NAME_FIELD);
            String description = (String) inputs.get(DESCRIPTION_FIELD);
            Boolean includeOutputInAgentResponse = ParseUtils.parseIfExists(inputs, INCLUDE_OUTPUT_IN_AGENT_RESPONSE, Boolean.class);

            // parse connector_id, model_id and agent_id from previous node inputs
            Set<String> toolParameterKeys = Set.of(CONNECTOR_ID, MODEL_ID, AGENT_ID);
            Map<String, String> parameters = getToolsParametersMap(
                inputs.getOrDefault(PARAMETERS_FIELD, new HashMap<>()),
                previousNodeInputs,
                outputs,
                toolParameterKeys
            );
            @SuppressWarnings("unchecked")
            Map<String, String> config = (Map<String, String>) inputs.getOrDefault(CONFIG_FIELD, Collections.emptyMap());

            MLToolSpec.MLToolSpecBuilder builder = MLToolSpec.builder();

            builder.type(type);
            if (name != null) {
                builder.name(name);
            }
            if (description != null) {
                builder.description(description);
            }
            if (parameters != null) {
                builder.parameters(parameters);
            }
            if (includeOutputInAgentResponse != null) {
                builder.includeOutputInAgentResponse(includeOutputInAgentResponse);
            }
            builder.configMap(config);

            MLToolSpec mlToolSpec = builder.build();

            toolFuture.onResponse(
                new WorkflowData(
                    Map.ofEntries(Map.entry(TOOLS_FIELD, mlToolSpec)),
                    currentNodeInputs.getWorkflowId(),
                    currentNodeInputs.getNodeId()
                )
            );

            logger.info("Tool registered successfully {}", type);

        } catch (IllegalArgumentException iae) {
            toolFuture.onFailure(new WorkflowStepException(iae.getMessage(), RestStatus.BAD_REQUEST));
        } catch (FlowFrameworkException e) {
            toolFuture.onFailure(e);
        }
        return toolFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private Map<String, String> getToolsParametersMap(
        Object parameters,
        Map<String, String> previousNodeInputs,
        Map<String, WorkflowData> outputs,
        Set<String> toolParameterKeys
    ) {
        @SuppressWarnings("unchecked")
        Map<String, String> parametersMap = (Map<String, String>) parameters;

        for (String toolParameterKey : toolParameterKeys) {
            Optional<String> previousNodeParameter = previousNodeInputs.entrySet()
                .stream()
                .filter(e -> toolParameterKey.equals(e.getValue()))
                .map(Map.Entry::getKey)
                .findFirst();

            // Case when toolParameterKey is passed through previousSteps and not present already in parameters
            if (previousNodeParameter.isPresent() && !parametersMap.containsKey(toolParameterKey)) {
                WorkflowData previousNodeOutput = outputs.get(previousNodeParameter.get());
                if (previousNodeOutput != null && previousNodeOutput.getContent().containsKey(toolParameterKey)) {
                    parametersMap.put(toolParameterKey, previousNodeOutput.getContent().get(toolParameterKey).toString());
                }
            }
        }

        // For other cases where toolParameterKey is already present in the parameters or not return the parametersMap
        return parametersMap;
    }

}
