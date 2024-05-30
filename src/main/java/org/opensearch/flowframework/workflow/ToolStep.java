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
import org.opensearch.common.Booleans;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.common.agent.MLToolSpec;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.INCLUDE_OUTPUT_IN_AGENT_RESPONSE;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PARAMETERS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TOOLS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TYPE;
import static org.opensearch.flowframework.common.WorkflowResources.AGENT_ID;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;

/**
 * Step to register a tool for an agent
 */
public class ToolStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(ToolStep.class);
    PlainActionFuture<WorkflowData> toolFuture = PlainActionFuture.newFuture();
    static final String NAME = "create_tool";

    @Override
    public PlainActionFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs,
        Map<String, String> params
    ) {
        Set<String> requiredKeys = Set.of(TYPE);
        Set<String> optionalKeys = Set.of(NAME_FIELD, DESCRIPTION_FIELD, PARAMETERS_FIELD, INCLUDE_OUTPUT_IN_AGENT_RESPONSE);

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                requiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs,
                params
            );

            String type = (String) inputs.get(TYPE);
            String name = (String) inputs.get(NAME_FIELD);
            String description = (String) inputs.get(DESCRIPTION_FIELD);
            Boolean includeOutputInAgentResponse = ParseUtils.checkIfInputsContainsKey(inputs ,INCLUDE_OUTPUT_IN_AGENT_RESPONSE);
            Map<String, String> parameters = getToolsParametersMap(inputs.get(PARAMETERS_FIELD), previousNodeInputs, outputs);

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
        Map<String, WorkflowData> outputs
    ) {
        @SuppressWarnings("unchecked")
        Map<String, String> parametersMap = (Map<String, String>) parameters;
        Optional<String> previousNodeModel = previousNodeInputs.entrySet()
            .stream()
            .filter(e -> MODEL_ID.equals(e.getValue()))
            .map(Map.Entry::getKey)
            .findFirst();

        Optional<String> previousNodeAgent = previousNodeInputs.entrySet()
            .stream()
            .filter(e -> AGENT_ID.equals(e.getValue()))
            .map(Map.Entry::getKey)
            .findFirst();

        // Case when modelId is passed through previousSteps and not present already in parameters
        if (previousNodeModel.isPresent() && !parametersMap.containsKey(MODEL_ID)) {
            WorkflowData previousNodeOutput = outputs.get(previousNodeModel.get());
            if (previousNodeOutput != null && previousNodeOutput.getContent().containsKey(MODEL_ID)) {
                parametersMap.put(MODEL_ID, previousNodeOutput.getContent().get(MODEL_ID).toString());
            }
        }

        // Case when agentId is passed through previousSteps and not present already in parameters
        if (previousNodeAgent.isPresent() && !parametersMap.containsKey(AGENT_ID)) {
            WorkflowData previousNodeOutput = outputs.get(previousNodeAgent.get());
            if (previousNodeOutput != null && previousNodeOutput.getContent().containsKey(AGENT_ID)) {
                parametersMap.put(AGENT_ID, previousNodeOutput.getContent().get(AGENT_ID).toString());
            }
        }

        // For other cases where modelId is already present in the parameters or not return the parametersMap
        return parametersMap;
    }

}
