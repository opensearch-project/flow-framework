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
import org.opensearch.flowframework.common.WorkflowResources;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.common.agent.MLToolSpec;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.INCLUDE_OUTPUT_IN_AGENT_RESPONSE;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PARAMETERS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TOOLS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TYPE;

/**
 * Step to register a tool for an agent
 */
public class ToolStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(ToolStep.class);
    CompletableFuture<WorkflowData> toolFuture = new CompletableFuture<>();
    static final String NAME = "create_tool";

    @Override
    public CompletableFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs
    ) {
        Set<String> requiredKeys = Set.of(TYPE);
        Set<String> optionalKeys = Set.of(NAME_FIELD, DESCRIPTION_FIELD, PARAMETERS_FIELD, INCLUDE_OUTPUT_IN_AGENT_RESPONSE);

        try {
            Map<String, Object> inputs = ParseUtils.getInputsFromPreviousSteps(
                requiredKeys,
                optionalKeys,
                currentNodeInputs,
                outputs,
                previousNodeInputs
            );

            String type = (String) inputs.get(TYPE);
            String name = (String) inputs.get(NAME_FIELD);
            String description = (String) inputs.get(DESCRIPTION_FIELD);
            Boolean includeOutputInAgentResponse = (Boolean) inputs.get(INCLUDE_OUTPUT_IN_AGENT_RESPONSE);
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

            toolFuture.complete(
                new WorkflowData(
                    Map.ofEntries(Map.entry(TOOLS_FIELD, mlToolSpec)),
                    currentNodeInputs.getWorkflowId(),
                    currentNodeInputs.getNodeId()
                )
            );

            logger.info("Tool registered successfully {}", type);

        } catch (FlowFrameworkException e) {
            toolFuture.completeExceptionally(e);
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
        Map<String, String> parametersMap = (Map<String, String>) parameters;
        Optional<String> previousNodeModel = previousNodeInputs.entrySet()
            .stream()
            .filter(e -> WorkflowResources.MODEL_ID.equals(e.getValue()))
            .map(Map.Entry::getKey)
            .findFirst();

        Optional<String> previousNodeAgent = previousNodeInputs.entrySet()
            .stream()
            .filter(e -> WorkflowResources.AGENT_ID.equals(e.getValue()))
            .map(Map.Entry::getKey)
            .findFirst();

        // Case when modelId is passed through previousSteps and not present already in parameters
        if (previousNodeModel.isPresent() && !parametersMap.containsKey(WorkflowResources.MODEL_ID)) {
            WorkflowData previousNodeOutput = outputs.get(previousNodeModel.get());
            if (previousNodeOutput != null && previousNodeOutput.getContent().containsKey(WorkflowResources.MODEL_ID)) {
                parametersMap.put(WorkflowResources.MODEL_ID, previousNodeOutput.getContent().get(WorkflowResources.MODEL_ID).toString());
            }
        }

        // Case when agentId is passed through previousSteps and not present already in parameters
        if (previousNodeAgent.isPresent() && !parametersMap.containsKey(WorkflowResources.AGENT_ID)) {
            WorkflowData previousNodeOutput = outputs.get(previousNodeAgent.get());
            if (previousNodeOutput != null && previousNodeOutput.getContent().containsKey(WorkflowResources.AGENT_ID)) {
                parametersMap.put(WorkflowResources.AGENT_ID, previousNodeOutput.getContent().get(WorkflowResources.AGENT_ID).toString());
            }
        }

        // For other cases where modelId is already present in the parameters or not return the parametersMap
        return parametersMap;
    }

}
