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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.ml.common.agent.MLToolSpec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.opensearch.flowframework.common.CommonValue.*;

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
    ) throws IOException {
        String type = null;
        String name = null;
        String description = null;
        Map<String, String> parameters = Collections.emptyMap();
        Boolean includeOutputInAgentResponse = null;

        // TODO: Recreating the list to get this compiling
        // Need to refactor the below iteration to pull directly from the maps
        List<WorkflowData> data = new ArrayList<>();
        data.add(currentNodeInputs);
        data.addAll(outputs.values());

        for (WorkflowData workflowData : data) {
            Map<String, Object> content = workflowData.getContent();

            for (Entry<String, Object> entry : content.entrySet()) {
                switch (entry.getKey()) {
                    case TYPE:
                        type = (String) entry.getValue();
                        break;
                    case NAME_FIELD:
                        name = (String) entry.getValue();
                        break;
                    case DESCRIPTION_FIELD:
                        description = (String) entry.getValue();
                        break;
                    case PARAMETERS_FIELD:
                        parameters = getToolsParametersMap(entry.getValue(), previousNodeInputs, outputs);
                        break;
                    case INCLUDE_OUTPUT_IN_AGENT_RESPONSE:
                        includeOutputInAgentResponse = (Boolean) entry.getValue();
                        break;
                    default:
                        break;
                }

            }

        }

        if (type == null) {
            toolFuture.completeExceptionally(new FlowFrameworkException("Tool type is not provided", RestStatus.BAD_REQUEST));
        } else {
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
        }

        logger.info("Tool registered successfully {}", type);
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
