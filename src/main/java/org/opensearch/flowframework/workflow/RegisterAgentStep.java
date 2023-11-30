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
import org.opensearch.ml.common.agent.LLMSpec;
import org.opensearch.ml.common.agent.MLAgent;
import org.opensearch.ml.common.agent.MLAgent.MLAgentBuilder;
import org.opensearch.ml.common.agent.MLMemorySpec;
import org.opensearch.ml.common.agent.MLToolSpec;
import org.opensearch.ml.common.transport.agent.MLRegisterAgentResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.opensearch.flowframework.common.CommonValue.*;
import static org.opensearch.flowframework.util.ParseUtils.getStringToStringMap;

/**
 * Step to register an agent
 */
public class RegisterAgentStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(RegisterAgentStep.class);

    private MachineLearningNodeClient mlClient;

    static final String NAME = "register_agent";

    private static final String LLM_MODEL_ID = "llm.model_id";
    private static final String LLM_PARAMETERS = "llm.parameters";

    private List<MLToolSpec> mlToolSpecList;

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     */
    public RegisterAgentStep(MachineLearningNodeClient mlClient) {
        this.mlClient = mlClient;
        this.mlToolSpecList = new ArrayList<>();
    }

    @Override
    public CompletableFuture<WorkflowData> execute(
        String currentNodeId,
        WorkflowData currentNodeInputs,
        Map<String, WorkflowData> outputs,
        Map<String, String> previousNodeInputs
    ) throws IOException {

        CompletableFuture<WorkflowData> registerAgentModelFuture = new CompletableFuture<>();

        ActionListener<MLRegisterAgentResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLRegisterAgentResponse mlRegisterAgentResponse) {
                logger.info("Remote Agent registration successful");
                registerAgentModelFuture.complete(
                    new WorkflowData(
                        Map.ofEntries(Map.entry(AGENT_ID, mlRegisterAgentResponse.getAgentId())),
                        currentNodeInputs.getWorkflowId(),
                        currentNodeInputs.getNodeId()
                    )
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to register the agent");
                registerAgentModelFuture.completeExceptionally(new FlowFrameworkException(e.getMessage(), ExceptionsHelper.status(e)));
            }
        };

        String name = null;
        String type = null;
        String description = null;
        LLMSpec llm = null;
        String llmModelId = null;
        Map<String, String> llmParameters = Collections.emptyMap();
        List<MLToolSpec> tools = new ArrayList<>();
        Map<String, String> parameters = Collections.emptyMap();
        MLMemorySpec memory = null;
        Instant createdTime = null;
        Instant lastUpdateTime = null;
        String appType = null;

        // TODO: Recreating the list to get this compiling
        // Need to refactor the below iteration to pull directly from the maps
        List<WorkflowData> data = new ArrayList<>();
        data.add(currentNodeInputs);
        data.addAll(outputs.values());

        for (WorkflowData workflowData : data) {
            Map<String, Object> content = workflowData.getContent();

            for (Entry<String, Object> entry : content.entrySet()) {
                switch (entry.getKey()) {
                    case NAME_FIELD:
                        name = (String) entry.getValue();
                        break;
                    case DESCRIPTION_FIELD:
                        description = (String) entry.getValue();
                        break;
                    case TYPE:
                        type = (String) entry.getValue();
                        break;
                    case LLM_MODEL_ID:
                        llmModelId = getLlmModelId((String) entry.getValue(), previousNodeInputs, outputs);
                        break;
                    case LLM_PARAMETERS:
                        llmParameters = getStringToStringMap(entry.getValue(), LLM_PARAMETERS);
                        break;
                    case TOOLS_FIELD:
                        tools = addTools(entry.getValue());
                        break;
                    case PARAMETERS_FIELD:
                        parameters = getStringToStringMap(entry.getValue(), PARAMETERS_FIELD);
                        break;
                    case MEMORY_FIELD:
                        memory = getMLMemorySpec(entry.getValue());
                        break;
                    case CREATED_TIME:
                        createdTime = Instant.ofEpochMilli((Long) entry.getValue());
                        break;
                    case LAST_UPDATED_TIME_FIELD:
                        lastUpdateTime = Instant.ofEpochMilli((Long) entry.getValue());
                        break;
                    case APP_TYPE_FIELD:
                        appType = (String) entry.getValue();
                        break;
                    default:
                        break;
                }
            }
        }

        LLMSpec llmSpec = getLLMSpec(llmModelId, llmParameters);

        if (Stream.of(name, type, llmSpec).allMatch(x -> x != null)) {
            MLAgentBuilder builder = MLAgent.builder().name(name);

            if (description != null) {
                builder.description(description);
            }

            builder.type(type)
                .llm(llmSpec)
                .tools(tools)
                .parameters(parameters)
                .memory(memory)
                .createdTime(createdTime)
                .lastUpdateTime(lastUpdateTime)
                .appType(appType);

            MLAgent mlAgent = builder.build();

            mlClient.registerAgent(mlAgent, actionListener);

        } else {
            registerAgentModelFuture.completeExceptionally(
                new FlowFrameworkException("Required fields are not provided", RestStatus.BAD_REQUEST)
            );
        }

        return registerAgentModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private List<MLToolSpec> addTools(Object tools) {
        MLToolSpec mlToolSpec = (MLToolSpec) tools;
        mlToolSpecList.add(mlToolSpec);
        return mlToolSpecList;
    }

    private String getLlmModelId(String llmModelId, Map<String, String> previousNodeInputs, Map<String, WorkflowData> outputs) {
        // Case when modelId is already pass in the template
        if (llmModelId != null) {
            return llmModelId;
        }

        // Case when modelId is passed through previousSteps
        Optional<String> previousNode = previousNodeInputs.entrySet()
            .stream()
            .filter(e -> MODEL_ID.equals(e.getValue()))
            .map(Map.Entry::getKey)
            .findFirst();

        if (previousNode.isPresent()) {
            WorkflowData previousNodeOutput = outputs.get(previousNode.get());
            if (previousNodeOutput != null && previousNodeOutput.getContent().containsKey(MODEL_ID)) {
                llmModelId = previousNodeOutput.getContent().get(MODEL_ID).toString();
            }
        }
        return llmModelId;
    }

    private LLMSpec getLLMSpec(String llmModelId, Map<String, String> llmParameters) {
        if (llmModelId == null) {
            throw new IllegalArgumentException("model id for llm is null");
        }
        LLMSpec.LLMSpecBuilder builder = LLMSpec.builder();
        builder.modelId(llmModelId);
        if (llmParameters != null) {
            builder.parameters(llmParameters);
        }

        LLMSpec llmSpec = builder.build();
        return llmSpec;
    }

    private MLMemorySpec getMLMemorySpec(Object mlMemory) {

        Map<?, ?> map = (Map<?, ?>) mlMemory;
        String type = null;
        String sessionId = null;
        Integer windowSize = null;
        type = (String) map.get(MLMemorySpec.MEMORY_TYPE_FIELD);
        if (type == null) {
            throw new IllegalArgumentException("agent name is null");
        }
        sessionId = (String) map.get(MLMemorySpec.SESSION_ID_FIELD);
        windowSize = (Integer) map.get(MLMemorySpec.WINDOW_SIZE_FIELD);

        @SuppressWarnings("unchecked")
        MLMemorySpec.MLMemorySpecBuilder builder = MLMemorySpec.builder();

        builder.type(type);
        if (sessionId != null) {
            builder.sessionId(sessionId);
        }
        if (windowSize != null) {
            builder.windowSize(windowSize);
        }

        MLMemorySpec mlMemorySpec = builder.build();
        return mlMemorySpec;

    }

    private Instant getInstant(Object instant, String fieldName) {
        if (instant instanceof Instant) {
            return (Instant) instant;
        }
        throw new IllegalArgumentException("[" + fieldName + "] must be of type Instant.");
    }

}
