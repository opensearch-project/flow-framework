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
import org.opensearch.common.Nullable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.agent.LLMSpec;
import org.opensearch.ml.common.agent.MLAgent;
import org.opensearch.ml.common.agent.MLAgent.MLAgentBuilder;
import org.opensearch.ml.common.agent.MLMemorySpec;
import org.opensearch.ml.common.agent.MLToolSpec;
import org.opensearch.ml.common.transport.agent.MLRegisterAgentResponse;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.APP_TYPE_FIELD;
import static org.opensearch.flowframework.common.CommonValue.CREATED_TIME;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.LAST_UPDATED_TIME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.MEMORY_FIELD;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PARAMETERS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TOOLS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TOOLS_ORDER_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TYPE;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_ID;
import static org.opensearch.flowframework.common.WorkflowResources.getResourceByWorkflowStep;
import static org.opensearch.flowframework.util.ParseUtils.getStringToStringMap;

/**
 * Step to register an agent
 */
public class RegisterAgentStep implements WorkflowStep {

    private static final Logger logger = LogManager.getLogger(RegisterAgentStep.class);

    private MachineLearningNodeClient mlClient;
    private final FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    /** The name of this step, used as a key in the template and the {@link WorkflowStepFactory} */
    public static final String NAME = "register_agent";

    /** The model ID for the LLM */
    public static final String LLM_MODEL_ID = "llm.model_id";
    /** The parameters for the LLM */
    public static final String LLM_PARAMETERS = "llm.parameters";

    /**
     * Instantiate this class
     * @param mlClient client to instantiate MLClient
     * @param flowFrameworkIndicesHandler FlowFrameworkIndicesHandler class to update system indices
     */
    public RegisterAgentStep(MachineLearningNodeClient mlClient, FlowFrameworkIndicesHandler flowFrameworkIndicesHandler) {
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

        String workflowId = currentNodeInputs.getWorkflowId();

        PlainActionFuture<WorkflowData> registerAgentModelFuture = PlainActionFuture.newFuture();

        ActionListener<MLRegisterAgentResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLRegisterAgentResponse mlRegisterAgentResponse) {
                try {
                    String resourceName = getResourceByWorkflowStep(getName());
                    logger.info("Agent registration successful for the agent {}", mlRegisterAgentResponse.getAgentId());
                    flowFrameworkIndicesHandler.updateResourceInStateIndex(
                        workflowId,
                        currentNodeId,
                        getName(),
                        mlRegisterAgentResponse.getAgentId(),
                        ActionListener.wrap(response -> {
                            logger.info("successfully updated resources created in state index: {}", response.getIndex());
                            registerAgentModelFuture.onResponse(
                                new WorkflowData(
                                    Map.ofEntries(Map.entry(resourceName, mlRegisterAgentResponse.getAgentId())),
                                    workflowId,
                                    currentNodeId
                                )
                            );
                        }, exception -> {
                            String errorMessage = "Failed to update new created "
                                + currentNodeId
                                + " resource "
                                + getName()
                                + " id "
                                + mlRegisterAgentResponse.getAgentId();
                            logger.error(errorMessage, exception);
                            registerAgentModelFuture.onFailure(
                                new FlowFrameworkException(errorMessage, ExceptionsHelper.status(exception))
                            );
                        })
                    );

                } catch (Exception e) {
                    String errorMessage = "Failed to parse and update new created resource";
                    logger.error(errorMessage, e);
                    registerAgentModelFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
                }
            }

            @Override
            public void onFailure(Exception e) {
                String errorMessage = "Failed to register the agent";
                logger.error(errorMessage, e);
                registerAgentModelFuture.onFailure(new FlowFrameworkException(errorMessage, ExceptionsHelper.status(e)));
            }
        };

        Set<String> requiredKeys = Set.of(NAME_FIELD, TYPE);
        Set<String> optionalKeys = Set.of(
            DESCRIPTION_FIELD,
            LLM_MODEL_ID,
            LLM_PARAMETERS,
            TOOLS_FIELD,
            TOOLS_ORDER_FIELD,
            PARAMETERS_FIELD,
            MEMORY_FIELD,
            CREATED_TIME,
            LAST_UPDATED_TIME_FIELD,
            APP_TYPE_FIELD
        );

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
            String llmModelId = (String) inputs.get(LLM_MODEL_ID);
            Object llmParams = inputs.get(LLM_PARAMETERS);
            Map<String, String> llmParameters = llmParams == null
                ? Collections.emptyMap()
                : getStringToStringMap(llmParams, LLM_PARAMETERS);
            String[] toolsOrder = (String[]) inputs.get(TOOLS_ORDER_FIELD);
            List<MLToolSpec> toolsList = getTools(toolsOrder, previousNodeInputs, outputs);
            Object parameters = inputs.get(PARAMETERS_FIELD);
            Map<String, String> parametersMap = parameters == null
                ? Collections.emptyMap()
                : getStringToStringMap(parameters, PARAMETERS_FIELD);
            MLMemorySpec memory = getMLMemorySpec(inputs.get(MEMORY_FIELD));
            Instant createdTime = Instant.now();
            Instant lastUpdateTime = createdTime;
            String appType = (String) inputs.get(APP_TYPE_FIELD);

            // Case when modelId is present in previous node inputs
            if (llmModelId == null) {
                llmModelId = getLlmModelId(previousNodeInputs, outputs);
            }

            LLMSpec llmSpec = getLLMSpec(llmModelId, llmParameters, workflowId, currentNodeId);

            MLAgentBuilder builder = MLAgent.builder().name(name);

            if (description != null) {
                builder.description(description);
            }
            if (memory != null) {
                builder.memory(memory);
            }
            if (llmSpec != null) {
                builder.llm(llmSpec);
            }

            builder.type(type)
                .tools(toolsList)
                .parameters(parametersMap)
                .createdTime(createdTime)
                .lastUpdateTime(lastUpdateTime)
                .appType(appType);

            MLAgent mlAgent = builder.build();

            mlClient.registerAgent(mlAgent, actionListener);

        } catch (FlowFrameworkException e) {
            registerAgentModelFuture.onFailure(e);
        }
        return registerAgentModelFuture;
    }

    @Override
    public String getName() {
        return NAME;
    }

    private List<MLToolSpec> getTools(@Nullable String[] tools, Map<String, String> previousNodeInputs, Map<String, WorkflowData> outputs) {
        List<MLToolSpec> mlToolSpecList = new ArrayList<>();
        List<String> previousNodes = previousNodeInputs.entrySet()
            .stream()
            .filter(e -> TOOLS_FIELD.equals(e.getValue()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
        // Anything in tools is sorted first, followed by anything else in previous node inputs
        List<String> sortedNodes = tools == null ? new ArrayList<>() : Arrays.asList(tools);
        previousNodes.removeAll(sortedNodes);
        sortedNodes.addAll(previousNodes);
        sortedNodes.forEach(node -> {
            WorkflowData previousNodeOutput = outputs.get(node);
            if (previousNodeOutput != null && previousNodeOutput.getContent().containsKey(TOOLS_FIELD)) {
                MLToolSpec mlToolSpec = (MLToolSpec) previousNodeOutput.getContent().get(TOOLS_FIELD);
                logger.info("Tool added {}", mlToolSpec.getType());
                mlToolSpecList.add(mlToolSpec);
            }
        });
        return mlToolSpecList;
    }

    private String getLlmModelId(Map<String, String> previousNodeInputs, Map<String, WorkflowData> outputs) {
        // Case when modelId is passed through previousSteps
        Optional<String> previousNode = previousNodeInputs.entrySet()
            .stream()
            .filter(e -> MODEL_ID.equals(e.getValue()))
            .map(Map.Entry::getKey)
            .findFirst();

        if (previousNode.isPresent()) {
            WorkflowData previousNodeOutput = outputs.get(previousNode.get());
            if (previousNodeOutput != null) {
                // Use either llm.model_id (if present) or model_id (backup)
                Object modelId = previousNodeOutput.getContent().getOrDefault(LLM_MODEL_ID, previousNodeOutput.getContent().get(MODEL_ID));
                if (modelId != null) {
                    return modelId.toString();
                }
            }
        }
        return null;
    }

    private LLMSpec getLLMSpec(String llmModelId, Map<String, String> llmParameters, String workflowId, String currentNodeId) {
        if (llmModelId == null) {
            return null;
        }
        LLMSpec.LLMSpecBuilder builder = LLMSpec.builder();
        builder.modelId(llmModelId);
        if (llmParameters != null) {
            builder.parameters(llmParameters);
        }

        return builder.build();
    }

    private MLMemorySpec getMLMemorySpec(Object mlMemory) {
        if (mlMemory == null) {
            return null;
        }

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

        MLMemorySpec.MLMemorySpecBuilder builder = MLMemorySpec.builder();

        builder.type(type);
        if (sessionId != null) {
            builder.sessionId(sessionId);
        }
        if (windowSize != null) {
            builder.windowSize(windowSize);
        }

        return builder.build();
    }
}
