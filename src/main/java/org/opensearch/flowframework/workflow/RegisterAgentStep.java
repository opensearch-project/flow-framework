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
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.Nullable;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.exception.WorkflowStepException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.agent.LLMSpec;
import org.opensearch.ml.common.agent.MLAgent;
import org.opensearch.ml.common.agent.MLAgent.MLAgentBuilder;
import org.opensearch.ml.common.agent.MLAgentModelSpec;
import org.opensearch.ml.common.agent.MLMemorySpec;
import org.opensearch.ml.common.agent.MLToolSpec;
import org.opensearch.ml.common.contextmanager.ContextManagementTemplate;
import org.opensearch.ml.common.transport.agent.MLRegisterAgentResponse;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.flowframework.common.CommonValue.APP_TYPE_FIELD;
import static org.opensearch.flowframework.common.CommonValue.CONTEXT_MANAGEMENT_FIELD;
import static org.opensearch.flowframework.common.CommonValue.CONTEXT_MANAGEMENT_NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.CREATED_TIME;
import static org.opensearch.flowframework.common.CommonValue.DESCRIPTION_FIELD;
import static org.opensearch.flowframework.common.CommonValue.LAST_UPDATED_TIME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.LLM;
import static org.opensearch.flowframework.common.CommonValue.MEMORY_FIELD;
import static org.opensearch.flowframework.common.CommonValue.MODEL_FIELD;
import static org.opensearch.flowframework.common.CommonValue.NAME_FIELD;
import static org.opensearch.flowframework.common.CommonValue.PARAMETERS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TOOLS_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TOOLS_ORDER_FIELD;
import static org.opensearch.flowframework.common.CommonValue.TYPE;
import static org.opensearch.flowframework.common.WorkflowResources.AGENT_ID;
import static org.opensearch.flowframework.exception.WorkflowStepException.getSafeException;
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
    /** Required input keys **/
    public static final Set<String> REQUIRED_INPUTS = Set.of(NAME_FIELD, TYPE);
    /** Optional input keys */
    public static final Set<String> OPTIONAL_INPUTS = Set.of(
        DESCRIPTION_FIELD,
        LLM,
        TOOLS_FIELD,
        TOOLS_ORDER_FIELD,
        PARAMETERS_FIELD,
        MEMORY_FIELD,
        CREATED_TIME,
        LAST_UPDATED_TIME_FIELD,
        APP_TYPE_FIELD
    );
    /** Provided output keys */
    public static final Set<String> PROVIDED_OUTPUTS = Set.of(AGENT_ID);
    /** The model ID for the LLM */
    public static final String MODEL_ID = "model_id";

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
        Map<String, String> params,
        String tenantId
    ) {

        String workflowId = currentNodeInputs.getWorkflowId();

        PlainActionFuture<WorkflowData> registerAgentModelFuture = PlainActionFuture.newFuture();

        ActionListener<MLRegisterAgentResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(MLRegisterAgentResponse mlRegisterAgentResponse) {
                logger.info("Agent registration successful for the agent {}", mlRegisterAgentResponse.getAgentId());
                flowFrameworkIndicesHandler.addResourceToStateIndex(
                    currentNodeInputs,
                    currentNodeId,
                    getName(),
                    mlRegisterAgentResponse.getAgentId(),
                    tenantId,
                    registerAgentModelFuture
                );
            }

            @Override
            public void onFailure(Exception ex) {
                Exception e = getSafeException(ex);
                String errorMessage = (e == null ? "Failed to register the agent" : e.getMessage());
                logger.error(errorMessage, e);
                registerAgentModelFuture.onFailure(new WorkflowStepException(errorMessage, ExceptionsHelper.status(e)));
            }
        };

        Set<String> requiredKeys = Set.of(NAME_FIELD, TYPE);
        Set<String> optionalKeys = Set.of(
            DESCRIPTION_FIELD,
            LLM,
            MODEL_FIELD,
            TOOLS_FIELD,
            TOOLS_ORDER_FIELD,
            PARAMETERS_FIELD,
            MEMORY_FIELD,
            CREATED_TIME,
            LAST_UPDATED_TIME_FIELD,
            APP_TYPE_FIELD,
            CONTEXT_MANAGEMENT_NAME_FIELD,
            CONTEXT_MANAGEMENT_FIELD
        );

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
            String llmField = (String) inputs.get(LLM);
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
            String contextManagementName = (String) inputs.get(CONTEXT_MANAGEMENT_NAME_FIELD);
            String contextManagementField = (String) inputs.get(CONTEXT_MANAGEMENT_FIELD);
            String modelField = (String) inputs.get(MODEL_FIELD);

            String llmModelId = null;
            Map<String, String> llmParameters = new HashMap<>();
            if (llmField != null) {
                try {
                    // Convert llm field string to map
                    Map<String, Object> llmFieldMap = getParseFieldMap(llmField);
                    llmModelId = (String) llmFieldMap.get(MODEL_ID);
                    Object llmParams = llmFieldMap.get(PARAMETERS_FIELD);

                    if (llmParams != null) {
                        validateLLMParametersMap(llmParams);
                        @SuppressWarnings("unchecked")
                        Map<String, String> llmParamsMap = (Map<String, String>) llmParams;
                        llmParameters.putAll(llmParamsMap);
                    }
                } catch (IllegalArgumentException ex) {
                    String errorMessage = "Failed to parse llm field: " + ex.getMessage();
                    logger.error(errorMessage, ex);
                    registerAgentModelFuture.onFailure(new WorkflowStepException(ex.getMessage(), RestStatus.BAD_REQUEST));
                    return registerAgentModelFuture;
                }
            }

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
                .appType(appType)
                .tenantId(tenantId);

            if (contextManagementName != null) {
                builder.contextManagementName(contextManagementName);
            }
            if (contextManagementField != null) {
                try {
                    BytesReference cmBytes = new BytesArray(contextManagementField.getBytes(StandardCharsets.UTF_8));
                    var cmParser = XContentHelper.createParser(
                        org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                        org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS,
                        cmBytes,
                        MediaTypeRegistry.JSON
                    );
                    cmParser.nextToken();
                    builder.contextManagement(ContextManagementTemplate.parse(cmParser));
                } catch (Exception ex) {
                    String errorMessage = "Failed to parse context_management field: " + ex.getMessage();
                    logger.error(errorMessage, ex);
                    registerAgentModelFuture.onFailure(new WorkflowStepException(errorMessage, RestStatus.BAD_REQUEST));
                    return registerAgentModelFuture;
                }
            }
            if (modelField != null) {
                Map<String, Object> modelMap = getParseFieldMap(modelField);
                MLAgentModelSpec.MLAgentModelSpecBuilder modelSpecBuilder = MLAgentModelSpec.builder();
                modelSpecBuilder.modelId((String) modelMap.get(MLAgentModelSpec.MODEL_ID_FIELD));
                modelSpecBuilder.modelProvider((String) modelMap.get(MLAgentModelSpec.MODEL_PROVIDER_FIELD));
                @SuppressWarnings("unchecked")
                Map<String, String> modelCred = (Map<String, String>) modelMap.get(MLAgentModelSpec.CREDENTIAL_FIELD);
                if (modelCred != null) {
                    modelSpecBuilder.credential(modelCred);
                }
                @SuppressWarnings("unchecked")
                Map<String, String> modelParams = (Map<String, String>) modelMap.get(MLAgentModelSpec.MODEL_PARAMETERS_FIELD);
                if (modelParams != null) {
                    modelSpecBuilder.modelParameters(modelParams);
                }
                builder.model(modelSpecBuilder.build());
            }

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
                Object modelId = previousNodeOutput.getContent().getOrDefault(MODEL_ID, previousNodeOutput.getContent().get(MODEL_ID));
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
        String memoryContainerId = (String) map.get(MLMemorySpec.MEMORY_CONTAINER_ID_FIELD);

        MLMemorySpec.MLMemorySpecBuilder builder = MLMemorySpec.builder();

        builder.type(type);
        if (sessionId != null) {
            builder.sessionId(sessionId);
        }
        if (windowSize != null) {
            builder.windowSize(windowSize);
        }
        if (memoryContainerId != null) {
            builder.memoryContainerId(memoryContainerId);
        }

        return builder.build();
    }

    private Map<String, Object> getParseFieldMap(String llmFieldMapString) throws OpenSearchParseException {
        BytesReference llmFieldBytes = new BytesArray(llmFieldMapString.getBytes(StandardCharsets.UTF_8));
        return XContentHelper.convertToMap(llmFieldBytes, false, MediaTypeRegistry.JSON).v2();
    }

    private void validateLLMParametersMap(Object llmParams) {
        String errorMessage = "llm field [" + PARAMETERS_FIELD + "] must be a string to string map";
        if (!(llmParams instanceof Map)) {
            throw new IllegalArgumentException(errorMessage);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> llmParamsMap = (Map<String, Object>) llmParams;
        for (Map.Entry<String, Object> entry : llmParamsMap.entrySet()) {
            if (!(entry.getValue() instanceof String)) {
                throw new IllegalArgumentException(errorMessage);
            }
        }
    }
}
