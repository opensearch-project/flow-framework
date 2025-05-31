/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.flowframework.util.ApiSpecFetcher;
import org.opensearch.flowframework.util.ParseUtils;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.MLAgentType;
import org.opensearch.ml.common.agent.LLMSpec;
import org.opensearch.ml.common.agent.MLAgent;
import org.opensearch.ml.common.agent.MLMemorySpec;
import org.opensearch.ml.common.agent.MLToolSpec;
import org.opensearch.ml.common.transport.agent.MLRegisterAgentResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.flowframework.common.CommonValue.ML_COMMONS_API_SPEC_YAML_URI;
import static org.opensearch.flowframework.common.WorkflowResources.AGENT_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RegisterAgentTests extends OpenSearchTestCase {
    private WorkflowData inputData = WorkflowData.EMPTY;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;
    private MLToolSpec tools;
    private LLMSpec llmSpec;
    private Map<String, String> llmParams;
    private Map<?, ?> mlMemorySpec;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        MockitoAnnotations.openMocks(this);

        this.tools = MLToolSpec.builder()
            .type("tool1")
            .name("CatIndexTool")
            .description("desc")
            .parameters(Collections.emptyMap())
            .includeOutputInAgentResponse(false)
            .build();

        this.llmSpec = new LLMSpec("xyz", Collections.emptyMap());

        this.mlMemorySpec = Map.ofEntries(
            Map.entry(MLMemorySpec.MEMORY_TYPE_FIELD, "type"),
            Map.entry(MLMemorySpec.SESSION_ID_FIELD, "abc"),
            Map.entry(MLMemorySpec.WINDOW_SIZE_FIELD, 2)
        );

        this.llmParams = Map.ofEntries(Map.entry("a", "a"), Map.entry("b", "b"), Map.entry("c", "c"));

        Map<String, Object> llmFieldMap = Map.ofEntries(Map.entry("model_id", "xyz"), Map.entry("parameters", llmParams));

        inputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "test"),
                Map.entry("description", "description"),
                Map.entry("type", MLAgentType.FLOW.name()),
                Map.entry("llm", ParseUtils.parseArbitraryStringToObjectMapToString(llmFieldMap)),
                Map.entry("tools", tools),
                Map.entry("tools_order", new String[] { "abc", "xyz" }),
                Map.entry("parameters", Collections.emptyMap()),
                Map.entry("memory", mlMemorySpec),
                Map.entry("created_time", 1689793598499L),
                Map.entry("last_updated_time", 1689793598499L),
                Map.entry("app_type", "app")
            ),
            "test-id",
            "test-node-id"
        );
    }

    public void testRegisterAgent() throws IOException, ExecutionException, InterruptedException {
        String agentId = AGENT_ID;
        RegisterAgentStep registerAgentStep = new RegisterAgentStep(machineLearningNodeClient, flowFrameworkIndicesHandler);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<MLRegisterAgentResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<MLAgent> mlAgentArgumentCaptor = ArgumentCaptor.forClass(MLAgent.class);

        doAnswer(invocation -> {
            ActionListener<MLRegisterAgentResponse> actionListener = invocation.getArgument(1);
            MLRegisterAgentResponse output = new MLRegisterAgentResponse(agentId);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).registerAgent(any(MLAgent.class), actionListenerCaptor.capture());

        doAnswer(invocation -> {
            ActionListener<WorkflowData> updateResponseListener = invocation.getArgument(5);
            updateResponseListener.onResponse(new WorkflowData(Map.of(AGENT_ID, agentId), "test-id", "test-node-id"));
            return null;
        }).when(flowFrameworkIndicesHandler)
            .addResourceToStateIndex(any(WorkflowData.class), anyString(), anyString(), anyString(), nullable(String.class), any());

        PlainActionFuture<WorkflowData> future = registerAgentStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );

        verify(machineLearningNodeClient).registerAgent(mlAgentArgumentCaptor.capture(), actionListenerCaptor.capture());
        assertEquals(llmParams, mlAgentArgumentCaptor.getValue().getLlm().getParameters());

        assertTrue(future.isDone());
        assertEquals(agentId, future.get().getContent().get(AGENT_ID));
    }

    public void testRegisterAgentFailure() throws IOException {
        String agentId = AGENT_ID;
        RegisterAgentStep registerAgentStep = new RegisterAgentStep(machineLearningNodeClient, flowFrameworkIndicesHandler);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<MLRegisterAgentResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLRegisterAgentResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new FlowFrameworkException("Failed to register the agent", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(machineLearningNodeClient).registerAgent(any(MLAgent.class), actionListenerCaptor.capture());

        doAnswer(invocation -> {
            ActionListener<WorkflowData> updateResponseListener = invocation.getArgument(5);
            updateResponseListener.onResponse(new WorkflowData(Map.of(AGENT_ID, agentId), "test-id", "test-node-id"));
            return null;
        }).when(flowFrameworkIndicesHandler)
            .addResourceToStateIndex(any(WorkflowData.class), anyString(), anyString(), anyString(), any(), any());

        PlainActionFuture<WorkflowData> future = registerAgentStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );

        verify(machineLearningNodeClient).registerAgent(any(MLAgent.class), actionListenerCaptor.capture());

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to register the agent", ex.getCause().getMessage());
    }

    public void testApiSpecRegisterAgentInputParamComparison() throws Exception {
        List<String> requiredEnumParams = WorkflowStepFactory.WorkflowSteps.REGISTER_AGENT.inputs();

        boolean isMatch = ApiSpecFetcher.compareRequiredFields(
            requiredEnumParams,
            ML_COMMONS_API_SPEC_YAML_URI,
            "/_plugins/_ml/agents/_register",
            RestRequest.Method.POST
        );

        assertTrue(isMatch);
    }

    public void testLLMParametersFieldParseFailure() throws Exception {
        String agentId = AGENT_ID;
        RegisterAgentStep registerAgentStep = new RegisterAgentStep(machineLearningNodeClient, flowFrameworkIndicesHandler);

        // Create llm parameters with wrong format
        Map<String, Object> invalidLLMFieldMap = Map.ofEntries(Map.entry("model_id", "xyz"), Map.entry("parameters", "invalidString"));
        WorkflowData invalidWorkflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "test"),
                Map.entry("description", "description"),
                Map.entry("type", MLAgentType.FLOW.name()),
                Map.entry("llm", ParseUtils.parseArbitraryStringToObjectMapToString(invalidLLMFieldMap)),
                Map.entry("tools", tools),
                Map.entry("tools_order", new String[] { "abc", "xyz" }),
                Map.entry("parameters", Collections.emptyMap()),
                Map.entry("memory", mlMemorySpec),
                Map.entry("created_time", 1689793598499L),
                Map.entry("last_updated_time", 1689793598499L),
                Map.entry("app_type", "app")
            ),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = registerAgentStep.execute(
            invalidWorkflowData.getNodeId(),
            invalidWorkflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("llm field [parameters] must be a string to string map", ex.getCause().getMessage());
    }

    public void testLLMParametersValidationFailure() throws Exception {
        String agentId = AGENT_ID;
        RegisterAgentStep registerAgentStep = new RegisterAgentStep(machineLearningNodeClient, flowFrameworkIndicesHandler);

        // Create llm parameters with non-string value
        Map<String, Object> invalidLLMFieldMap = Map.ofEntries(Map.entry("model_id", "xyz"), Map.entry("parameters", 123));
        WorkflowData invalidWorkflowData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "test"),
                Map.entry("description", "description"),
                Map.entry("type", MLAgentType.FLOW.name()),
                Map.entry("llm", ParseUtils.parseArbitraryStringToObjectMapToString(invalidLLMFieldMap)),
                Map.entry("tools", tools),
                Map.entry("tools_order", new String[] { "abc", "xyz" }),
                Map.entry("parameters", Collections.emptyMap()),
                Map.entry("memory", mlMemorySpec),
                Map.entry("created_time", 1689793598499L),
                Map.entry("last_updated_time", 1689793598499L),
                Map.entry("app_type", "app")
            ),
            "test-id",
            "test-node-id"
        );

        PlainActionFuture<WorkflowData> future = registerAgentStep.execute(
            invalidWorkflowData.getNodeId(),
            invalidWorkflowData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            null
        );

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("llm field [parameters] must be a string to string map", ex.getCause().getMessage());
    }

}
