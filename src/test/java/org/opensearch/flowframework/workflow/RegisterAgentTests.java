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
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.agent.LLMSpec;
import org.opensearch.ml.common.agent.MLAgent;
import org.opensearch.ml.common.agent.MLMemorySpec;
import org.opensearch.ml.common.agent.MLToolSpec;
import org.opensearch.ml.common.transport.agent.MLRegisterAgentResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.common.WorkflowResources.AGENT_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RegisterAgentTests extends OpenSearchTestCase {
    private WorkflowData inputData = WorkflowData.EMPTY;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    private FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        MockitoAnnotations.openMocks(this);

        MLToolSpec tools = new MLToolSpec("tool1", "CatIndexTool", "desc", Collections.emptyMap(), false);

        LLMSpec llmSpec = new LLMSpec("xyz", Collections.emptyMap());

        Map<?, ?> mlMemorySpec = Map.ofEntries(
            Map.entry(MLMemorySpec.MEMORY_TYPE_FIELD, "type"),
            Map.entry(MLMemorySpec.SESSION_ID_FIELD, "abc"),
            Map.entry(MLMemorySpec.WINDOW_SIZE_FIELD, 2)
        );

        inputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "test"),
                Map.entry("description", "description"),
                Map.entry("type", "type"),
                Map.entry("llm.model_id", "xyz"),
                Map.entry("llm.parameters", Collections.emptyMap()),
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

        doAnswer(invocation -> {
            ActionListener<MLRegisterAgentResponse> actionListener = invocation.getArgument(1);
            MLRegisterAgentResponse output = new MLRegisterAgentResponse(agentId);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).registerAgent(any(MLAgent.class), actionListenerCaptor.capture());

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> updateResponseListener = invocation.getArgument(4);
            updateResponseListener.onResponse(new UpdateResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "id", -2, 0, 0, UPDATED));
            return null;
        }).when(flowFrameworkIndicesHandler).updateResourceInStateIndex(anyString(), anyString(), anyString(), anyString(), any());

        PlainActionFuture<WorkflowData> future = registerAgentStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        verify(machineLearningNodeClient).registerAgent(any(MLAgent.class), actionListenerCaptor.capture());

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
            ActionListener<UpdateResponse> updateResponseListener = invocation.getArgument(4);
            updateResponseListener.onResponse(new UpdateResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "id", -2, 0, 0, UPDATED));
            return null;
        }).when(flowFrameworkIndicesHandler).updateResourceInStateIndex(anyString(), anyString(), anyString(), anyString(), any());

        PlainActionFuture<WorkflowData> future = registerAgentStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        verify(machineLearningNodeClient).registerAgent(any(MLAgent.class), actionListenerCaptor.capture());

        assertTrue(future.isDone());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to register the agent", ex.getCause().getMessage());
    }
}
