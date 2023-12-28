/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.flowframework.workflow;

import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.flowframework.exception.FlowFrameworkException;
import org.opensearch.flowframework.indices.FlowFrameworkIndicesHandler;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.AccessMode;
import org.opensearch.ml.common.MLTaskState;
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupInput;
import org.opensearch.ml.common.transport.model_group.MLRegisterModelGroupResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.flowframework.common.CommonValue.WORKFLOW_STATE_INDEX;
import static org.opensearch.flowframework.common.WorkflowResources.MODEL_GROUP_ID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ModelGroupStepTests extends OpenSearchTestCase {
    private WorkflowData inputData;
    private WorkflowData inputDataWithNoName;

    @Mock
    MachineLearningNodeClient machineLearningNodeClient;

    FlowFrameworkIndicesHandler flowFrameworkIndicesHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.flowFrameworkIndicesHandler = mock(FlowFrameworkIndicesHandler.class);
        MockitoAnnotations.openMocks(this);
        inputData = new WorkflowData(
            Map.ofEntries(
                Map.entry("name", "test"),
                Map.entry("description", "description"),
                Map.entry("backend_roles", List.of("role-1")),
                Map.entry("access_mode", AccessMode.PUBLIC),
                Map.entry("add_all_backend_roles", false)
            ),
            "test-id",
            "test-node-id"
        );
        inputDataWithNoName = new WorkflowData(Collections.emptyMap(), "test-id", "test-node-id");
    }

    public void testRegisterModelGroup() throws ExecutionException, InterruptedException, IOException {
        String modelGroupId = MODEL_GROUP_ID;
        String status = MLTaskState.CREATED.name();

        ModelGroupStep modelGroupStep = new ModelGroupStep(machineLearningNodeClient, flowFrameworkIndicesHandler);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<MLRegisterModelGroupResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelGroupResponse> actionListener = invocation.getArgument(1);
            MLRegisterModelGroupResponse output = new MLRegisterModelGroupResponse(modelGroupId, status);
            actionListener.onResponse(output);
            return null;
        }).when(machineLearningNodeClient).registerModelGroup(any(MLRegisterModelGroupInput.class), actionListenerCaptor.capture());

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> updateResponseListener = invocation.getArgument(4);
            updateResponseListener.onResponse(new UpdateResponse(new ShardId(WORKFLOW_STATE_INDEX, "", 1), "id", -2, 0, 0, UPDATED));
            return null;
        }).when(flowFrameworkIndicesHandler).updateResourceInStateIndex(anyString(), anyString(), anyString(), anyString(), any());

        CompletableFuture<WorkflowData> future = modelGroupStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        verify(machineLearningNodeClient).registerModelGroup(any(MLRegisterModelGroupInput.class), actionListenerCaptor.capture());

        assertTrue(future.isDone());
        assertEquals(modelGroupId, future.get().getContent().get(MODEL_GROUP_ID));
        assertEquals(status, future.get().getContent().get("model_group_status"));

    }

    public void testRegisterModelGroupFailure() throws IOException {
        ModelGroupStep modelGroupStep = new ModelGroupStep(machineLearningNodeClient, flowFrameworkIndicesHandler);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ActionListener<MLRegisterModelGroupResponse>> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);

        doAnswer(invocation -> {
            ActionListener<MLRegisterModelGroupResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new FlowFrameworkException("Failed to register model group", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(machineLearningNodeClient).registerModelGroup(any(MLRegisterModelGroupInput.class), actionListenerCaptor.capture());

        CompletableFuture<WorkflowData> future = modelGroupStep.execute(
            inputData.getNodeId(),
            inputData,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        verify(machineLearningNodeClient).registerModelGroup(any(MLRegisterModelGroupInput.class), actionListenerCaptor.capture());

        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Failed to register model group", ex.getCause().getMessage());

    }

    public void testRegisterModelGroupWithNoName() throws IOException {
        ModelGroupStep modelGroupStep = new ModelGroupStep(machineLearningNodeClient, flowFrameworkIndicesHandler);

        CompletableFuture<WorkflowData> future = modelGroupStep.execute(
            inputDataWithNoName.getNodeId(),
            inputDataWithNoName,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertTrue(future.isCompletedExceptionally());
        ExecutionException ex = assertThrows(ExecutionException.class, () -> future.get().getContent());
        assertTrue(ex.getCause() instanceof FlowFrameworkException);
        assertEquals("Missing required inputs [name] in workflow [test-id] node [test-node-id]", ex.getCause().getMessage());
    }

}
